# apps/exchange/exchange.py
from __future__ import annotations
import logging
from time import time_ns
from typing import Dict, List
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from pymongo import ReturnDocument
from pymongo import MongoClient

from apps.exchange.mongo_queued_db_writer import QueuedDbWriter
from apps.exchange.order_book   import OrderBook
from apps.exchange.models import Order, Trade, Side, OrderType
from apps.exchange.composite_writer import CompositeWriter
from apps.exchange.settings import get_settings, admin_uri

SET = get_settings()

class _AuthMixin(BaseModel):
    party_id: int  = Field(gt=0)
    password: str  = Field(min_length=1)

class NewOrderReq(_AuthMixin):
    instrument_id: int
    side: Side | str
    order_type: OrderType | str
    price_cents: int | None = Field(None, ge=0)
    quantity: int = Field(gt=0)
    party_id: int = Field(gt=0)

    @field_validator("side", mode="before")
    def _cast_side(cls, v):
        if isinstance(v, str):
            try:
                return Side[v]
            except KeyError:
                raise ValueError(f"invalid side '{v}'")
        return v

    @field_validator("order_type", mode="before")
    def _cast_ot(cls, v):
        if isinstance(v, str):
            try:
                return OrderType[v]
            except KeyError:
                raise ValueError(f"invalid order_type '{v}'")
        return v

    @model_validator(mode="after")
    def _check_price(cls, m):
        if m.order_type in (OrderType.GTC, OrderType.IOC) and m.price_cents is None:
            raise ValueError("price_cents required for GTC/IOC")
        if m.price_cents is None:
            logging.getLogger("Exchange").debug("price missing for MARKET; default 0")
            m.price_cents = 0
        return m


class CancelReq(_AuthMixin):
    instrument_id: int
    order_id: int

class CancelAllReq(_AuthMixin):
    instrument_id: int


class Exchange:
    def __init__(self, writer: CompositeWriter):
        self.log = logging.getLogger("Exchange")
        self._writer = writer
        self._books: Dict[int, OrderBook] = {}
        self.log.info("Exchange starting — rebuilding books ...")
        self.log.info("Exchange rebuild complete — ready to serve")
        self._empty_hit = 0
        self._mongo_client = MongoClient(admin_uri())
        self.mongo_db = self._mongo_client[SET.mongo_db]

    def _get_next_order_id(self) -> int:
        coll = self.mongo_db["counters"]
        doc = coll.find_one_and_update(
            {"_id": "order_id"},
            {"$inc": {"seq": 1}},
            return_document=ReturnDocument.AFTER,
            upsert=True
        )
        return doc["seq"]

    # ───────────── public API (routes will call) ──────────────────────
    def handle_new_order(self, payload: dict) -> dict:
        self.log.debug("RX new-order JSON: %s", payload)
        try:
            req = NewOrderReq(**payload)
        except ValidationError as e:
            self.log.warning("validation error: %s", e)
            return {"status": "ERROR", "details": e.errors()}

        book = self._books.get(req.instrument_id)
        if not book:
            self.log.warning("new-order unknown instrument %s", req.instrument_id)
            return {"status": "ERROR", "details": "unknown instrument"}

        order = self._build_order(req)
        trades: List[Trade] = book.submit(order)
        if (order.order_type is OrderType.GTC) and order.remaining_quantity > 0 and order.cancelled == False:
            self._writer.upsert_live_order(order)

        self._writer.record_order(order)
        for trade in trades:
            self._writer.record_trade(trade)
            for oid, qty_rem in [(trade.maker_order_id, trade.maker_quantity_remaining),(trade.taker_order_id, trade.taker_quantity_remaining)]:
                self._writer.update_order_quantity(
                    instrument_id=trade.instrument_id,
                    order_id=oid,
                    quantity_modification=trade.quantity,
                )
                if qty_rem == 0:
                    self._writer.remove_live_order(
                        inst=order.instrument_id,
                        order_id=oid
                    )

        self.log.info("ACCEPT  oid=%s qty_rem=%s trades=%d",
                 order.order_id, order.remaining_quantity, len(trades))
        return {
            "status": "ACCEPTED",
            "order_id": order.order_id,
            "remaining_qty": order.remaining_quantity,
            "cancelled": order.cancelled,
            "trades": [t.__dict__ for t in trades],
        }

    def handle_cancel(self, payload: dict) -> dict:
        self.log.debug("RX cancel JSON: %s", payload)
        try:
            req = CancelReq(**payload)
        except ValidationError as e:
            return {"status": "ERROR", "details": e.errors()}

        book = self._books.get(req.instrument_id)
        if not book:
            self.log.warning("cancel unknown instrument %s", req.instrument_id)
            return {"status": "ERROR", "details": "unknown instrument"}

        cancelled_order = book.oid_map.get(req.order_id)
        if book.cancel(req.order_id):
            self._writer.record_cancel(req.instrument_id, req.order_id)
            self._writer.remove_live_order(
                inst=req.instrument_id,
                order_id=req.order_id
            )
            if cancelled_order is not None:
                self._writer.record_order(cancelled_order)
            self.log.info("CANCELLED oid=%s", req.order_id)
            return {"status": "CANCELLED", "order_id": req.order_id}
        self.log.info("cancel miss oid=%s", req.order_id)
        return {"status": "ERROR", "details": "order not open"}

    def handle_cancel_all(self, payload: dict) -> dict:
        try:
            req = CancelAllReq(**payload)
        except ValidationError as e:
            return {"status": "ERROR", "details": e.errors()}
        try:
            instrument_id = int(payload.get("instrument_id", 0))
        except ValueError:
            return {"status": "ERROR", "details": "invalid instrument_id"}
        book = self._books.get(instrument_id)
        if not book:
            return {"status": "ERROR", "details": "unknown instrument"}

        cancelled_ids = []
        failed_ids = []
        for oid, order in list(book.oid_map.items()):
            if order.party_id == req.party_id:
                first_time = book.cancel(oid)
                if first_time:
                    self._writer.record_cancel(req.instrument_id, oid)
                    self._writer.remove_live_order(
                        inst=req.instrument_id, order_id=oid
                    )
                    self._writer.record_order(order)
                    cancelled_ids.append(oid)
                else:
                    failed_ids.append(oid)
        return {
            "status": "CANCELLED_ALL",
            "cancelled_order_ids": cancelled_ids,
            "failed_order_ids": failed_ids
        }

    # ───────────── management API  (callable from a POST /new_book) ───
    def create_order_book(self, instrument_id: int) -> dict:
        if instrument_id in self._books:
            return {"status": "ERROR", "details": "instrument already exists"}

        self.log.info("CREATE-BOOK %s", instrument_id)
        self._books[instrument_id] = OrderBook(instrument_id)
        self._writer.create_instrument(instrument_id)
        self.log.info("CREATE-BOOK %s ok; total books=%d", instrument_id, len(self._books))
        return {"status": "CREATED", "instrument_id": instrument_id}

    # ───────────── internal helpers ───────────────────────────────────
    def _build_order(self, req: NewOrderReq) -> Order:
        return Order(
            order_type=req.order_type,
            side=req.side,
            instrument_id=req.instrument_id,
            price_cents=req.price_cents,
            quantity=req.quantity,
            timestamp=time_ns(),
            order_id=self._get_next_order_id(),
            party_id=req.party_id,
            cancelled=False,
            filled_quantity=0,
            remaining_quantity=req.quantity,
        )

    # ───────────── cold-start rebuild logic ───────────────────────────
    async def rebuild_from_database(self, writer: QueuedDbWriter) -> None:
        for instr in writer.list_instruments():
            book = OrderBook(instr)
            self._books[instr] = book
            row_iter = writer.iter_orders(instr)
            self.log.info("REBUILD-START instrument=%s", instr)

            count_rows = 0
            for row in row_iter:
                if row["cancelled"] or row["remaining_quantity"] <= 0:
                    continue
                order = Order(
                    order_type=OrderType[row["order_type"]],
                    side=Side[row["side"]],
                    instrument_id=instr,
                    price_cents=row["price_cents"],
                    quantity=row["quantity"],
                    timestamp=row["timestamp"],
                    order_id=row["order_id"],
                    party_id=row["party_id"],
                    cancelled=row["cancelled"],
                    remaining_quantity=row["remaining_quantity"],
                    filled_quantity=row["filled_quantity"]
                )
                book.rest_order(order)
                count_rows += 1
            self.log.info("REBUILD-END  instrument=%s rows=%d", instr, count_rows)
