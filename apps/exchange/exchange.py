# apps/exchange/exchange.py
from __future__ import annotations
import logging
from time import time_ns
from typing import Dict, List
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator
from dataclasses import asdict

from apps.exchange.order_book   import OrderBook
from apps.exchange.models import Order, Trade, Side, OrderType
from apps.exchange.composite_writer import CompositeWriter

# ───────────────────── request DTOs ───────────────────────────────────
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


# ───────────────────── Exchange ───────────────────────────────────────
class Exchange:
    def __init__(self, writer: CompositeWriter):
        self.log = logging.getLogger("Exchange")
        self._writer = writer
        self._books: Dict[int, OrderBook] = {}
        self._next_oid: int = 1
        self.log.info("Exchange starting — rebuilding books ...")
        self._rebuild_from_database()
        self.log.info("Exchange rebuild complete — ready to serve")
        self.log.info("OID counter starts at %s", self._next_oid)
        self._empty_hit = 0

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
        if (order.order_type is OrderType.GTC or order.order_type is OrderType.LIMIT) and order.quantity > 0 and order.cancelled == False:
            self._writer.upsert_live_order(order)

        self._writer.record_order(order)
        for trade in trades:
            self._writer.record_trade(trade)
            if trade.maker_quantity_remaining == 0:
                self._writer.remove_live_order(
                    inst=order.instrument_id,
                    order_id=trade.maker_order_id
                )
            if trade.taker_quantity_remaining == 0:
                self._writer.remove_live_order(
                    inst=order.instrument_id,
                    order_id=trade.taker_order_id
                )

        self.log.info("ACCEPT  oid=%s qty_rem=%s trades=%d",
                 order.order_id, order.quantity, len(trades))
        return {
            "status": "ACCEPTED",
            "order_id": order.order_id,
            "remaining_qty": order.quantity,
            "cancelled": order.cancelled,
            "trades": [asdict(t) for t in trades],
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

        if book.cancel(req.order_id):
            self._writer.record_cancel(req.instrument_id, req.order_id)
            self._writer.remove_live_order(
                inst=req.instrument_id,
                order_id=req.order_id
            )
            self.log.info("CANCELLED oid=%s", req.order_id)
            return {"status": "CANCELLED", "order_id": req.order_id}

        self.log.info("cancel miss oid=%s", req.order_id)
        return {"status": "ERROR", "details": "order not open"}

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
        oid = self._next_oid
        self._next_oid += 1
        return Order(
            order_type=req.order_type,
            side=req.side,
            instrument_id=req.instrument_id,
            price_cents=req.price_cents,
            quantity=req.quantity,
            timestamp=time_ns(),
            order_id=oid,
            party_id=req.party_id,
            cancelled=False,
        )

    # ───────────── cold-start rebuild logic ───────────────────────────
    def _rebuild_from_database(self) -> None:
        for instr in self._writer.list_instruments():
            book = OrderBook(instr)
            self._books[instr] = book
            row_iter = self._writer.iter_orders(instr)  # must be ordered by ts
            self.log.info("REBUILD-START instrument=%s", instr)

            count_rows = 0
            for row in row_iter:
                if row["cancelled"] or row["quantity"] <= 0:
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
                )
                book.rest_order(order)
                self._next_oid = max(self._next_oid, row["order_id"] + 1)
                count_rows += 1

            self.log.info("REBUILD-END  instrument=%s rows=%d", instr, count_rows)
