# apps/exchange/mongo_queued_db_writer.py
import asyncio
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Any, Dict, List, Tuple

from apps.exchange.models import Order, Trade
from apps.exchange.settings import get_settings, admin_uri

SET = get_settings()


class QueuedDbWriter:
    def __init__(self):
        self._sync_client = MongoClient(admin_uri())
        self.sync_db = self._sync_client[SET.mongo_db]

        self._async_client = AsyncIOMotorClient(admin_uri())
        self._async_db = self._async_client[SET.mongo_db]

        self._queue: asyncio.Queue[Tuple[str, Any]] = asyncio.Queue()
        self._consumer_task: asyncio.Task | None = None

    # ───────── rebuild‐time helpers (called by Exchange.rebuild_from_database) ─────
    def list_instruments(self) -> List[int]:
        coll_names = self.sync_db.list_collection_names()
        inst_ids: List[int] = []
        for name in coll_names:
            if name.startswith("orders_"):
                try:
                    inst_ids.append(int(name.split("_", 1)[1]))
                except ValueError:
                    pass
        return inst_ids

    def iter_orders(self, instrument_id: int) -> List[Dict[str, Any]]:
        coll = self.sync_db[f"orders_{instrument_id}"]
        cursor = coll.find({}, {"_id": 0}).sort("timestamp", 1)
        return list(cursor)  # each document already has no "_id"

    def create_instrument(self, instrument_id: str) -> None:
        orders_coll = f"orders_{instrument_id}"
        live_coll = f"live_orders_{instrument_id}"
        trades_coll = f"trades_{instrument_id}"

        async def _ensure_collections():
            try:
                await self.sync_db.create_collection(orders_coll)
            except CollectionInvalid:
                pass

            try:
                await self.sync_db.create_collection(live_coll)
            except CollectionInvalid:
                pass

            try:
                await self.sync_db.create_collection(trades_coll)
            except CollectionInvalid:
                pass

        asyncio.create_task(_ensure_collections())


    # ───────── hot‐path enqueue methods (never await anything) ───────────────────────
    def record_order(self, order: Order) -> None:
        self._queue.put_nowait(("ORDER", order.__dict__))

    def record_trade(self, trade: Trade) -> None:
        self._queue.put_nowait(("TRADE", trade.__dict__))

    def record_cancel(self, instrument_id: int, order_id: int) -> None:
        self._queue.put_nowait(("CANCEL", {"instrument_id": instrument_id, "order_id": order_id}))

    def upsert_live_order(self, order: Order) -> None:
        self._queue.put_nowait(("UPS_LIVE", order.__dict__))

    def remove_live_order(self, inst: int, order_id: int) -> None:
        self._queue.put_nowait(("REM_LIVE", {"instrument_id": inst, "order_id": order_id}))

    def update_order_quantity(self, instrument_id: int, order_id: int, quantity_modification: int) -> None:
        self._queue.put_nowait(("UPDATE_LIVE", {
            "instrument_id": instrument_id,
            "order_id": order_id,
            "quantity_modification": quantity_modification
        }))


    # ───────── background consumer routinely drains the queue ───────────────────────
    async def _consumer_loop(self) -> None:
        while True:
            msg_type, payload = await self._queue.get()
            try:
                if msg_type == "ORDER":
                    instr = payload["instrument_id"]
                    coll = self._async_db[f"orders_{instr}"]
                    await coll.replace_one(
                        {"order_id": payload["order_id"]},
                        payload,
                        upsert=True
                    )

                elif msg_type == "TRADE":
                    instr = payload["instrument_id"]
                    coll = self._async_db[f"trades_{instr}"]
                    await coll.insert_one(payload)

                elif msg_type == "CANCEL":
                    instr = payload["instrument_id"]
                    oid = payload["order_id"]
                    # remove from live_orders; if you want a history, insert into cancel_history here
                    coll_live = self._async_db[f"live_orders_{instr}"]
                    await coll_live.delete_one({"order_id": oid})

                elif msg_type == "UPS_LIVE":
                    instr = payload["instrument_id"]
                    coll = self._async_db[f"live_orders_{instr}"]
                    await coll.replace_one(
                        {"order_id": payload["order_id"]},
                        payload,
                        upsert=True
                    )

                elif msg_type == "REM_LIVE":
                    instr = payload["instrument_id"]
                    oid = payload["order_id"]
                    coll = self._async_db[f"live_orders_{instr}"]
                    await coll.delete_one({"order_id": oid})

                elif msg_type == "UPDATE_LIVE":
                    instr = payload["instrument_id"]
                    oid = payload["order_id"]
                    quantity_modification = payload["quantity_modification"]
                    coll = self._async_db[f"live_orders_{instr}"]

                    await coll.update_one(
                        {"order_id": oid},
                        {"$inc": {
                            "remaining_quantity": -quantity_modification,
                            "filled_quantity": quantity_modification
                        }}
                    )

                else:
                    # unknown message → ignore or log
                    pass

            except Exception as e:
                pass
            finally:
                self._queue.task_done()

    async def startup(self) -> None:
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._consumer_loop())

    async def shutdown(self) -> None:
        if self._consumer_task:
            await self._queue.join()
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None
