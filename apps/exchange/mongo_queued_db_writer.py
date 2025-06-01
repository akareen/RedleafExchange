# apps/exchange/mongo_queued_db_writer.py
import asyncio
from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Any, Dict, List, Tuple

from apps.exchange.models import Order, Trade
from apps.exchange.settings import get_settings

SET = get_settings()


def _admin_uri() -> str:
    """
    Build a URI that connects as the “root” or “admin” user.
    You must have created an admin user in Mongo with the correct role.
    """
    if SET.mongo_user and SET.mongo_pass:
        return f"mongodb://{SET.mongo_user}:{SET.mongo_pass}@{SET.mongo_host}:{SET.mongo_port}/admin"
    else:
        # if no auth is configured, assume localhost without credentials
        return f"mongodb://{SET.mongo_host}:{SET.mongo_port}/admin"

class QueuedDbWriter:
    """
    A drop‐in replacement for MongoAsyncWriter that never blocks the “hot path.”
    All writes get enqueued; a single background coroutine drains them.
    Meanwhile, rebuild‐time methods use a separate synchronous PyMongo client.
    """
    def __init__(self):
        # — synchronous client for rebuild()
        self._sync_client = MongoClient(_admin_uri())
        self._sync_db = self._sync_client[SET.mongo_db]

        # — motor client for background writes
        self._async_client = AsyncIOMotorClient(_admin_uri())
        self._async_db = self._async_client[SET.mongo_db]

        # queue of (msg_type, payload)
        self._queue: asyncio.Queue[Tuple[str, Any]] = asyncio.Queue()
        self._consumer_task: asyncio.Task | None = None

    # ───────── rebuild‐time helpers (called by Exchange.rebuild_from_database) ─────
    def list_instruments(self) -> List[int]:
        """
        Return a list of all instrument‐IDs for which we have an 'orders_<instr>' collection.
        Uses synchronous pymongo, so no asyncio.run→ never conflicts with FastAPI's loop.
        """
        coll_names = self._sync_db.list_collection_names()
        inst_ids: List[int] = []
        for name in coll_names:
            if name.startswith("orders_"):
                try:
                    inst_ids.append(int(name.split("_", 1)[1]))
                except ValueError:
                    pass
        return inst_ids

    def iter_orders(self, instrument_id: int) -> List[Dict[str, Any]]:
        """
        Stream all existing orders_{instrument_id}, sorted by timestamp ascending.
        Uses synchronous pymongo, so it's a normal blocking call.
        """
        coll = self._sync_db[f"orders_{instrument_id}"]
        cursor = coll.find({}, {"_id": 0}).sort("timestamp", 1)
        return list(cursor)  # each document already has no "_id"

    def create_instrument(self, instrument_id: str) -> None:
        """
        Create two collections named:
          - f"orders_{instrument_id}"
          - f"live_orders_{instrument_id}"
        in the background, but don’t error if they already exist.
        """
        orders_coll = f"orders_{instrument_id}"
        live_coll = f"live_orders_{instrument_id}"
        trades_coll = f"trades_{instrument_id}"

        async def _ensure_collections():
            try:
                await self._sync_db.create_collection(orders_coll)
            except CollectionInvalid:
                # already existed, ignore
                pass

            try:
                await self._sync_db.create_collection(live_coll)
            except CollectionInvalid:
                # already existed, ignore
                pass

            try:
                await self._sync_db.create_collection(trades_coll)
            except CollectionInvalid:
                # already existed, ignore
                pass

        # Schedule the task; pass the coroutine directly to create_task.
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
        """
        Update the quantity of an existing order in the live_orders collection.
        This is a special case where we modify an existing order's quantity.
        """
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

            except Exception:
                # In production, use a real logger
                print(f"[QueuedDbWriter] error handling {msg_type} → {payload}")

            finally:
                self._queue.task_done()

    async def startup(self) -> None:
        """Spawn the background consumer (if not already running)."""
        if self._consumer_task is None or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self._consumer_loop())

    async def shutdown(self) -> None:
        """
        Flush any remaining writes, then cancel the consumer task.
        Called on FastAPI shutdown.
        """
        if self._consumer_task:
            await self._queue.join()
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None
