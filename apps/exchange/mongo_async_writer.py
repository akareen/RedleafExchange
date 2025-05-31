# apps/exchange/mongo_async_writer.py
import asyncio, logging
from urllib.parse import quote_plus
from motor.motor_asyncio import AsyncIOMotorClient
from dataclasses import asdict

from apps.exchange.settings import get_settings
from apps.exchange.models import Order, Trade

SET = get_settings()

def _mongo_uri() -> str:
    if SET.mongo_user:
        auth = f"{quote_plus(SET.mongo_user)}:{quote_plus(SET.mongo_pass)}@"
    else:
        auth = ""
    return f"mongodb://{auth}{SET.mongo_host}:{SET.mongo_port}/{SET.mongo_db}"

class MongoAsyncWriter:
    def __init__(self):
        self.client = AsyncIOMotorClient(_mongo_uri())
        self.db     = self.client[SET.mongo_db]
        self.log = logging.getLogger("MongoAsyncWriter")
        self.log.info("Mongo connected %s", _mongo_uri())

    # ------------ rebuild helpers (sync wrappers) -------------------
    def list_instruments(self):
        self.log.info("rebuilding all orders from MongoDB")
        names = asyncio.get_event_loop().run_until_complete(self.db.list_collection_names())
        return [int(n.split("_")[1]) for n in names if n.startswith("orders_")]

    def iter_orders(self, instrument_id: int):
        async def _collect():
            return [doc async for doc in self._stream_orders(instrument_id)]
        return asyncio.get_event_loop().run_until_complete(_collect())

    async def _stream_orders(self, instrument_id: int):
        coll = self.db[f"orders_{instrument_id}"]
        cursor = coll.find().sort("timestamp", 1)
        count = 0
        async for doc in cursor:
            doc.pop("_id", None)
            yield doc
            count += 1
            if count % 100000 == 0:
                self.log.info("rebuild %s streamed %dk rows", instrument_id, count // 1000)

    def create_instrument(self, instrument_id: int):
        self.log.info("creating instrument %s for DB", instrument_id)
        self.db[f"orders_{instrument_id}"].create_index("order_id", unique=True)
        self.db[f"trades_{instrument_id}"].create_index("timestamp")

    # ------------ async writes --------------------------------------
    async def _insert(self, coll, doc):
        await coll.insert_one(doc)

    async def _upsert(self, coll, key, doc):
        await coll.replace_one(key, doc, upsert=True)

    def record_order(self, order: Order):
        self.log.info("inserting order %s", order)
        coll = self.db[f"orders_{order.instrument_id}"]
        asyncio.create_task(self._upsert(coll, {"order_id": order.order_id}, asdict(order)))

    def record_trade(self, trade: Trade):
        self.log.info("inserting trade %s", trade)
        coll = self.db[f"trades_{trade.instrument_id}"]
        asyncio.create_task(self._insert(coll, asdict(trade)))

    def record_cancel(self, inst, oid):
        self.log.info("inserting cancel %s", oid)
        self.remove_live_order(inst, oid)

    # ---------- NEW live-order helpers -----------------
    async def _upsert_live_order(self, order: Order):
        coll = self.db[f"live_orders_{order.instrument_id}"]
        await coll.replace_one(
            {"order_id": order.order_id},
            {**order.__dict__},
            upsert=True,
        )

    async def _remove_live_order(self, inst: int, order_id: int):
        coll = self.db[f"live_orders_{inst}"]
        await coll.delete_one({"order_id": order_id})

    # ---------- called synchronously by Exchange -------
    def upsert_live_order(self, order: Order):
        asyncio.create_task(self._upsert_live_order(order))

    def remove_live_order(self, inst: int, order_id: int):
        self.log.info("removing live order %s for instr %s", order_id, inst)
        asyncio.create_task(self._remove_live_order(inst, order_id))