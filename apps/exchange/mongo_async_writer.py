# apps/exchange/mongo_async_writer.py
import asyncio, logging
from urllib.parse import quote_plus
from motor.motor_asyncio import AsyncIOMotorClient

from apps.exchange.settings import get_settings

log = logging.getLogger("MongoAsyncWriter")
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
        log.info("Mongo connected %s", _mongo_uri())

    # ------------ rebuild helpers (sync wrappers) -------------------
    def list_instruments(self):
        names = asyncio.get_event_loop().run_until_complete(self.db.list_collection_names())
        return [int(n.split("_")[1]) for n in names if n.startswith("orders_")]

    def iter_orders(self, instr):
        async def _collect():
            return [doc async for doc in self._stream_orders(instr)]
        return asyncio.get_event_loop().run_until_complete(_collect())

    async def _stream_orders(self, instr):
        coll = self.db[f"orders_{instr}"]
        cursor = coll.find().sort("timestamp", 1)
        count = 0
        async for doc in cursor:
            doc.pop("_id", None)
            yield doc
            count += 1
            if count % 100000 == 0:
                log.info("rebuild %s streamed %dk rows", instr, count // 1000)

    def create_instrument(self, instr):
        self.db[f"orders_{instr}"].create_index("order_id", unique=True)
        self.db[f"trades_{instr}"].create_index("timestamp")

    # ------------ async writes --------------------------------------
    async def _insert(self, coll, doc):
        await coll.insert_one(doc)

    async def _upsert(self, coll, key, doc):
        await coll.replace_one(key, doc, upsert=True)

    def record_order(self, order):
        coll = self.db[f"orders_{order.instrument_id}"]
        asyncio.create_task(self._upsert(coll, {"order_id": order.order_id}, order.__dict__))

    def record_trade(self, trade):
        coll = self.db[f"trades_{trade.instrument_id}"]
        asyncio.create_task(self._insert(coll, trade.__dict__))

    def record_cancel(self, instr, oid):
        coll = self.db[f"orders_{instr}"]
        asyncio.create_task(
            coll.update_one({"order_id": oid}, {"$set": {"cancelled": True, "quantity": 0}})
        )
