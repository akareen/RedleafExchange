from pymongo import MongoClient
from pymongo.errors import CollectionInvalid
from typing import Any, Dict, List

from apps.exchange.models import Order, Trade
from apps.exchange.settings import get_settings, admin_uri

SET = get_settings()


class MongoDbWriter:
    def __init__(self):
        self._sync_client = MongoClient(admin_uri())
        self.sync_db = self._sync_client[SET.mongo_db]

    # ───────── rebuild‐time helpers ────────────────────────────────
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
        return list(cursor)

    def create_instrument(self, instrument_id: str) -> None:
        for suffix in ("orders", "live_orders", "trades"):
            try:
                self.sync_db.create_collection(f"{suffix}_{instrument_id}")
            except CollectionInvalid:
                pass

    # ───────── hot-path methods (now immediate) ─────────────────────
    def record_order(self, order: Order) -> None:
        self._increment_action_count()
        coll = self.sync_db[f"orders_{order.instrument_id}"]
        coll.replace_one({"order_id": order.order_id}, order.__dict__, upsert=True)

    def record_trade(self, trade: Trade) -> None:
        self._increment_action_count()
        coll = self.sync_db[f"trades_{trade.instrument_id}"]
        coll.insert_one(trade.__dict__)

    def record_cancel(self, instrument_id: int, order_id: int) -> None:
        self._increment_action_count()
        coll = self.sync_db[f"live_orders_{instrument_id}"]
        coll.delete_one({"order_id": order_id})

    def upsert_live_order(self, order: Order) -> None:
        self._increment_action_count()
        coll = self.sync_db[f"live_orders_{order.instrument_id}"]
        coll.replace_one({"order_id": order.order_id}, order.__dict__, upsert=True)

    def remove_live_order(self, inst: int, order_id: int) -> None:
        self._increment_action_count()
        coll = self.sync_db[f"live_orders_{inst}"]
        coll.delete_one({"order_id": order_id})

    def update_order_quantity(self, instrument_id: int, order_id: int, quantity_modification: int) -> None:
        self._increment_action_count()
        coll = self.sync_db[f"live_orders_{instrument_id}"]
        coll.update_one(
            {"order_id": order_id},
            {"$inc": {
                "remaining_quantity": -quantity_modification,
                "filled_quantity": quantity_modification
            }}
        )

    # ───────── internal: update global action counter ──────────────
    def _increment_action_count(self) -> None:
        self.sync_db["counters"].find_one_and_update(
            {"_id": "action_count"},
            {"$inc": {"seq": 1}},
            upsert=True
        )

    # ───────── legacy lifecycle methods (noop now) ─────────────────
    async def startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        pass
