# apps/exchange/api.py
from fastapi import FastAPI, Depends, HTTPException
from utils.logging import setup as setup_logging
import datetime
from pymongo.errors import DuplicateKeyError

from apps.exchange.exchange import Exchange
from apps.exchange.composite_writer import CompositeWriter
from apps.exchange.mongo_queued_db_writer import QueuedDbWriter
from apps.exchange.multicast_writer import MulticastWriter
from apps.exchange.text_backup_writer import TextBackupWriter
from apps.exchange.mongo_party_auth import Auth

from apps.exchange.settings import get_settings

SET = get_settings()
print("→ Loaded Settings:", SET.mongo_user, SET.mongo_pass, SET.mongo_db)

setup_logging()

# Instantiate the composite writer and exchange
multicast_writer = MulticastWriter()
queued_db_writer = QueuedDbWriter()
text_backup  = TextBackupWriter(directory="text_backup")
writer = CompositeWriter(
    multicast_writer,
    queued_db_writer,
    text_backup
)
ex     = Exchange(writer)

app = FastAPI()

# Two “flavors” of Auth dependency: normal vs. admin
AuthCommon = Auth(require_admin=False)
AuthAdmin  = Auth(require_admin=True)


@app.on_event("startup")
async def load_exchange_state():
    # On startup, read any existing orders from Mongo into in‐memory books
    await ex.rebuild_from_database()
    await queued_db_writer.startup()


@app.post("/orders")
async def new_order(
    payload: dict = Depends(AuthCommon),
):
    # `payload` is the JSON body after AuthCommon verified party_id/password
    # You can still do `party_doc = request.state.party` if needed, but here we only need payload.
    return ex.handle_new_order(payload)


@app.post("/cancel")
async def cancel(
    payload: dict = Depends(AuthCommon),
):
    return ex.handle_cancel(payload)


@app.post("/new_book")
async def new_book(payload: dict = Depends(AuthAdmin)):
    """
    JSON body must now include:
      • instrument_id   (int, as before)
      • instrument_name (str, required)
      • instrument_description (str, optional but recommended)
    """
    # 1) create the in-memory order-book as before
    resp = ex.create_order_book(payload["instrument_id"])

    # 2) persist the instrument meta in Mongo so the UI can query it quickly
    if resp["status"] == "CREATED":
        meta = {
            "instrument_id":       payload["instrument_id"],
            "instrument_name":     payload["instrument_name"],
            "instrument_description": payload.get("instrument_description", ""),
            "created_time":        datetime.datetime.now(datetime.UTC),
            "created_by":          payload["party_id"],
        }
        try:
            queued_db_writer.sync_db["instruments"].insert_one(meta)
        except DuplicateKeyError:
            # should not happen because Exchange already rejected the dup,
            # but we ignore the race gracefully
            pass

    return resp

@app.on_event("shutdown")
async def unload_exchange_state():
    # Ensure we cleanly shut down the queued writer
    await queued_db_writer.shutdown()
    print("Exchange API shutdown complete.")

def _coll_exists(name: str) -> bool:
    return name in queued_db_writer.sync_db.list_collection_names()

@app.get("/instruments")
def list_instruments():
    """
    Return *all* rows in the `instruments` collection (no auth required).
    """
    coll = queued_db_writer.sync_db["instruments"]
    # exclude _id for cleanliness; sort oldest→newest
    return list(coll.find({}, {"_id": 0}).sort("created_time", 1))

@app.get("/orders/{instrument_id}")
def list_all_orders(instrument_id: int):
    coll_name = f"orders_{instrument_id}"
    if not _coll_exists(coll_name):
        raise HTTPException(status_code=404, detail="instrument not found")
    coll = queued_db_writer.sync_db[coll_name]
    return list(coll.find({}, {"_id": 0}).sort("order_id", 1))

@app.get("/live_orders/{instrument_id}")
def list_live_orders(instrument_id: int):
    coll_name = f"live_orders_{instrument_id}"
    if not _coll_exists(coll_name):
        raise HTTPException(status_code=404, detail="instrument not found")
    coll = queued_db_writer.sync_db[coll_name]
    return list(coll.find({}, {"_id": 0}).sort("order_id", 1))

@app.get("/trades/{instrument_id}")
def list_trades(instrument_id: int):
    coll_name = f"trades_{instrument_id}"
    if not _coll_exists(coll_name):
        raise HTTPException(status_code=404, detail="instrument not found")
    coll = queued_db_writer.sync_db[coll_name]
    return list(coll.find({}, {"_id": 0}).sort("timestamp", 1))

@app.get("/parties")
def list_parties():
    """
    Open end-point – read-only – returns every party’s friendly name.
    Schema: [{ "party_id": 12, "party_name": "Mega-Fund" }, …]
    """
    coll = queued_db_writer.sync_db["parties"]
    return list(
        coll.find({}, {"_id": 0, "party_id": 1, "party_name": 1})
           .sort("party_id", 1)
    )