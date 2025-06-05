# apps/exchange/api.py
from fastapi import FastAPI, Depends, HTTPException
from pymongo.errors import DuplicateKeyError
import datetime

from utils.logging import setup as setup_logging

from apps.exchange.exchange import Exchange
from apps.exchange.composite_writer import CompositeWriter
from apps.exchange.mongo_db_writer import MongoDbWriter
from apps.exchange.multicast_writer import MulticastWriter
from apps.exchange.text_backup_writer import TextBackupWriter
from apps.exchange.mongo_party_auth import Auth
from apps.exchange.settings import get_settings

SET = get_settings()

setup_logging()

multicast_writer = MulticastWriter()
db_writer = MongoDbWriter()
text_backup = TextBackupWriter(directory="text_backup")
writer = CompositeWriter(
    db_writer,
    multicast_writer,
    text_backup
)
ex = Exchange(writer)

app = FastAPI()

AuthCommon = Auth(require_admin=False)
AuthAdmin = Auth(require_admin=True)

@app.on_event("startup")
async def load_exchange_state():
    instruments_coll = db_writer.sync_db["instruments"]
    try:
        cursor = instruments_coll.find({}, {"instrument_id": 1})
    except Exception:
        cursor = []

    for doc in cursor:
        instr_id = doc.get("instrument_id")
        if instr_id is None:
            continue
        try:
            ex.create_order_book(instr_id)
        except Exception:
            pass

    await ex.rebuild_from_database(db_writer)
    await db_writer.startup()


@app.post("/orders")
async def new_order(
    payload: dict = Depends(AuthCommon),
):
    return ex.handle_new_order(payload)


@app.post("/cancel")
async def cancel(
    payload: dict = Depends(AuthCommon),
):
    return ex.handle_cancel(payload)


@app.post("/cancel_all")
async def cancel_all(
    payload: dict = Depends(AuthCommon),
):
    return ex.handle_cancel_all(payload)


@app.post("/new_book")
async def new_book(
    payload: dict = Depends(AuthAdmin),
):
    resp = ex.create_order_book(payload["instrument_id"])
    if resp["status"] == "CREATED":
        meta = {
            "instrument_id": payload["instrument_id"],
            "instrument_name": payload["instrument_name"],
            "instrument_description": payload.get("instrument_description", ""),
            "created_time": datetime.datetime.now(datetime.UTC),
            "created_by": payload["party_id"],
        }
        try:
            db_writer.sync_db["instruments"].insert_one(meta)
        except DuplicateKeyError:
            pass
    return resp


@app.on_event("shutdown")
async def unload_exchange_state():
    await db_writer.shutdown()


def _coll_exists(name: str) -> bool:
    return name in db_writer.sync_db.list_collection_names()


@app.get("/instruments")
def list_instruments():
    coll = db_writer.sync_db["instruments"]
    return list(coll.find({}, {"_id": 0}).sort("created_time", 1))


@app.get("/orders/{instrument_id}")
def list_all_orders(instrument_id: int):
    coll_name = f"orders_{instrument_id}"
    if not _coll_exists(coll_name):
        raise HTTPException(status_code=404, detail="instrument not found")
    coll = db_writer.sync_db[coll_name]
    return list(coll.find({}, {"_id": 0}).sort("order_id", 1))


@app.get("/live_orders/{instrument_id}")
def list_live_orders(instrument_id: int):
    coll_name = f"live_orders_{instrument_id}"
    if not _coll_exists(coll_name):
        raise HTTPException(status_code=404, detail="instrument not found")
    coll = db_writer.sync_db[coll_name]
    return list(coll.find({}, {"_id": 0}).sort("order_id", 1))


@app.get("/trades/{instrument_id}")
def list_trades(instrument_id: int):
    coll_name = f"trades_{instrument_id}"
    if not _coll_exists(coll_name):
        raise HTTPException(status_code=404, detail="instrument not found")
    coll = db_writer.sync_db[coll_name]
    return list(coll.find({}, {"_id": 0}).sort("timestamp", 1))


@app.get("/parties")
def list_parties():
    coll = db_writer.sync_db["parties"]
    return list(
        coll.find({}, {"_id": 0, "party_id": 1, "party_name": 1})
           .sort("party_id", 1)
    )


@app.get("/action_count_seq")
def action_count_seq():
    doc = db_writer.sync_db["counters"].find_one({"_id": "action_count"})
    if not doc:
        raise HTTPException(status_code=404, detail="action_count counter not found")
    return {"seq": doc["seq"]}
