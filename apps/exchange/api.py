# apps/exchange/api.py
from fastapi import FastAPI, Depends
from utils.logging import setup as setup_logging
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
async def new_book(
    payload: dict = Depends(AuthAdmin),
):
    return ex.create_order_book(payload["instrument_id"])

@app.on_event("shutdown")
async def unload_exchange_state():
    # Ensure we cleanly shut down the queued writer
    await queued_db_writer.shutdown()
    print("Exchange API shutdown complete.")
