from fastapi import FastAPI, Body
from utils.logging import setup as setup_logging
from apps.exchange.exchange import Exchange
from apps.exchange.composite_writer import CompositeWriter
from apps.exchange.mongo_async_writer import MongoAsyncWriter
from apps.exchange.multicast_writer import MulticastWriter

setup_logging()

# writers ----------------------------------------------------------------
writer = CompositeWriter(MulticastWriter(), MongoAsyncWriter())
ex      = Exchange(writer)

app = FastAPI()

@app.post("/orders")
def new_order(payload: dict = Body(...)):
    return ex.handle_new_order(payload)

@app.post("/cancel")
def cancel(payload: dict = Body(...)):
    return ex.handle_cancel(payload)

@app.post("/new_book")
def new_book(payload: dict = Body(...)):
    return ex.create_order_book(payload["instrument_id"])
