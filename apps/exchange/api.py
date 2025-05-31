from fastapi import FastAPI, Body, Request, HTTPException, status, Depends

from utils.logging import setup as setup_logging
from apps.exchange.exchange import Exchange
from apps.exchange.composite_writer import CompositeWriter
from apps.exchange.mongo_async_writer import MongoAsyncWriter
from apps.exchange.multicast_writer import MulticastWriter
from apps.exchange.mongo_party_auth import MongoPartyAuth

setup_logging()

# writers ----------------------------------------------------------------
writer = CompositeWriter(MulticastWriter(), MongoAsyncWriter())
ex      = Exchange(writer)
app = FastAPI()

# ---------- common auth (all endpoints) ------------------------------
async def require_auth(request: Request, payload: dict = Body(...)):
    pid = payload.get("party_id")
    pwd = payload.get("password", "")
    if not await MongoPartyAuth.verify(pid, pwd):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="invalid party_id / password")
    request.state.party = await MongoPartyAuth.get(pid)     # pass whole record
    return payload                                          # Route receives it

# ---------- stricter auth for admin-only end-points ------------------
async def require_admin(payload: dict = Depends(require_auth),
                        request: Request = None):
    if not request.state.party.get("is_admin"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="admin privileges required")
    return payload

# ----------- routes ---------------------------------------------------
@app.post("/orders")
def new_order(payload: dict = Depends(require_auth)):
    return ex.handle_new_order(payload)               # unchanged

@app.post("/cancel")
def cancel(payload: dict = Depends(require_auth)):
    return ex.handle_cancel(payload)

@app.post("/new_book")
def new_book(payload: dict = Depends(require_admin)):
    return ex.create_order_book(payload["instrument_id"])
