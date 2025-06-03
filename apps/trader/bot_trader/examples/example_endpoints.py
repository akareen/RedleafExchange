#!/usr/bin/env python3
import sys
import pathlib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
# ─────────────────────────────────────────────────────────────────────────

from apps.trader.bot_trader.public_endpoints import (
    ExchangeClient,
    ExchangeClientConfig,
    ExchangeClientError,
    ValidationError,
    HTTPRequestError,
)

# Use environment variables (or your .env) to set:
#   API_URL, PARTY_ID, PASSWORD
config = ExchangeClientConfig()
client = ExchangeClient(config)

def pretty_print(label, resp):
    print(f"\n--- {label} ---")
    print(resp)

try:
    # ──────────── Instrument #100, Partial‐fill + Cancellation ────────────

    # 1) Create instrument 100
    resp = client.create_order_book(instrument_id=100)
    pretty_print("Create Book 100", resp)

    # 2) Place a GTC SELL 5 @ 10000
    resp = client.place_order(
        instrument_id=100,
        side="SELL",
        order_type="GTC",
        price_cents=10000,
        quantity=5
    )
    sell_100_oid = resp["order_id"]
    pretty_print("Placed SELL 100 ×5 @10000", resp)

    # 3) Place a crossing BUY 3 @ 10100 (should fill 3/5 of the sell)
    resp = client.place_order(
        instrument_id=100,
        side="BUY",
        order_type="GTC",
        price_cents=10100,
        quantity=3
    )
    buy_100_oid = resp["order_id"]
    pretty_print("Placed BUY 100 ×3 @10100", resp)

    # 4) Cancel the remaining SELL (2 units left)
    resp = client.cancel_order(
        instrument_id=100,
        order_id=sell_100_oid
    )
    pretty_print("Cancel remaining SELL 100", resp)

    # 5) Attempt to re‐create instrument 100 (should raise a ValidationError or HTTP 400)
    try:
        resp = client.create_order_book(instrument_id=100)
        pretty_print("Re‐create Book 100 (unexpectedly succeeded)", resp)
    except (ValidationError, HTTPRequestError, ExchangeClientError) as e:
        pretty_print("Re‐create Book 100 (expected error)", str(e))

    # ──────────── Instrument #200, Exact‐match ────────────

    # 6) Create instrument 200
    resp = client.create_order_book(instrument_id=200)
    pretty_print("Create Book 200", resp)

    # 7) Place a GTC SELL 10 @ 20000
    resp = client.place_order(
        instrument_id=200,
        side="SELL",
        order_type="GTC",
        price_cents=20000,
        quantity=10
    )
    sell_200_oid = resp["order_id"]
    pretty_print("Placed SELL 200 ×10 @20000", resp)

    # 8) Place a crossing BUY 10 @ 20000 (should fill 10/10 exactly)
    resp = client.place_order(
        instrument_id=200,
        side="BUY",
        order_type="GTC",
        price_cents=20000,
        quantity=10
    )
    buy_200_oid = resp["order_id"]
    pretty_print("Placed BUY 200 ×10 @20000", resp)

    # At this point there is no live order left on 200.

except ValidationError as ve:
    print("Validation error:", ve.details)
except HTTPRequestError as he:
    print("HTTP error:", he)
except ExchangeClientError as ce:
    print("Client error:", ce)
