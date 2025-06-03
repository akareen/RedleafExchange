#!/usr/bin/env python3
"""
Populate a *test* instrument with a ladder of resting orders and a few
cross-trades so the UI has something juicy to display.

Usage:
    python scripts/populate_dummy_test_instrument.py
Requires:
    • API_URL, PARTY_ID, PASSWORD in your .env   (or hard-code below)
"""

import random, time
from apps.trader.bot_trader.public_endpoints import (
    ExchangeClient, ExchangeClientConfig, ExchangeClientError
)

API = ExchangeClient(ExchangeClientConfig())

INSTRUMENT_ID   = 1
INSTRUMENT_NAME = "test_instrument_2"
DESC            = "A dummy book filled with GTC orders by party 1."
PASSWORD        = ""

def main():
    try:
        API.create_order_book(
            instrument_id      = INSTRUMENT_ID,
            instrument_name=INSTRUMENT_NAME,
            instrument_description=DESC,
            admin_party_id     = "Adam",
            admin_password     = PASSWORD,
        )
    except ExchangeClientError:
        pass  # already there

    px_mid = 10000
    for i in range(1, 21):
        API.place_order(
            instrument_id = INSTRUMENT_ID,
            side          = "BUY",
            order_type    = "GTC",
            price_cents   = px_mid - i * 5,
            quantity      = random.randint(1, 5),
            party_id      = 1,
            password      = PASSWORD,
        )
        API.place_order(
            instrument_id = INSTRUMENT_ID,
            side          = "SELL",
            order_type    = "GTC",
            price_cents   = px_mid + i * 5,
            quantity      = random.randint(1, 5),
            party_id      = 1,
            password      = PASSWORD,
        )

    # 3) shoot a few market pokes to generate trades
    for _ in range(10):
        API.place_order(
            instrument_id = INSTRUMENT_ID,
            side          = random.choice(["BUY", "SELL"]),
            order_type    = "MARKET",
            quantity      = random.randint(1, 3),
            party_id      = 1,
            password      = PASSWORD,
        )
        time.sleep(0.1)

if __name__ == "__main__":
    main()
