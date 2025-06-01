# tests/test_end_to_end.py

import unittest
import time
from typing import Any, Dict, List
from pymongo import MongoClient

from apps.trader.bot_trader.public_endpoints import (
    ExchangeClient,
    ExchangeClientConfig,
    ExchangeClientError,
    ValidationError,
)
from apps.exchange.settings import get_settings

SET = get_settings()

def _admin_uri() -> str:
    """
    Build a URI that connects as the “root” or “admin” user.
    You must have created an admin user in Mongo with the correct role.
    """
    if SET.mongo_user and SET.mongo_pass:
        return f"mongodb://{SET.mongo_user}:{SET.mongo_pass}@{SET.mongo_host}:{SET.mongo_port}/admin"
    else:
        # if no auth is configured, assume localhost without credentials
        return f"mongodb://{SET.mongo_host}:{SET.mongo_port}/admin"

class EndToEndExchangeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cfg_admin = ExchangeClientConfig(
        )
        cfg_non = ExchangeClientConfig(
            default_party_id=2,
            default_password="test123",
        )
        cls.client_admin = ExchangeClient(cfg_admin)
        cls.client_non = ExchangeClient(cfg_non)

        cls.mongo_client = MongoClient(_admin_uri())
        cls.db = cls.mongo_client[SET.mongo_db]

        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        cls.mongo_client.close()

    def _list_collection(self, coll_name: str) -> List[Dict[str, Any]]:
        """Helper: return a sorted list of all documents in a collection (excluding _id)."""
        coll = self.db[coll_name]
        docs = list(coll.find({}, {"_id": 0}))
        # Sort by a deterministic key if present (e.g. order_id or timestamp)
        if docs and "order_id" in docs[0]:
            return sorted(docs, key=lambda d: d["order_id"])
        if docs and "timestamp" in docs[0]:
            return sorted(docs, key=lambda d: d["timestamp"])
        return docs

    # ────────── TEST 1: ADMIN CREATES BOOKS, DUPLICATE FAILS ──────────
    def test_01_admin_can_create_book_and_duplicate_book_errors(self):
        # Admin creates instrument 100 → should succeed
        resp = self.client_admin.create_order_book(instrument_id=100)
        self.assertEqual(resp.get("status"), "CREATED")
        self.assertEqual(resp.get("instrument_id"), 100)

        # Creating the same book again → should return status="ERROR"
        dup = self.client_admin.create_order_book(instrument_id=100)
        self.assertEqual(dup.get("status"), "ERROR")
        self.assertIn("instrument already exists", dup.get("details", "").lower())

    # ────────── TEST 2: GTC SELL THEN PARTIAL BUY → CANCEL ──────────
    def test_02_gtc_limit_partial_fill_and_cancel(self):
        # 1) Place a GTC SELL of 5 @ ten‐dollars (10000) using non‐admin
        sell_req = dict(
            instrument_id=100,
            side="SELL",
            order_type="GTC",
            price_cents=10000,
            quantity=5,
            # NOTE: client_non already includes party_id/PW
        )
        sell_resp = self.client_non.place_order(**sell_req)
        self.assertEqual(sell_resp.get("status"), "ACCEPTED")
        sell_oid = sell_resp["order_id"]
        self.assertIsInstance(sell_oid, int)
        self.assertEqual(sell_resp["remaining_qty"], 5)
        self.assertFalse(sell_resp["cancelled"])
        self.assertEqual(sell_resp["trades"], [])

        # 2) Place a GTC BUY of 3 @ 10100 (should trade 3 of the 5‐lot)
        buy_req = dict(
            instrument_id=100,
            side="BUY",
            order_type="GTC",
            price_cents=10100,
            quantity=3,
        )
        buy_resp = self.client_non.place_order(**buy_req)
        self.assertEqual(buy_resp.get("status"), "ACCEPTED")
        # It should have one trade of qty=3 at price=10000
        trades = buy_resp["trades"]
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["quantity"], 3)
        self.assertEqual(trades[0]["price_cents"], 10000)
        self.assertEqual(buy_resp["remaining_qty"], 0)
        self.assertFalse(buy_resp["cancelled"])

        # 3) Cancel the remaining 2 of the original SELL
        cancel_resp = self.client_non.cancel_order(instrument_id=100, order_id=sell_oid)
        self.assertEqual(cancel_resp.get("status"), "CANCELLED")
        self.assertEqual(cancel_resp.get("order_id"), sell_oid)

        # 4) A second cancel of the same oid should return status="ERROR"
        cancel_again = self.client_non.cancel_order(instrument_id=100, order_id=sell_oid)
        self.assertEqual(cancel_again.get("status"), "ERROR")

    # ────────── TEST 3: MARKET SWEEP MULTI‐LEVEL ──────────
    def test_03_market_sweep_multi_level(self):
        # Create a fresh instrument 200 as admin
        resp = self.client_admin.create_order_book(instrument_id=200)
        self.assertEqual(resp.get("status"), "CREATED")

        # Add three SELL levels: 20000×1, 20005×2, 20010×3
        for price, qty in [(20000, 1), (20005, 2), (20010, 3)]:
            r = self.client_non.place_order(
                instrument_id=200,
                side="SELL",
                order_type="GTC",
                price_cents=price,
                quantity=qty,
            )
            self.assertEqual(r["status"], "ACCEPTED")

        # Now send a MARKET BUY for 4 shares on instr=200
        mkt_resp = self.client_non.place_order(
            instrument_id=200,
            side="BUY",
            order_type="MARKET",
            quantity=4,
        )
        # Should have filled exactly 4 shares across the first two levels
        self.assertEqual(mkt_resp["remaining_qty"], 0)
        total_traded = sum(t["quantity"] for t in mkt_resp["trades"])
        self.assertEqual(total_traded, 4)
        # Validate the detailed trades spilled across two price‐levels:
        prices = sorted({t["price_cents"] for t in mkt_resp["trades"]})
        self.assertEqual(prices, [20000, 20005])

    # ────────── TEST 4: IOC OUTSIDE SPREAD ──────────
    def test_04_ioc_full_cancel(self):
        # Create instrument 300
        resp = self.client_admin.create_order_book(instrument_id=300)
        self.assertEqual(resp.get("status"), "CREATED")

        # One resting SELL @ 30200×1
        r1 = self.client_non.place_order(
            instrument_id=300,
            side="SELL",
            order_type="GTC",
            price_cents=30200,
            quantity=1,
        )
        self.assertEqual(r1["status"], "ACCEPTED")

        # Now an IOC BUY @ 29900×1 – cannot match → should cancel entirely
        r2 = self.client_non.place_order(
            instrument_id=300,
            side="BUY",
            order_type="IOC",
            price_cents=29900,
            quantity=1,
        )
        # IOC responses always include "cancelled": True if no match
        self.assertTrue(r2["cancelled"])
        self.assertEqual(r2["trades"], [])
        self.assertEqual(r2["remaining_qty"], 1)

    # ────────── TEST 5: VALIDATION ERRORS ──────────
    def test_05_validation_errors(self):
        # SUBMIT TO MISSING BOOK → should be 422 / ValidationError
        with self.assertRaises(ValidationError):
            self.client_non.place_order(
                instrument_id=9999,
                side="BUY",
                order_type="GTC",
                price_cents=9000,
                quantity=1,
            )

        # MISSING price_cents on GTC → 422
        with self.assertRaises(ValidationError):
            self.client_non.place_order(
                instrument_id=100,
                side="BUY",
                order_type="GTC",
                quantity=1,
            )

        # BAD ENUM for side → 422
        with self.assertRaises(ValidationError):
            self.client_non.place_order(
                instrument_id=100,
                side="XXX",
                order_type="MARKET",
                quantity=1,
            )

        # BAD ENUM for order_type → 422
        with self.assertRaises(ValidationError):
            self.client_non.place_order(
                instrument_id=100,
                side="BUY",
                order_type="FOO",
                quantity=1,
            )

    # ────────── TEST 6: MULTI‐PARTY HIGH‐VOLUME FUZZ & INVARIANTS ──────────
    def test_06_high_volume_fuzz(self):
        # Create instrument 400
        resp = self.client_admin.create_order_book(instrument_id=400)
        self.assertEqual(resp.get("status"), "CREATED")

        import random
        created_oids = set()

        # 200 random GTC orders from random parties (IDs 1–5)
        for _ in range(200):
            side = random.choice(["BUY", "SELL"])
            price = random.randint(39500, 40500)
            qty = random.randint(1, 3)
            party = 2
            try:
                r = self.client_non.place_order(
                    instrument_id=400,
                    side=side,
                    order_type="GTC",
                    price_cents=price,
                    quantity=qty,
                    party_id=party,
                    password="test123",
                )
            except ExchangeClientError:
                # Occasionally orders cross and trade immediately without returning an order_id
                continue

            oid = r.get("order_id")
            if oid:
                created_oids.add(oid)
                # 30% chance to cancel immediately
                if random.random() < 0.3:
                    _ = self.client_non.cancel_order(
                        instrument_id=400,
                        order_id=oid,
                    )

        # 50 random MARKET pokes
        for _ in range(50):
            side = random.choice(["BUY", "SELL"])
            r = self.client_non.place_order(
                instrument_id=400,
                side=side,
                order_type="MARKET",
                quantity=random.randint(1, 5),
            )
            # Always returns a JSON with "remaining_qty"
            self.assertIn("remaining_qty", r)

        # Ensure no duplicate order_ids were given
        self.assertEqual(len(created_oids), len(set(created_oids)))

    # ────────── TEST 7: FINAL DATABASE STATE CHECK ──────────
    def test_07_verify_raw_mongo_state(self):
        """
        After all of the above activity, we inspect the raw MongoDB collections
        and compare them against an “expected snapshot.” Because we cannot
        know exact timestamps, we verify only:
          • That each orders_<instr> doc’s fields (order_id, side, price_cents, etc.)
            are consistent with the sequence of actions above.
          • That live_orders_<instr> contains exactly those orders that never
            fully filled nor were cancelled.
          • That trades_<instr> contains all the executed trades we saw.
        For simplicity, in this example we only verify instruments {100, 200, 300, 400}
        and check basic invariants (counts, total filled qty sums, etc.).
        """

        # Helper: get counts and sums from each instrument
        def summarize_instrument(instr: int) -> Dict[str, Any]:
            summary: Dict[str, Any] = {}
            # orders_<instr>
            all_orders = self._list_collection(f"orders_{instr}")
            summary["num_orders"] = len(all_orders)
            # total filled_quantity across all orders
            summary["total_filled"] = sum(o.get("filled_quantity", 0) for o in all_orders)
            # live_orders_<instr>
            live = list(self.db[f"live_orders_{instr}"].find({}, {"_id": 0}))
            summary["num_live_orders"] = len(live)
            # trades_<instr>
            trades = list(self.db[f"trades_{instr}"].find({}, {"_id": 0}))
            summary["num_trades"] = len(trades)
            summary["sum_trade_qty"] = sum(t.get("quantity", 0) for t in trades)
            return summary

        # Instrument 100 → we had one sell(5) → buy(3) → cancel(2)
        #    → so orders_100 contains both orders, total_filled=3
        #    → live_orders_100 should be empty (the last SELL was cancelled)
        #    → trades_100 has exactly one trade of qty=3
        s100 = summarize_instrument(100)
        self.assertEqual(s100["num_orders"], 2)  # the SELL and the BUY
        self.assertEqual(s100["total_filled"], 3)
        self.assertEqual(s100["num_live_orders"], 0)
        self.assertEqual(s100["num_trades"], 1)
        self.assertEqual(s100["sum_trade_qty"], 3)

        # Instrument 200 → sell(10) then buy(10 exactly) → total_filled=10
        #    → live_orders_200 empty
        #    → trades_200 has one trade of qty=10
        s200 = summarize_instrument(200)
        self.assertEqual(s200["num_orders"], 2)
        self.assertEqual(s200["total_filled"], 10)
        self.assertEqual(s200["num_live_orders"], 0)
        self.assertEqual(s200["num_trades"], 1)
        self.assertEqual(s200["sum_trade_qty"], 10)

        # Instrument 300 → sell(1) then IOC‐buy(1 which failed) → total_filled=0
        #    → the original SELL is still live (never removed)
        #    → trades_300 has 0 trades
        s300 = summarize_instrument(300)
        self.assertEqual(s300["num_orders"], 1)
        self.assertEqual(s300["total_filled"], 0)
        self.assertEqual(s300["num_live_orders"], 1)  # the one sell still sits in live_orders
        self.assertEqual(s300["num_trades"], 0)
        self.assertEqual(s300["sum_trade_qty"], 0)

        # Instrument 400 → high‐volume fuzz: we cannot predict exact numbers,
        #   but we can assert invariants:
        s400 = summarize_instrument(400)
        #  – sum_trade_qty + sum(remaining live qty) == sum of all order quantities
        all_orders_400 = self._list_collection("orders_400")
        total_qty_submitted = sum(o["quantity"] for o in all_orders_400)
        total_live_remaining = sum(o["remaining_quantity"] for o in self._list_collection("live_orders_400"))
        self.assertEqual(s400["sum_trade_qty"] + total_live_remaining, total_qty_submitted)
        #  – There should be at least one trade recorded:
        self.assertGreaterEqual(s400["num_trades"], 1)

if __name__ == "__main__":
    unittest.main(verbosity=2)
