# tests/test_exchange_full.py
from __future__ import annotations
import unittest, itertools
from time import time_ns
from typing import List, Dict

from apps.exchange.exchange import Exchange
from apps.exchange.models   import OrderType, Side


# ─────────────────── MockWriter helper ───────────────────────────────
class MockWriter:
    """
    • list_instruments   – returns preset instrument ids
    • iter_orders        – yields dict rows in timestamp order
    • record_* methods   – just log to self.calls list
    • create_instrument  – logs creation request
    """
    def __init__(self):
        # preload: instrument 1 with two resting orders    (bid & ask)
        self._orders: Dict[int, List[dict]] = {
            1: [
                {
                    "order_id":    1,
                    "order_type":  "GTC",
                    "side":        "BUY",
                    "price_cents": 10000,
                    "quantity":    5,
                    "timestamp":   time_ns(),
                    "party_id":    11,
                    "cancelled":   False,
                },
                {
                    "order_id":    2,
                    "order_type":  "GTC",
                    "side":        "SELL",
                    "price_cents": 10050,
                    "quantity":    7,
                    "timestamp":   time_ns(),
                    "party_id":    12,
                    "cancelled":   False,
                },
            ]
        }
        self.calls: List[tuple[str, tuple, dict]] = []  # fn_name, args, kwargs

    # ---- rebuild helpers -------------------------------------------
    def list_instruments(self) -> list[int]:
        self.calls.append(("list_instruments", (), {}))
        return list(self._orders.keys())

    def iter_orders(self, instrument_id: int):
        self.calls.append(("iter_orders", (instrument_id,), {}))
        for row in self._orders[instrument_id]:
            yield row

    # ---- live-time persistence -------------------------------------
    def record_order(self, order):
        self.calls.append(("record_order", (order,), {}))

    def record_trade(self, trade):
        self.calls.append(("record_trade", (trade,), {}))

    def record_cancel(self, instrument_id: int, order_id: int):
        self.calls.append(("record_cancel", (instrument_id, order_id), {}))

    # ---- new instrument --------------------------------------------
    def create_instrument(self, instrument_id: int):
        self.calls.append(("create_instrument", (instrument_id,), {}))
        self._orders[instrument_id] = []


# ─────────────────── Test-suite ──────────────────────────────────────
class ExchangeFullSuite(unittest.TestCase):
    def setUp(self):
        self.writer = MockWriter()
        self.ex = Exchange(self.writer)          # triggers rebuild

    # ---------- rebuild verified ------------------------------------
    def test_rebuild_called(self):
        self.assertIn(
            ("list_instruments", (), {}), self.writer.calls, "list_instruments not invoked"
        )
        self.assertIn(("iter_orders", (1,), {}), self.writer.calls)
        # Best bid/ask reflect rebuilt book
        book = self.ex._books[1]
        self.assertEqual(book.best_bid(), 10000)
        self.assertEqual(book.best_ask(), 10050)

    # ---------- new order path --------------------------------------
    def test_handle_new_order_gtc(self):
        payload = {
            "instrument_id": 1,
            "side": "BUY",
            "order_type": "GTC",
            "price_cents": 10020,
            "quantity": 3,
            "party_id": 77,
        }
        resp = self.ex.handle_new_order(payload)
        self.assertEqual(resp["status"], "ACCEPTED")
        # writer should receive exactly one order persist
        recs = [c for c in self.writer.calls if c[0] == "record_order"]
        self.assertTrue(recs and recs[-1][1][0].price_cents == 10020)

    # ---------- market order produces trade + writer trade call -----
    def test_market_trade_flow(self):
        payload = {
            "instrument_id": 1,
            "side": "BUY",
            "order_type": "MARKET",
            "quantity": 4,
            "party_id": 55,
        }
        resp = self.ex.handle_new_order(payload)
        self.assertEqual(resp["trades"][0]["price_cents"], 10050)
        # record_trade must be called
        self.assertTrue(any(c[0] == "record_trade" for c in self.writer.calls))

    # ---------- IOC residue cancelled automatically -----------------
    def test_ioc_residue_cancelled(self):
        payload = {
            "instrument_id": 1,
            "side": "BUY",
            "order_type": "IOC",
            "price_cents": 9950,
            "quantity": 10,
            "party_id": 88,
        }
        resp = self.ex.handle_new_order(payload)
        self.assertTrue(resp["cancelled"])
        # no trade because bid < best ask
        self.assertEqual(resp["trades"], [])

    # ---------- cancel happy path -----------------------------------
    def test_cancel_success(self):
        # cancel the ask rebuilt earlier (id=2)
        resp = self.ex.handle_cancel({"instrument_id": 1, "order_id": 2})
        self.assertEqual(resp["status"], "CANCELLED")
        self.assertTrue(
            any(c[0] == "record_cancel" and c[1][1] == 2 for c in self.writer.calls)
        )

    # ---------- cancel duplicate / miss -----------------------------
    def test_cancel_miss(self):
        self.ex.handle_cancel({"instrument_id": 1, "order_id": 2})  # first time
        resp = self.ex.handle_cancel({"instrument_id": 1, "order_id": 2})
        self.assertEqual(resp["status"], "ERROR")

    # ---------- validation errors -----------------------------------
    def test_validation_fail(self):
        bad = {
            "instrument_id": 1,
            "side": "BUY",
            "order_type": "GTC",
            # price missing
            "quantity": 1,
            "party_id": 99,
        }
        resp = self.ex.handle_new_order(bad)
        self.assertEqual(resp["status"], "ERROR")

    # ---------- dynamic book creation -------------------------------
    def test_create_new_book(self):
        out = self.ex.create_order_book(7)
        self.assertEqual(out, {"status": "CREATED", "instrument_id": 7})
        self.assertIn(7, self.ex._books)
        self.assertIn(("create_instrument", (7,), {}), self.writer.calls)

        # inserting into new book works
        payload = {
            "instrument_id": 7,
            "side": "SELL",
            "order_type": "GTC",
            "price_cents": 11000,
            "quantity": 2,
            "party_id": 66,
        }
        r = self.ex.handle_new_order(payload)
        self.assertEqual(r["status"], "ACCEPTED")

    # ---------- new book duplicate id -------------------------------
    def test_create_duplicate_book(self):
        self.ex.create_order_book(9)
        dup = self.ex.create_order_book(9)
        self.assertEqual(dup["status"], "ERROR")


if __name__ == "__main__":
    unittest.main(verbosity=2)
