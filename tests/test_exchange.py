from __future__ import annotations
import unittest, random
from typing import Dict, List, Tuple
from dataclasses import asdict
from time import time_ns

from fastapi import FastAPI, Body, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.testclient import TestClient

from tests.conftest import PWD


# ───────────── DummyWriter ───────────────────────────────────────────
class DummyWriter:
    def __init__(self):
        self.orders:  List[dict] = []
        self.trades:  List[dict] = []
        self.cancels: List[Tuple[int, int]] = []
        self.created: List[int]  = []
        self._orders_by_instr: Dict[int, List[dict]] = {}

    # ---- rebuild helpers -------------------------------------------
    def list_instruments(self): return list(self._orders_by_instr.keys())
    def iter_orders(self, i):   return self._orders_by_instr.get(i, [])
    def create_instrument(self, i): self.created.append(i); self._orders_by_instr.setdefault(i, [])

    # ---- live persist ----------------------------------------------
    def record_order (self, o): self.orders.append(o.__dict__)
    def record_trade (self, t): self.trades.append(t.__dict__)
    def record_cancel(self, i, oid): self.cancels.append((i, oid))

    # ---- live-order (new) ---------------------------------------------
    def upsert_live_order(self, order):  # called when a resting order is accepted
        pass

    def remove_live_order(self, inst: int, order_id: int):  # called on fill / cancel
        pass


# ───────────── helper: in-memory FastAPI + Exchange ------------------
def make_client() -> tuple[TestClient, DummyWriter]:
    from apps.exchange.exchange         import Exchange
    from apps.exchange.composite_writer import CompositeWriter

    dummy = DummyWriter()
    ex = Exchange(CompositeWriter(dummy))          # no DB/auth layer

    app = FastAPI()

    @app.post("/orders")
    def _orders(p: dict = Body(...)):
        out = ex.handle_new_order(p)
        if out.get("status") == "ERROR":
            raise HTTPException(status_code=422, detail=jsonable_encoder(out["details"]))
        return out

    @app.post("/cancel")
    def _cancel(p: dict = Body(...)):
        return ex.handle_cancel(p)

    @app.post("/new_book")
    def _new_book(p: dict = Body(...)):
        return ex.create_order_book(p["instrument_id"])

    return TestClient(app), dummy


# ───────────── Integration tests ─────────────────────────────────────
class APIFullIntegration(unittest.TestCase):

    def setUp(self):
        self.client, self.w = make_client()

    # ----- /new_book -------------------------------------------------
    def test_new_book_and_duplicate(self):
        self.assertEqual(self.client.post("/new_book", json={"instrument_id": 10}).status_code, 200)
        dup = self.client.post("/new_book", json={"instrument_id": 10}).json()
        self.assertEqual(dup["status"], "ERROR")

    # ----- GTC life-cycle -------------------------------------------
    def test_gtc_limit_lifecycle(self):
        self.client.post("/new_book", json={"instrument_id": 1})

        ask = dict(instrument_id=1, side="SELL", order_type="GTC",
                   price_cents=10500, quantity=5, party_id=1, password=PWD)
        ask_id = self.client.post("/orders", json=ask).json()["order_id"]

        bid = dict(instrument_id=1, side="BUY", order_type="GTC",
                   price_cents=11000, quantity=3, party_id=2, password=PWD)
        trades = self.client.post("/orders", json=bid).json()["trades"]
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["quantity"], 3)

        cancel = dict(instrument_id=1, order_id=ask_id,
                      party_id=99, password=PWD)
        self.assertEqual(self.client.post("/cancel", json=cancel).json()["status"], "CANCELLED")

    # ----- MARKET sweep multi-level ---------------------------------
    def test_market_sweep_multi_level(self):
        self.client.post("/new_book", json={"instrument_id": 2})
        for px, qty in [(10000, 1), (10005, 2), (10010, 3)]:
            self.client.post("/orders", json=dict(
                instrument_id=2, side="SELL", order_type="GTC",
                price_cents=px, quantity=qty, party_id=9, password=PWD))

        mkt = dict(instrument_id=2, side="BUY", order_type="MARKET",
                   quantity=4, party_id=77, password=PWD)
        r = self.client.post("/orders", json=mkt).json()
        self.assertEqual(r["remaining_qty"], 0)
        self.assertEqual(sum(t["quantity"] for t in r["trades"]), 4)

    # ----- MARKET on empty book -------------------------------------
    def test_market_on_empty_book(self):
        self.client.post("/new_book", json={"instrument_id": 3})
        r = self.client.post("/orders", json=dict(
            instrument_id=3, side="BUY", order_type="MARKET",
            quantity=2, party_id=5, password=PWD)).json()
        self.assertEqual(r["trades"], [])
        self.assertEqual(r["remaining_qty"], 2)

    # ----- IOC outside spread ---------------------------------------
    def test_ioc_full_cancel(self):
        self.client.post("/new_book", json={"instrument_id": 4})
        self.client.post("/orders", json=dict(
            instrument_id=4, side="SELL", order_type="GTC",
            price_cents=10200, quantity=1, party_id=8, password=PWD))
        r = self.client.post("/orders", json=dict(
            instrument_id=4, side="BUY", order_type="IOC",
            price_cents=9900, quantity=1, party_id=9, password=PWD)).json()
        self.assertTrue(r["cancelled"] is True)

    # ----- Validation errors ----------------------------------------
    def test_validation_errors(self):
        bads = [
            dict(instrument_id=5, side="BUY", order_type="GTC", quantity=1,
                 party_id=1, password=PWD),                         # price missing
            dict(instrument_id=5, side="XXX", order_type="MARKET", quantity=1,
                 party_id=1, password=PWD),                         # bad side
            dict(instrument_id=5, side="BUY", order_type="FOO", quantity=1,
                 party_id=1, password=PWD),                         # bad order_type
        ]
        for b in bads:
            with self.subTest(b=b):
                self.assertEqual(self.client.post("/orders", json=b).status_code, 422)

    # ----- cancel edge cases ----------------------------------------
    def test_cancel_edge_cases(self):
        self.client.post("/new_book", json={"instrument_id": 6})
        place = self.client.post("/orders", json=dict(
            instrument_id=6, side="SELL", order_type="GTC",
            price_cents=9999, quantity=1, party_id=44, password=PWD)).json()
        oid = place["order_id"]

        ok = dict(instrument_id=6, order_id=oid, party_id=1, password=PWD)
        self.assertEqual(self.client.post("/cancel", json=ok).json()["status"], "CANCELLED")
        self.assertEqual(self.client.post("/cancel", json=ok).json()["status"], "ERROR")

    # ----- OID monotonicity -----------------------------------------
    def test_oid_monotonicity(self):
        self.client.post("/new_book", json={"instrument_id": 7})
        oids = []
        for i in range(5):
            r = self.client.post("/orders", json=dict(
                instrument_id=7, side="BUY", order_type="GTC",
                price_cents=7000+i, quantity=1, party_id=2, password=PWD)).json()
            oids.append(r["order_id"])
        self.assertEqual(oids, sorted(oids))


    # ----- high-volume fuzz -----------------------------------------
    def test_high_volume_fuzz(self):
        self.client.post("/new_book", json={"instrument_id": 10})
        oids: List[int] = []
        for _ in range(200):
            side = random.choice(["BUY", "SELL"])
            px   = random.randint(9000, 11000)
            qty  = random.randint(1, 3)
            r = self.client.post("/orders", json=dict(
                instrument_id=10, side=side, order_type="GTC",
                price_cents=px, quantity=qty,
                party_id=random.randint(1, 5), password=PWD)).json()
            if "order_id" in r: oids.append(r["order_id"])
            if "order_id" in r and random.random() < 0.3:
                self.client.post("/cancel", json=dict(
                    instrument_id=10, order_id=r["order_id"],
                    party_id=1, password=PWD))

        # 50 market pokes
        for _ in range(50):
            side = random.choice(["BUY", "SELL"])
            self.assertIn("remaining_qty", self.client.post(
                "/orders", json=dict(instrument_id=10, side=side,
                order_type="MARKET", quantity=random.randint(1,5),
                party_id=99, password=PWD)).json())

        self.assertEqual(len(set(oids)), len(oids))

    # ----- cancel on missing book -----------------------------------
    def test_cancel_missing_book(self):
        bad = dict(instrument_id=123, order_id=1, party_id=1, password=PWD)
        j = self.client.post("/cancel", json=bad).json()
        self.assertEqual(j["status"], "ERROR")


if __name__ == "__main__":
    unittest.main(verbosity=2)
