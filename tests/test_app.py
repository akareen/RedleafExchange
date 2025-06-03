# tests/test_app.py

from __future__ import annotations
import unittest
from typing import Dict, List, Tuple
from time import time_ns
from dataclasses import asdict

from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastapi.encoders import jsonable_encoder
from pymsgbox import password

from tests.conftest import PWD

# ───────────── DummyWriter: implements Writer interface ─────────────
class DummyWriter:
    def __init__(self):
        self.orders   : List[dict] = []
        self.trades   : List[dict] = []
        self.cancels  : List[Tuple[int,int]] = []
        self.created  : List[int]  = []
        # rebuild store (instrument_id -> list of order‐dicts)
        self._orders_by_instr : Dict[int, List[dict]] = {}

    # ---- rebuild helpers -------------------------------------------
    def list_instruments(self) -> List[int]:
        # Return all keys—empty list if none
        return list(self._orders_by_instr.keys())

    def iter_orders(self, instr: int) -> List[dict]:
        # Return stored rows or empty list
        return self._orders_by_instr.get(instr, [])

    def create_instrument(self, instr: int):
        # Record that instrument, plus initialize its order‐list
        self.created.append(instr)
        self._orders_by_instr.setdefault(instr, [])

    # ---- live persist ----------------------------------------------
    def record_order(self, o):
        # Save the order's __dict__ for inspection
        self.orders.append(o.__dict__)

    def record_trade(self, t):
        # Save the trade's __dict__ for inspection
        self.trades.append(t.__dict__)

    def record_cancel(self, instr: int, oid: int):
        # Record (instrument_id, order_id) pairs
        self.cancels.append((instr, oid))

    def upsert_live_order(self, order):
        bucket = self._orders_by_instr.setdefault(order.instrument_id, [])
        for i, row in enumerate(bucket):
            if row.get("order_id") == order.order_id:
                bucket[i] = order.__dict__
                break
        else:
            bucket.append(order.__dict__)

    def remove_live_order(self, inst: int, order_id: int):
        rows = self._orders_by_instr.get(inst, [])
        self._orders_by_instr[inst] = [r for r in rows if r.get("order_id") != order_id]


# ───────────── Helper: spin up a fresh FastAPI + DummyWriter ───────
def make_client() -> tuple[TestClient, DummyWriter]:
    from apps.exchange.exchange         import Exchange
    from apps.exchange.composite_writer import CompositeWriter

    dummy = DummyWriter()
    # CompositeWriter wraps one or more writers; we supply only DummyWriter
    writer = CompositeWriter(dummy)
    exchange = Exchange(writer)  # This will run a “rebuild” but DummyWriter has no data

    app = FastAPI()
    from fastapi import Body, HTTPException

    @app.post("/orders")
    def _orders(p: dict = Body(...)):
        try:
            out = exchange.handle_new_order(p)
            if out.get("status") == "ERROR":
                # ensure the 'detail' is 100 % JSON-serialisable
                raise HTTPException(status_code=422,
                                    detail=jsonable_encoder(out["details"]))
            return out
        except ValueError as e:  # unknown instrument
            raise HTTPException(status_code=422, detail=str(e))

    def _cancel(p: dict = Body(...)):
        return exchange.handle_cancel(p)                  # always 200 JSON

    def _new_book(p: dict = Body(...)):
        return exchange.create_order_book(p["instrument_id"])

    app = FastAPI()
    app.post("/orders")  (_orders)
    app.post("/cancel")  (_cancel)
    app.post("/new_book")(_new_book)

    return TestClient(app), dummy


# ───────────── Integration TestCase ────────────────────────────────
class APIFullIntegration(unittest.TestCase):

    def setUp(self):
        # Each test gets its own FastAPI test client + DummyWriter
        self.client, self.writer = make_client()

    # ----- /new_book: happy path & duplicate ID ----------------------
    def test_new_book_and_duplicate(self):
        # First: create instrument 10
        resp = self.client.post("/new_book", json={"instrument_id": 10})
        # FastAPI will return 200 OK with our JSON
        self.assertEqual(resp.status_code, 200)
        j = resp.json()
        self.assertEqual(j["status"], "CREATED")
        self.assertIn(10, self.writer.created)

        # Duplicate ID → status="ERROR"
        dup = self.client.post("/new_book", json={"instrument_id": 10})
        self.assertEqual(dup.status_code, 200)
        dupj = dup.json()
        self.assertEqual(dupj["status"], "ERROR")
        self.assertIn("instrument already exists", dupj.get("details", ""))

    # ----- GTC limit add, cross trade, residual cancel ---------------
    def test_gtc_limit_lifecycle(self):
        self.client.post("/new_book", json={"instrument_id": 1})

        # Step 1: place a resting ASK (sell) @ 10500×5
        ask_payload = dict(
            instrument_id=1,
            side="SELL",
            order_type="GTC",
            price_cents=10500,
            quantity=5,
            party_id="Adam",
            password=PWD
        )
        ask_resp = self.client.post("/orders", json=ask_payload)
        # Should be 200 OK with {"status":"ACCEPTED", ...}
        self.assertEqual(ask_resp.status_code, 200)
        askj = ask_resp.json()
        self.assertEqual(askj.get("status"), "ACCEPTED")
        ask_id = askj.get("order_id")
        self.assertIsNotNone(ask_id, "order_id must be present on ACCEPTED")
        # DummyWriter recorded an order
        self.assertTrue(any(o["order_id"] == ask_id for o in self.writer.orders))

        # Step 2: crossing BID @11000×3
        bid_payload = dict(
            instrument_id=1,
            side="BUY",
            order_type="GTC",
            price_cents=11000,
            quantity=3,
            party_id="Adam",
            password=PWD
        )
        bid_resp = self.client.post("/orders", json=bid_payload)
        self.assertEqual(bid_resp.status_code, 200)
        bidj = bid_resp.json()
        # Should have one trade of quantity 3
        trades = bidj.get("trades", [])
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["quantity"], 3)
        self.assertEqual(bidj.get("remaining_qty"), 0)
        # DummyWriter recorded exactly one trade
        self.assertEqual(len(self.writer.trades), 1)

        # Step 3: cancel residual ASK (2 shares left)
        cancel_resp = self.client.post("/cancel", json={"instrument_id": 1, "order_id": ask_id, "party_id":"Adam", "password":PWD})
        self.assertEqual(cancel_resp.status_code, 200)
        cancelj = cancel_resp.json()
        self.assertEqual(cancelj.get("status"), "CANCELLED")
        self.assertIn((1, ask_id), self.writer.cancels)

    # ----- MARKET sweep multi‐level, partial fill -----------
    def test_market_sweep_multi_level(self):
        self.client.post("/new_book", json={"instrument_id": 2})

        # Add three price levels: 10000×1, 10005×2, 10010×3
        for px, qty in [(10000,1),(10005,2),(10010,3)]:
            payload = dict(
                instrument_id=2,
                side="SELL",
                order_type="GTC",
                price_cents=px,
                quantity=qty,
                party_id="Adam",
                password=PWD
            )
            r = self.client.post("/orders", json=payload)
            self.assertEqual(r.status_code, 200)

        # Now send a MARKET BUY for 4 shares → should sweep first two levels entirely
        mkt_payload = dict(
            instrument_id=2,
            side="BUY",
            order_type="MARKET",
            quantity=4,
            party_id="Adam",
            password=PWD
        )
        mktr = self.client.post("/orders", json=mkt_payload)
        self.assertEqual(mktr.status_code, 200)
        mktj = mktr.json()
        # No remainder (exact fill of 4 out of total 6 at best prices)
        self.assertEqual(mktj.get("remaining_qty"), 0)
        total_traded = sum(t["quantity"] for t in mktj.get("trades", []))
        self.assertEqual(total_traded, 4)

        # DummyWriter saw 3 initial orders + at least 2 new trades
        self.assertTrue(len(self.writer.orders) >= 3)
        self.assertTrue(len(self.writer.trades) >= 2)

    # ----- MARKET on empty book leaves full residual -----------------
    def test_market_on_empty_book(self):
        self.client.post("/new_book", json={"instrument_id": 3})

        mkt_payload = dict(
            instrument_id=3,
            side="BUY",
            order_type="MARKET",
            quantity=2,
            party_id="Adam",
            password=PWD
        )
        r = self.client.post("/orders", json=mkt_payload)
        self.assertEqual(r.status_code, 200)
        jr = r.json()
        # No trades, remainder = original quantity
        self.assertEqual(jr.get("trades"), [])
        self.assertEqual(jr.get("remaining_qty"), 2)
        # DummyWriter recorded the order but zero trades
        self.assertEqual(len(self.writer.orders), 1)
        self.assertEqual(len(self.writer.trades), 0)

    # ----- IOC outside spread should cancel entirely -----------------
    def test_ioc_full_cancel(self):
        self.client.post("/new_book", json={"instrument_id": 4})

        # Place one resting ask @10200×1
        r1 = self.client.post("/orders", json=dict(
            instrument_id=4,
            side="SELL",
            order_type="GTC",
            price_cents=10200,
            quantity=1,
            party_id="Adam",
            password=PWD
        ))
        self.assertEqual(r1.status_code, 200)

        # Now send IOC BUY @9900×1 → cannot match → cancel
        ioc_payload = dict(
            instrument_id=4,
            side="BUY",
            order_type="IOC",
            price_cents=9900,
            quantity=1,
            party_id="Adam",
            password=PWD
        )
        r2 = self.client.post("/orders", json=ioc_payload)
        self.assertEqual(r2.status_code, 200)
        j2 = r2.json()
        # The response should include "cancelled": True
        self.assertTrue(j2.get("cancelled") is True)
        self.assertEqual(j2.get("trades"), [])

    # ----- Validation errors (missing price, bad enum) ----------------
    def test_validation_errors(self):
        # Do NOT create instrument 5 first → posting to /orders should 422
        bad_inputs = [
            # 1) missing price_cents on GTC
            dict(instrument_id=5, side="BUY", order_type="GTC", quantity=1, party_id="Adam", password=PWD),
            # 2) invalid enum for side
            dict(instrument_id=5, side="XXX", order_type="MARKET", quantity=1, party_id="Adam", password=PWD),
            # 3) invalid enum for order_type
            dict(instrument_id=5, side="BUY", order_type="FOO", quantity=1, party_id="Adam", password=PWD),
        ]
        for bad in bad_inputs:
            with self.subTest(bad=bad):
                r = self.client.post("/orders", json=bad)
                # FastAPI will reject with 422
                self.assertEqual(r.status_code, 422)

    # ----- /cancel edge cases: duplicate & wrong instrument ------------
    def test_cancel_edge_cases(self):
        self.client.post("/new_book", json={"instrument_id": 6})

        # Place and cancel one order
        place = self.client.post("/orders", json=dict(
            instrument_id=6, side="SELL", order_type="GTC",
            price_cents=9999, quantity=1, party_id="Adam", password=PWD
        ))
        self.assertEqual(place.status_code, 200)
        pj = place.json()
        if "order_id" not in pj:
            self.fail("Expected order_id in response for valid GTC")
        oid = pj["order_id"]

        # 1st cancel → OK
        r1 = self.client.post("/cancel", json={"instrument_id":6,"order_id":oid, "party_id":"Adam", "password":PWD})
        self.assertEqual(r1.status_code, 200)
        j1 = r1.json()
        self.assertEqual(j1.get("status"), "CANCELLED")

        # 2nd cancel → ERROR (HTTP 200 but status="ERROR")
        r2 = self.client.post("/cancel", json={"instrument_id":6,"order_id":oid, "party_id":"Adam", "password":PWD})
        self.assertEqual(r2.status_code, 200)
        j2 = r2.json()
        self.assertEqual(j2.get("status"), "ERROR")

        # Wrong instrument → ERROR JSON
        r3 = self.client.post("/cancel", json={"instrument_id":999,"order_id":1, "party_id":"Adam", "password":PWD})
        self.assertEqual(r3.status_code, 200)
        j3 = r3.json()
        self.assertEqual(j3.get("status"), "ERROR")

    # ----- OID monotonic increase across multiple calls ----------------
    def test_oid_monotonicity(self):
        self.client.post("/new_book", json={"instrument_id": 7})
        generated = []
        for i in range(5):
            r = self.client.post("/orders", json=dict(
                instrument_id=7, side="BUY",
                order_type="GTC", price_cents=7000 + i, quantity=1, party_id="Adam", password=PWD
            ))
            self.assertEqual(r.status_code, 200)
            j = r.json()
            if "order_id" not in j:
                self.fail("Expected order_id key for valid GTC order")
            generated.append(j["order_id"])
        self.assertEqual(generated, sorted(generated))

    # ----- Combined scenario: rebuild from DummyWriter + live orders ----
    def test_rebuild_then_live_orders(self):
        # Inject two **SELL** orders so total available = 5
        self.writer._orders_by_instr[9] = [
            dict(order_type="GTC", side="SELL", price_cents=5000,
                 quantity=2, timestamp=time_ns(), order_id=101,
                 party_id="Adam", cancelled=False, instrument_id=9, password=PWD),      # changed BUY→SELL
            dict(order_type="GTC", side="SELL", price_cents=5050,
                 quantity=3, timestamp=time_ns(), order_id=102,
                 party_id="Adam", cancelled=False, instrument_id=9, password=PWD)       # changed BUY→SELL,
        ]
        # Re‐create the Exchange so it rebuilds instrument 9
        from apps.exchange.exchange import Exchange
        from apps.exchange.composite_writer import CompositeWriter
        fresh_writer = self.writer
        fresh_exchange = Exchange(CompositeWriter(fresh_writer))

        # Now instrument 9 exists in memory with those two resting orders
        app = FastAPI()
        from fastapi import Body  # ← needed for the inline route
        # … Exchange build exactly as before …

        @app.post("/orders")
        def _orders(p: dict = Body(...)):  # now a proper signature
            return fresh_exchange.handle_new_order(p)
        test_client = TestClient(app)

        # Place a MARKET order that sweeps both (2 + 3) and leaves remainder 5
        resp = test_client.post("/orders", json=dict(
            instrument_id=9, side="BUY", order_type="MARKET",
            quantity=10, party_id="Adam", password=PWD
        ))
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        trades = data.get("trades", [])
        total_matched = sum(t["remaining_quantity"] for t in trades)
        self.assertEqual(total_matched, 5)   # (2+3)
        self.assertEqual(data.get("remaining_qty"), 5)

    # ----- High‐volume fuzz: many small orders / cancels, check invariants -
    def test_high_volume_fuzz(self):
        import random
        self.client.post("/new_book", json={"instrument_id": 10})
        all_oids = []
        for _ in range(200):
            side = random.choice(["BUY", "SELL"])
            px = random.randint(9000, 11000)
            qty = random.randint(1, 3)
            r = self.client.post("/orders", json=dict(
                instrument_id=10, side=side, order_type="GTC",
                price_cents=px, quantity=qty, party_id="Adam",
                password=PWD
            ))
            self.assertEqual(r.status_code, 200)
            j = r.json()
            oid = j.get("order_id")
            if oid is not None:
                all_oids.append(oid)
            # 30 % chance to cancel immediately
            if oid is not None and random.random() < 0.3:
                cancel_r = self.client.post("/cancel", json={"instrument_id": 10, "order_id": oid, "party_id":"Adam", "password":PWD})
                self.assertEqual(cancel_r.status_code, 200)

        # Now send 50 random MARKET orders
        for _ in range(50):
            side = random.choice(["BUY", "SELL"])
            r = self.client.post("/orders", json=dict(
                instrument_id=10, side=side, order_type="MARKET",
                quantity=random.randint(1, 5), party_id="Adam",
                password=PWD
            ))
            # Always returns 200 and has a "remaining_qty" key
            self.assertEqual(r.status_code, 200)
            j = r.json()
            self.assertIn("remaining_qty", j)

        # Ensure OIDs never repeated
        self.assertEqual(len(set(all_oids)), len(all_oids))

    # ----- Multiple consecutive cancels same OID ---------------------
    def test_consecutive_cancels_same_oid(self):
        self.client.post("/new_book", json={"instrument_id": 11})
        place = self.client.post("/orders", json=dict(
            instrument_id=11, side="SELL", order_type="GTC",
            price_cents=11111, quantity=2, party_id=11,
            password=PWD
        ))
        self.assertEqual(place.status_code, 200)
        pj = place.json()
        oid = pj.get("order_id")
        self.assertIsNotNone(oid)

        # First cancel → OK
        r1 = self.client.post("/cancel", json={"instrument_id":11,"order_id":oid, "party_id":99, "password":PWD})
        self.assertEqual(r1.status_code, 200)
        j1 = r1.json()
        self.assertEqual(j1.get("status"), "CANCELLED")

        # Second cancel → ERROR
        r2 = self.client.post("/cancel", json={"instrument_id":11,"order_id":oid, "party_id":99, "password":PWD})
        self.assertEqual(r2.status_code, 200)
        j2 = r2.json()
        self.assertEqual(j2.get("status"), "ERROR")

    # ----- Submit order to non‐existent book → FastAPI returns 422 ----
    def test_submit_to_missing_book(self):
        r = self.client.post("/orders", json=dict(
            instrument_id=99, side="BUY", order_type="GTC",
            price_cents=9999, quantity=1, party_id=1,
            password=PWD
        ))
        # Because Exchange._get_book() raises ValueError → FastAPI wraps as 422
        self.assertEqual(r.status_code, 422)

    # ----- Cancel on missing book → status="ERROR" JSON -------------
    def test_cancel_missing_book(self):
        r = self.client.post("/cancel", json={"instrument_id": 123, "order_id": 1, "party_id":99, "password":PWD})
        # Handler catches missing book and returns {"status":"ERROR"}
        self.assertEqual(r.status_code, 200)
        data = r.json()
        self.assertEqual(data.get("status"), "ERROR")


if __name__ == "__main__":
    unittest.main(verbosity=2)
