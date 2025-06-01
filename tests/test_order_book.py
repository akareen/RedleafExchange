# tests/test_order_book_extended.py
from __future__ import annotations
import unittest, random
from time import time_ns

from apps.exchange.order_book import OrderBook
from apps.exchange.models import Order, Side, OrderType

OID = 1
def fresh_order(
    side: Side,
    price: int,
    qty: int,
    *,
    typ: OrderType = OrderType.GTC,
    instr: int = 1,
) -> Order:
    """Convenience factory that autoincrements order-id."""
    global OID
    OID += 1
    return Order(
        order_type=typ,
        side=side,
        instrument_id=instr,
        price_cents=price,
        quantity=qty,
        timestamp=time_ns(),
        order_id=OID,
        party_id=random.randint(1, 9),
        cancelled=False,
        filled_quantity=0,
        remaining_quantity=qty,
    )


class OrderBookHeavyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.book = OrderBook(instrument_id=1)

    # ------------------------------------------------------------------
    # price–time priority (FIFO on same price)
    # ------------------------------------------------------------------
    def test_price_time_priority(self):
        a = fresh_order(Side.SELL, 10050, 4)
        b = fresh_order(Side.SELL, 10050, 4)
        self.book.submit(a)
        self.book.submit(b)

        mkt = fresh_order(Side.BUY, 0, 5, typ=OrderType.MARKET)
        trades = self.book.submit(mkt)

        # first 4 should be against 'a', last 1 against 'b'
        self.assertEqual(trades[0].maker_order_id, a.order_id)
        self.assertEqual(trades[0].quantity, 4)
        self.assertEqual(trades[1].maker_order_id, b.order_id)
        self.assertEqual(trades[1].quantity, 1)

    # ------------------------------------------------------------------
    # best_bid < best_ask invariant
    # ------------------------------------------------------------------
    def test_bid_ask_gap_never_negative(self):
        # Add inverted spread then match it out
        buy = fresh_order(Side.BUY, 10030, 2)
        sell= fresh_order(Side.SELL,10010, 2)
        self.book.submit(buy)
        self.book.submit(sell)           # matches instantly

        self.assertIsNone(self.book.best_bid())
        self.assertIsNone(self.book.best_ask())

        # Normal spread
        self.book.submit(fresh_order(Side.BUY, 10000, 1))
        self.book.submit(fresh_order(Side.SELL,10050, 1))
        self.assertLess(self.book.best_bid(), self.book.best_ask())

    # ------------------------------------------------------------------
    # Partial fill spanning three price levels
    # ------------------------------------------------------------------
    def test_sweep_multiple_levels(self):
        self.book.submit(fresh_order(Side.SELL, 10000, 1))
        self.book.submit(fresh_order(Side.SELL, 10005, 2))
        self.book.submit(fresh_order(Side.SELL, 10010, 3))

        mkt = fresh_order(Side.BUY, 0, 5, typ=OrderType.MARKET)
        trades = self.book.submit(mkt)

        self.assertEqual(sum(t.quantity for t in trades), 5)
        self.assertEqual(self.book.best_ask(), 10010)
        # remaining qty on 10010 should now be 1
        lvl = self.book.asks[10010]
        self.assertEqual(lvl.top().remaining_quantity, 1)

    # ------------------------------------------------------------------
    # Cancel unknown & cancel twice
    # ------------------------------------------------------------------
    def test_cancel_idempotent(self):
        o = fresh_order(Side.BUY, 9990, 5)
        self.book.submit(o)
        self.assertTrue(self.book.cancel(o.order_id))
        self.assertFalse(self.book.cancel(o.order_id))   # second time
        self.assertFalse(self.book.cancel(999999))       # never existed

    # ------------------------------------------------------------------
    # Heap should prune stale prices automatically
    # ------------------------------------------------------------------
    def test_heap_self_prune(self):
        o1 = fresh_order(Side.SELL, 10100, 1)
        o2 = fresh_order(Side.SELL, 10100, 1)
        self.book.submit(o1)
        self.book.submit(o2)
        self.book.cancel(o1.order_id)
        self.book.cancel(o2.order_id)

        # Force heap peek
        self.assertIsNone(self.book.best_ask())
        self.assertEqual(len(self.book.ask_heap.valid), 0)

    # ------------------------------------------------------------------
    # High-volume fuzz — 1 000 inserts, random cancels, ensure invariants
    # ------------------------------------------------------------------
    def test_fuzz_insert_cancel(self):
        random.seed(42)
        orders: list[Order] = []
        for _ in range(1000):
            side = random.choice([Side.BUY, Side.SELL])
            price = random.randint(9900, 10100)
            qty   = random.randint(1, 5)
            o     = fresh_order(side, price, qty)
            self.book.submit(o)
            orders.append(o)

            # 30 % chance cancel immediately
            if random.random() < 0.3:
                self.book.cancel(o.order_id)

        # random trades to shake the tree
        for _ in range(200):
            if random.random() < 0.5:
                m = fresh_order(Side.BUY, 0, random.randint(1, 5), typ=OrderType.MARKET)
            else:
                m = fresh_order(Side.SELL, 0, random.randint(1, 5), typ=OrderType.MARKET)
            self.book.submit(m)

        # invariant: all orders in oid_map with qty==0 must be cancelled flag, others >0 & !cancelled
        for o in self.book.oid_map.values():
            if o.remaining_quantity == 0:
                self.assertTrue(o.cancelled)
            else:
                self.assertFalse(o.cancelled)

        # heap validity: every price in heap.valid must exist in side dict
        for p in list(self.book.bid_heap.valid):
            self.assertIn(p, self.book.bids)
        for p in list(self.book.ask_heap.valid):
            self.assertIn(p, self.book.asks)


    # ------------------------------------------------------------------
    # Cancel after partial fill (order still in book)
    # ------------------------------------------------------------------
    def test_cancel_partially_filled(self):
        ask = fresh_order(Side.SELL, 10110, 8)
        self.book.submit(ask)

        buy = fresh_order(Side.BUY, 10110, 3)
        self.book.submit(buy)       # partial fill, ask qty = 5

        # now cancel the residual ask
        self.assertTrue(self.book.cancel(ask.order_id))
        self.assertIsNone(self.book.best_ask())

    # ------------------------------------------------------------------
    # Duplicate price spam does not blow up heap
    # ------------------------------------------------------------------
    def test_heap_size_under_duplicate_spam(self):
        price = 10050
        for _ in range(1000):
            self.book.submit(fresh_order(Side.BUY, price, 1))
            # cancel immediately to churn same price
            last_id = OID
            self.book.cancel(last_id)

        # After spam, heap.valid should be empty
        self.assertNotIn(price, self.book.bid_heap.valid)
        self.assertIsNone(self.book.best_bid())

    # ------------------------------------------------------------------
    # Interleaved insert/cancel keeps bid<ask invariant
    # ------------------------------------------------------------------
    def test_random_bid_ask_invariant(self):
        random.seed(7)
        for _ in range(500):
            if random.random() < 0.5:
                self.book.submit(fresh_order(Side.BUY, random.randint(9900, 10050), 1))
            else:
                self.book.submit(fresh_order(Side.SELL, random.randint(10060, 10120), 1))
            # occasional cancels
            if random.random() < 0.2 and self.book.oid_map:
                victim = random.choice(list(self.book.oid_map.values()))
                self.book.cancel(victim.order_id)

            bid = self.book.best_bid()
            ask = self.book.best_ask()
            if bid is not None and ask is not None:
                self.assertLess(bid, ask)

if __name__ == "__main__":
    unittest.main(verbosity=2)
