# apps/exchange/order_book.py
from __future__ import annotations
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional
import heapq, logging
from time import time_ns

from apps.exchange.models import Trade, Order, Side, OrderType

# ─────────── helpers ────────────
@dataclass(slots=True)
class PriceLevel:
    price_cents: int
    queue: Deque[Order] = field(default_factory=deque)

    def add(self, o: Order) -> None:
        self.queue.append(o)

    def _prune(self) -> None:
        while self.queue and (self.queue[0].remaining_quantity == 0 or self.queue[0].cancelled):
            self.queue.popleft()

    def top(self) -> Optional[Order]:
        self._prune()
        return self.queue[0] if self.queue else None

    def is_empty(self) -> bool:
        self._prune()
        return not self.queue


class PriceHeap:
    def __init__(self, is_bid: bool):
        self.is_bid = is_bid
        self.h: list[int] = []
        self.valid: set[int] = set()

    def push(self, price: int) -> None:
        key = -price if self.is_bid else price
        if price not in self.valid:
            heapq.heappush(self.h, key)
            self.valid.add(price)

    def mark_empty(self, price: int) -> None:
        self.valid.discard(price)               # lazy-delete

    def best(self) -> int | None:
        while self.h:
            price = -self.h[0] if self.is_bid else self.h[0]
            if price in self.valid:
                return price
            heapq.heappop(self.h)               # drop stale
        return None


# ─────────── main container ────────────
class OrderBook:
    def __init__(self, instrument_id: int):
        self.instrument_id = instrument_id
        self.log = logging.getLogger("OrderBook")
        self.bids: Dict[int, PriceLevel] = {}
        self.asks: Dict[int, PriceLevel] = {}
        self.bid_heap = PriceHeap(is_bid=True)
        self.ask_heap = PriceHeap(is_bid=False)
        self.oid_map: Dict[int, Order] = {}
        self.log.debug("OrderBook created")
        self.last_state = 0

    # ---------- public ---------------------------------------------------
    def submit(self, order: Order) -> List[Trade]:
        self.last_state += 1
        """
        • MARKET  →  execute immediately
        • GTC     →  match then rest
        • IOC     →  match; cancel residue
        """
        if order.instrument_id != self.instrument_id:
            raise ValueError("Order sent to wrong book")

        trades: List[Trade] = []
        if order.order_type is OrderType.MARKET:
            trades += self._execute_market(order)
        elif order.order_type is OrderType.GTC:
            trades += self._match_limit(order)
            if order.remaining_quantity:                 # residue rests
                self.rest_order(order)
        elif order.order_type is OrderType.IOC:
            trades += self._match_limit(order)
            if order.remaining_quantity:                 # unfilled part cancelled
                order.cancel()
        return trades

    def cancel(self, order_id: int) -> bool:
        self.last_state += 1
        """
        Idempotent cancel.

        Returns
        -------
        True   – the order was open and is now newly cancelled
        False  – the order was either unknown or had already been cancelled/filled
                 (book/heap cleanup is still performed).
        """
        order = self.oid_map.get(order_id)
        if order is None:
            self.log.debug("cancel miss %s", order_id)
            return False  # unknown ID

        first_time = not order.cancelled
        if first_time:
            order.cancel()
            self.log.debug("cancel flag %s", order_id)

        # Always do the data-structure cleanup
        level_dict = self.bids if order.side is Side.BUY else self.asks
        heap = self.bid_heap if order.side is Side.BUY else self.ask_heap
        pl = level_dict.get(order.price_cents)
        if pl is None or pl.is_empty():
            level_dict.pop(order.price_cents, None)
            heap.mark_empty(order.price_cents)

        if first_time:
            del self.oid_map[order_id]

        return first_time

    # ---------- internal helpers ----------------------------------------
    def rest_order(self, o: Order) -> None:
        self.last_state += 1
        lvl_dict = self.bids if o.side is Side.BUY else self.asks
        heap     = self.bid_heap if o.side is Side.BUY else self.ask_heap

        if o.price_cents not in lvl_dict:
            lvl_dict[o.price_cents] = PriceLevel(o.price_cents)
            heap.push(o.price_cents)
        lvl_dict[o.price_cents].add(o)
        self.oid_map[o.order_id] = o

    def _match_limit(self, o: Order) -> List[Trade]:
        trades: List[Trade] = []
        while o.remaining_quantity:
            best_price = (self.ask_heap.best() if o.side is Side.BUY else self.bid_heap.best())
            self.log.debug("match_limit: %s %s @ %s", o.side, o.remaining_quantity, best_price)

            if best_price is None:
                self.log.debug("match_limit: no best price found")
                break
            if (o.side is Side.BUY and best_price > o.price_cents) or \
               (o.side is Side.SELL and best_price < o.price_cents):
                self.log.debug("match_limit: no match at %s", best_price)
                break

            lvl = (self.asks if o.side is Side.BUY else self.bids)[best_price]
            top = lvl.top()
            if top is None:
                if lvl.is_empty():
                    del (self.asks if o.side is Side.BUY else self.bids)[best_price]
                    (self.ask_heap if o.side is Side.BUY else self.bid_heap).mark_empty(best_price)
                self.log.debug("match_limit: no top order at %s", best_price)
                continue

            self.log.debug("match_limit: top order %s", top)
            trade = self._match_orders(order=o, top_order=top)
            trades.append(trade)
            self.log.debug("trade executed: %s", trade)
        self.log.debug("match_limit: completed with %d trades", len(trades))
        return trades

    def _execute_market(self, o: Order) -> List[Trade]:
        """
        MARKET: identical to limit matching but no price check.
        """
        trades: List[Trade] = []
        while o.remaining_quantity:
            best_price = (self.ask_heap.best() if o.side is Side.BUY
                          else self.bid_heap.best())
            if best_price is None: break
            lvl = (self.asks if o.side is Side.BUY else self.bids)[best_price]
            top = lvl.top()
            if top is None:
                if lvl.is_empty():
                    del (self.asks if o.side is Side.BUY else self.bids)[best_price]
                    (self.ask_heap if o.side is Side.BUY else self.bid_heap).mark_empty(best_price)
                self.log.debug("execute_market: no top order at %s", best_price)
                continue

            self.log.debug("execute_market: top order %s", top)
            trade = self._match_orders(order=o, top_order=top)
            trades.append(trade)
            self.log.debug("trade executed %s", trade)
        self.log.debug("execute_market: completed with %d trades", len(trades))
        return trades

    def _match_orders(self, order: Order, top_order: Order) -> Trade:
        """
        Match an incoming order with the top order in the book.
        Returns a Trade object.
        """
        qty = min(order.remaining_quantity, top_order.remaining_quantity)
        order.fill(quantity=qty)
        top_order.fill(quantity=qty)
        trade = Trade(
            instrument_id=self.instrument_id,
            price_cents=top_order.price_cents,
            quantity=qty,
            timestamp=time_ns(),
            maker_order_id=top_order.order_id,  # resting book order
            taker_order_id=order.order_id,  # incoming order
            maker_party_id=top_order.party_id,
            taker_party_id=order.party_id,
            maker_is_buyer=(top_order.side is Side.BUY),
            maker_quantity_remaining=top_order.remaining_quantity,
            taker_quantity_remaining=order.remaining_quantity
        )
        if top_order.remaining_quantity == 0 and top_order.order_id in self.oid_map:
            self.cancel(top_order.order_id)  # remove from book if fully filled
        return trade

    def best_bid(self): return self.bid_heap.best()
    def best_ask(self): return self.ask_heap.best()