from __future__ import annotations
from collections import defaultdict, deque
from dataclasses import dataclass, field
from bisect import insort
from typing import Deque, Dict, List, Optional

from apps.exchange.models import Order, Side

@dataclass(slots=True)
class PriceLevel:
    price_cents: int
    queue: Deque[Order] = field(default_factory=deque)

    def add_order(self, order: Order) -> None:
        self.queue.append(order)

    def _prune_front(self) -> None:
        while self.queue and (self.queue[0].quantity == 0 or self.queue[0].cancelled):
            self.queue.popleft()

    def top(self) -> Optional[Order]:
        self._prune_front()
        return self.queue[0] if self.queue else None

    def pop(self) -> Optional[Order]:
        self._prune_front()
        return self.queue.popleft() if self.queue else None

    def is_empty(self) -> bool:
        self._prune_front()
        return not self.queue


@dataclass
class OrderBook:
    bids: Dict[int, PriceLevel] = field(default_factory=lambda: defaultdict(lambda: None))
    asks: Dict[int, PriceLevel] = field(default_factory=lambda: defaultdict(lambda: None))
    bid_prices: List[int] = field(default_factory=list)
    ask_prices: List[int] = field(default_factory=list)
    oid_map: Dict[int, Order] = field(default_factory=dict)

    def add(self, order: Order) -> None:
        levels, price_index = (
            (self.bids, self.bid_prices) if order.side is Side.BUY else (self.asks, self.ask_prices)
        )
        if levels[order.price_cents] is None:
            levels[order.price_cents] = PriceLevel(order.price_cents)
            insort(price_index, order.price_cents)
            if order.side is Side.BUY:
                price_index.sort(reverse=True)

        levels[order.price_cents].add_order(order)
        self.oid_map[order.order_id] = order

    def cancel(self, order_id: int) -> bool:
        order = self.oid_map.get(order_id)
        if order and not order.cancelled and order.quantity > 0:
            order.cancel()
            return True
        return False