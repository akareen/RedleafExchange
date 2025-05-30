from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

class Side(Enum):
    BUY = auto()
    SELL = auto()

class OrderType(Enum):
    LIMIT = auto()
    MARKET = auto()
    GTC = auto()
    IOC = auto()

@dataclass(slots=True)
class Order:
    order_type: OrderType
    side: Side
    instrument_id: int
    price_cents: int
    quantity: int
    timestamp: int
    order_id: int
    party_id: int
    cancelled: bool

    def cancel(self) -> None:
        self.cancelled = True
        self.quantity = 0