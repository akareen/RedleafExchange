# apps/exchange/book.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto

class Side(Enum):
    BUY = auto()
    SELL = auto()

class OrderType(Enum):
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

    def fill(self, quantity: int) -> None:
        if quantity > self.quantity:
            raise ValueError("Cannot fill more than the order's quantity")
        self.quantity -= quantity
        if self.quantity == 0:
            self.cancel()

    def __str__(self) -> str: # For logging and debugging
        return (f"Order(order_type={self.order_type}, side={self.side}, "
                f"instrument_id={self.instrument_id}, price_cents={self.price_cents}, "
                f"quantity={self.quantity}, timestamp={self.timestamp}, "
                f"order_id={self.order_id}, party_id={self.party_id}, cancelled={self.cancelled})")

@dataclass(slots=True)
class Trade:
    instrument_id: int
    price_cents: int
    quantity: int
    timestamp: int
    maker_order_id: int
    taker_order_id: int
    maker_is_buyer: bool

    def __str__(self) -> str:  # For logging and debugging
        return (f"Trade(instrument_id={self.instrument_id}, price_cents={self.price_cents}, "
                f"quantity={self.quantity}, timestamp={self.timestamp}, "
                f"maker_order_id={self.maker_order_id}, taker_order_id={self.taker_order_id}, "
                f"maker_is_buyer={self.maker_is_buyer})")