# apps/exchange/text_backup_writer.py
import asyncio
import csv
from pathlib import Path
from typing import Any, Dict, List

from apps.exchange.models import Order, Trade


def remove_live_order(instrument_id: int, order_id: int) -> None:
    return None


class TextBackupWriter:
    """
    An append‐only CSV “event logger.”  For each instrument:
      • orders_<instr>.csv       (one row per record_order)
      • trades_<instr>.csv       (one row per record_trade)
      • cancels_<instr>.csv      (one row per record_cancel)
      • live_events_<instr>.csv  (one row per upsert_live_order or remove_live_order)

    All writes are scheduled via asyncio.to_thread(...) so as not to block.
    """

    def __init__(self, directory: str = "text_backup"):
        self.base_dir = Path(directory)
        self.base_dir.mkdir(exist_ok=True)

        # Fieldnames for each CSV file:
        self._order_fields = [
            "order_type",
            "side",
            "instrument_id",
            "price_cents",
            "quantity",
            "timestamp",
            "order_id",
            "party_id",
            "cancelled",
            "filled_quantity",
            "remaining_quantity",
        ]

        self._trade_fields = [
            "instrument_id",
            "price_cents",
            "quantity",
            "timestamp",
            "maker_order_id",
            "maker_party_id",
            "taker_order_id",
            "taker_party_id",
            "maker_is_buyer",
            "maker_quantity_remaining",
            "taker_quantity_remaining",
        ]

        self._cancel_fields = [
            "instrument_id",
            "order_id",
            "timestamp"
        ]

        # In “live_events” we simply tag each row either “UPS_LIVE” or “REM_LIVE” plus full order info if applicable
        self._live_fields = [
            "event_type",       # either "UPS_LIVE" or "REM_LIVE"
            "order_type",
            "side",
            "instrument_id",
            "price_cents",
            "quantity",
            "timestamp",
            "order_id",
            "party_id",
            "cancelled",
            "filled_quantity",
            "remaining_quantity",
        ]

    # ──────── Required stubs (no replay) ─────────────────────────────────

    def list_instruments(self) -> List[int]:
        # We do not support replay; always return empty.
        return []

    def iter_orders(self, instrument_id: int) -> List[Dict[str, Any]]:
        # No replay; return empty.
        return []

    def update_order_quantity(self, instrument_id: int, order_id: int, quantity_modification: int) -> None:
        return None

    def remove_live_order(self, inst: int, order_id: int) -> None:
        return None

    def create_instrument(self, instrument_id: int) -> None:
        """
        Create (if missing) the four CSV files with headers.
        """
        def _ensure_csv(path: Path, header: List[str]) -> None:
            if not path.exists():
                with path.open("w", newline="", encoding="utf-8") as fp:
                    writer = csv.DictWriter(fp, fieldnames=header)
                    writer.writeheader()

        p_orders  = self.base_dir / f"orders_{instrument_id}.csv"
        p_trades  = self.base_dir / f"trades_{instrument_id}.csv"
        p_cancels = self.base_dir / f"cancels_{instrument_id}.csv"
        p_live    = self.base_dir / f"live_events_{instrument_id}.csv"

        _ensure_csv(p_orders,  self._order_fields)
        _ensure_csv(p_trades,  self._trade_fields)
        _ensure_csv(p_cancels, self._cancel_fields)
        _ensure_csv(p_live,    self._live_fields)

    # ──────── Hot‐path methods (append only) ─────────────────────────────

    def record_order(self, order: Order) -> None:
        """
        Append one row to orders_<instr>.csv
        """
        data = order.__dict__
        loop = asyncio.get_event_loop()
        loop.create_task(self._append_order_row(data))

    def record_trade(self, trade: Trade) -> None:
        """
        Append one row to trades_<instr>.csv
        """
        data = trade.__dict__
        loop = asyncio.get_event_loop()
        loop.create_task(self._append_trade_row(data))

    def record_cancel(self, instrument_id: int, order_id: int) -> None:
        """
        Append one row to cancels_<instr>.csv
        """
        timestamp = int(asyncio.get_event_loop().time() * 1e9)  # use nanosecond timestamp
        row = {
            "instrument_id": instrument_id,
            "order_id": order_id,
            "timestamp": timestamp,
        }
        loop = asyncio.get_event_loop()
        loop.create_task(self._append_cancel_row(row))

    def upsert_live_order(self, order: Order) -> None:
        """
        Append one “UPS_LIVE” row to live_events_<instr>.csv
        """
        data = order.__dict__.copy()
        data["event_type"] = "UPS_LIVE"
        loop = asyncio.get_event_loop()
        loop.create_task(self._append_live_row(data))

    # ──────── Internal async helpers ──────────────────────────────────────

    async def _append_order_row(self, row: Dict[str, Any]) -> None:
        instr = row["instrument_id"]
        path = self.base_dir / f"orders_{instr}.csv"

        def _sync_write():
            if not path.exists():
                with path.open("w", newline="", encoding="utf-8") as fp:
                    writer = csv.DictWriter(fp, fieldnames=self._order_fields)
                    writer.writeheader()
            with path.open("a", newline="", encoding="utf-8") as fp:
                writer = csv.DictWriter(fp, fieldnames=self._order_fields)
                # Convert booleans to literal "True"/"False"
                row2 = {
                    k: (str(v) if isinstance(v, bool) else v)
                    for k, v in row.items()
                }
                writer.writerow(row2)

        await asyncio.to_thread(_sync_write)

    async def _append_trade_row(self, row: Dict[str, Any]) -> None:
        instr = row["instrument_id"]
        path = self.base_dir / f"trades_{instr}.csv"

        def _sync_write():
            if not path.exists():
                with path.open("w", newline="", encoding="utf-8") as fp:
                    writer = csv.DictWriter(fp, fieldnames=self._trade_fields)
                    writer.writeheader()
            with path.open("a", newline="", encoding="utf-8") as fp:
                writer = csv.DictWriter(fp, fieldnames=self._trade_fields)
                writer.writerow(row)

        await asyncio.to_thread(_sync_write)

    async def _append_cancel_row(self, row: Dict[str, Any]) -> None:
        instr = row["instrument_id"]
        path = self.base_dir / f"cancels_{instr}.csv"

        def _sync_write():
            if not path.exists():
                with path.open("w", newline="", encoding="utf-8") as fp:
                    writer = csv.DictWriter(fp, fieldnames=self._cancel_fields)
                    writer.writeheader()
            with path.open("a", newline="", encoding="utf-8") as fp:
                writer = csv.DictWriter(fp, fieldnames=self._cancel_fields)
                writer.writerow(row)

        await asyncio.to_thread(_sync_write)

    async def _append_live_row(self, row: Dict[str, Any]) -> None:
        instr = row["instrument_id"]
        path = self.base_dir / f"live_events_{instr}.csv"

        def _sync_write():
            if not path.exists():
                with path.open("w", newline="", encoding="utf-8") as fp:
                    writer = csv.DictWriter(fp, fieldnames=self._live_fields)
                    writer.writeheader()
            with path.open("a", newline="", encoding="utf-8") as fp:
                writer = csv.DictWriter(fp, fieldnames=self._live_fields)
                # Ensure booleans become "True"/"False"; other missing fields stay as empty strings
                row2 = {
                    k: (str(v) if isinstance(v, bool) else v)
                    for k, v in row.items()
                }
                writer.writerow(row2)

        await asyncio.to_thread(_sync_write)
