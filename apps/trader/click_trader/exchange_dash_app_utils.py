import datetime
from decimal import Decimal

def dollars(cents: int) -> str:
    """Format integer cents → '1,234.56' (no currency symbol)."""
    return f"{cents/100:,.2f}"

def no_dollar(cents: int) -> str:
    """Alias for dollars (used when naming is clearer)."""
    return f"{cents/100:,.2f}"

def to_cents(txt: str) -> int:
    """Convert user input like '100.50' → 10050."""
    return int(Decimal(txt.strip()) * 100)

def format_dt(ts_ns: int) -> str:
    """
    Format nanosecond timestamp → 'HH:MM:SS - DD-MM-YYYY'.
    If invalid, return '--'.
    """
    try:
        dt = datetime.datetime.fromtimestamp(ts_ns / 1e9)
        return dt.strftime("%H:%M:%S - %d-%m-%Y")
    except Exception:
        return "--"