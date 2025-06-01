# utils/logging.py
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_LOG_DIR = Path("logs"); _LOG_DIR.mkdir(exist_ok=True)

_FMT     = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATEFMT = "%Y-%m-%dT%H:%M:%S%z"

class QuarterHourRotator(TimedRotatingFileHandler):
    """Roll every 15 min, keep forever, never put '-' or '.' in the filename."""
    def __init__(self, base_path: Path):
        super().__init__(filename=base_path,
                         when="M", interval=15, backupCount=0, encoding="utf-8")
        self.suffix = "%Y%m%d_%H%M"                      # 20250531_1045 (OK)

    def rotation_filename(self, default_name: str) -> str:
        """
        Build the new rollover filename.  If `default_name` (e.g. "app.log") has no dot
        in its stem, just append the timestamp at the end.  If it does contain a dot,
        split only on the first dot so we preserve things like "app.error.log" → "app.error.2025-06-01.log".
        """
        p = Path(default_name)
        stem = p.stem  # e.g. "app" or "app.error"
        suff = p.suffix  # e.g. ".log"

        if "." in stem:
            # split only on first dot
            name_part, extra = stem.split(".", 1)
            # e.g. name_part="app", extra="error"
            new_stem = f"{name_part}.{extra}"
        else:
            new_stem = stem

        # Build timestamp (you may already have a helper; this is just an example)
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # If there was an “extra” part after a dot, we want something like:
        #   app.error → app.error.2025-06-01_10-25-12.log
        # If there was no dot at all ( stem == “app” ), this yields:
        #   app → app.2025-06-01_10-25-12.log
        return f"{new_stem}.{timestamp}{suff}"


def setup(level=logging.INFO, fname: str = "exchangelog"):
    """
    Configure *one* root logger for the whole exchange.
    Call this ONCE (in Exchange.__init__) – everyone else just grabs loggers.
    """
    root = logging.getLogger()
    if root.handlers:           # already initialised
        return

    root.setLevel(level)
    # console
    con = logging.StreamHandler()
    con.setFormatter(logging.Formatter(_FMT, _DATEFMT))
    root.addHandler(con)
    # file  (logs/exchangelog, exchangelog_20250531_1045log, …)
    fh = QuarterHourRotator(_LOG_DIR / fname)
    fh.setFormatter(logging.Formatter(_FMT, _DATEFMT))
    root.addHandler(fh)
