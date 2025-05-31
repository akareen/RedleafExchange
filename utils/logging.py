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
    def rotation_filename(self, default_name: str):      # strip dot
        stem, stamp = Path(default_name).stem.split(".", 1)
        return str(Path(default_name).with_name(f"{stem}_{stamp}log"))

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
