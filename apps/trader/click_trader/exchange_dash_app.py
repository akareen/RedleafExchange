#!/usr/bin/env python3
"""
exchange_dash_app.py  –  Redleaf Exchange Dashboard (Bloomberg‐inspired)

Layout:

┌────────────────────────────────────────────────────────────────────────┐
│ Banner:  Instrument Name (white) | Description | Last Price (green)  │
│          | Last Trade Time | LastMaker / LastTaker                  │
│          | (Created … by …)                                         │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│ Order Entry (Party ID, Pwd, Qty, Price, Side, SEND GTC, SEND IOC,      │
│              Cancel All)                                               │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│ Order Book (50 vh, left)                      │ Price Chart (40 vh)  │
│ – scrollable table with Bid/Price/Ask levels  └───────────────────────┘
│ – (fills two “grid rows” worth of height)                         │
└───────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│ Recent Trades (30 vh, scrollable)       │ Positions (25 vh, scrollable) │
└────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│ Open Orders (Me) Card, total 30 vh:                                         │
│   – Input row (Party ID / Pwd / Fetch) → 5 vh                                │
│   – Scrollable table (25 vh)                                                 │
└────────────────────────────────────────────────────────────────────────────┘

Place a **favicon.ico** under `./assets/` so Dash serves it.

Run with:
    python exchange_dash_app.py
"""
import os
import socket
import threading
import json
import datetime
from collections import defaultdict, deque
from decimal import Decimal

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, dash_table, Input, Output, State, callback_context, ALL
import plotly.graph_objects as go
import requests

# ╭──────────────────────────────── CONFIG ─────────────────────────────╮
API_URL      = os.getenv("API_URL", "http://localhost:8000")
MCAST_GROUP  = os.getenv("MCAST_GROUP", "224.1.1.1")
MCAST_PORT   = int(os.getenv("MCAST_PORT", "4445"))

REFRESH_MS   = 500                 # UI refresh interval (ms)
MAX_TRADES   = 800                # Keep last N trades in memory

# Heights (viewport units):
BOOK_H         = "50vh"           # Order Book
PRICE_CH_H     = "40vh"           # Price Chart
TRADES_H       = "30vh"           # Recent Trades
POS_H          = "25vh"           # Positions (Everyone)
OPEN_INPUT_H   = "5vh"            # “Open Orders (Me)” input row
OPEN_TABLE_H   = "25vh"           # “Open Orders (Me)” scrollable table

# Colors / fonts:
BG           = "#1f2124"           # Soft dark‐gray background
ORANGE_TXT   = "#fb8b1e"           # Bloomberg‐orange
WHITE_TXT    = "#FFFFFF"           # Pure white for instrument name
LASTP_TXT    = "#00b050"           # Bright green for last price
BID_BAR      = "rgba(0,176,80,0.35)"   # Semi‐transparent green
ASK_BAR      = "rgba(230,74,25,0.35)"  # Semi‐transparent red
BID_FONT     = "#00b050"
ASK_FONT     = "#e64a19"
FONT_FAMILY  = "'IBM Plex Mono', monospace"
CELL_FONT_SZ = "0.75rem"            # Slightly smaller font
# ╰────────────────────────────────────────────────────────────────────╯

# ────────── Utility functions ─────────────────────────────────────────
def dollars(cents: int) -> str:
    return f"${cents/100:,.2f}"

def no_dollar(cents: int) -> str:
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


# ────────── Initial “cold” load from Mongo ─────────────────────────────
#   GET /instruments  → must return a list like:
#     [ { "instrument_id", "instrument_name", "instrument_description",
#         "created_time", "created_by" }, ... ]
_instruments = requests.get(f"{API_URL}/instruments").json()
if not _instruments:
    raise SystemExit("❌ No instruments found. Create at least one via API.")

#   GET /parties  → must return a list like: [ { "party_id", "party_name" }, ... ]
_parties = {p["party_id"]: p["party_name"] for p in requests.get(f"{API_URL}/parties").json()}


# ────────── Shared live state (seed from Mongo, then update via multicast) ──
class LiveState:
    book   = defaultdict(lambda: {"bid": defaultdict(int), "ask": defaultdict(int)})
    trades = defaultdict(lambda: deque(maxlen=MAX_TRADES))
    lock   = threading.Lock()

LIVE = LiveState()

def bootstrap_book_and_trades(inst_id: int):
    """
    On startup, pull from Mongo:
      • /live_orders/{inst_id} → seed LIVE.book[inst_id]
      • /trades/{inst_id}      → seed LIVE.trades[inst_id]
    """
    try:
        raw_live = requests.get(f"{API_URL}/live_orders/{inst_id}", timeout=3).json()
        for r in raw_live:
            side = "bid" if r["side"] == "BUY" else "ask"
            LIVE.book[inst_id][side][int(r["price_cents"])] += int(r["remaining_quantity"])
    except Exception:
        pass

    try:
        raw_trades = requests.get(f"{API_URL}/trades/{inst_id}", timeout=3).json()
        for t in sorted(raw_trades, key=lambda x: x["timestamp"]):
            LIVE.trades[inst_id].append(t)
    except Exception:
        pass

for inst in _instruments:
    bootstrap_book_and_trades(inst["instrument_id"])


# ────────── Multicast listener (runs on background thread) ─────────────
def multicast_listener():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("", MCAST_PORT))
    mreq = socket.inet_aton(MCAST_GROUP) + socket.inet_aton("0.0.0.0")
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    while True:
        pkt, _ = sock.recvfrom(65536)
        try:
            m = json.loads(pkt)
        except Exception:
            continue
        typ = m.get("type")
        inst = m.get("instrument_id")
        if typ not in ("ORDER", "TRADE", "CANCEL") or inst is None:
            continue
        with LIVE.lock:
            book = LIVE.book[inst]
            if typ == "ORDER" and m.get("order_type") == "GTC":
                side = "bid" if m["side"] == "BUY" else "ask"
                book[side][int(m["price_cents"])] += int(m["remaining_quantity"])
            elif typ == "TRADE":
                LIVE.trades[inst].append(m)
                px = int(m["price_cents"])
                q = int(m["quantity"])
                # Deduct from the resting side:
                if m["maker_is_buyer"]:
                    book["bid"][px] = max(book["bid"].get(px, 0) - q, 0)
                else:
                    book["ask"][px] = max(book["ask"].get(px, 0) - q, 0)
            elif typ == "CANCEL":
                px = m.get("price_cents")
                side = m.get("side")
                if px is not None and side:
                    s = "bid" if side == "BUY" else "ask"
                    book[s].pop(int(px), None)

threading.Thread(target=multicast_listener, daemon=True).start()


# ────────── Dash App Setup ─────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="Redleaf Exchange",
    external_stylesheets=[dbc.themes.SLATE],
    suppress_callback_exceptions=True
)

# Custom favicon (place `favicon.ico` in ./assets/)
app.index_string = f"""
<!DOCTYPE html>
<html>
<head>
{{%metas%}}
<title>{{%title%}}</title>
<link rel="shortcut icon" href="/assets/favicon.ico">
{{%favicon%}}
{{%css%}}
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;500&display=swap" rel="stylesheet">
<style>
  body {{ background:{BG}; color:{ORANGE_TXT}; font-family:{FONT_FAMILY}; }}
  .bg-dark   {{ background:{BG} !important; }}
  .text-light{{ color:{ORANGE_TXT} !important; }}
  ::-webkit-scrollbar {{
      width:6px; height:6px;
  }}
  ::-webkit-scrollbar-thumb {{
      background:#444;
  }}
</style>
</head>
<body>
  {{%app_entry%}}
  <footer>{{%config%}}{{%scripts%}}{{%renderer%}}</footer>
</body>
</html>
"""


# ────────── Helpers ────────────────────────────────────────────────────

def dropdown_options():
    """
    Build dropdown options for instruments.  Tooltip shows created_by/time + description.
    """
    opts = []
    for inst in _instruments:
        label = inst["instrument_name"]
        val   = inst["instrument_id"]
        created_by = _parties.get(inst["created_by"], str(inst["created_by"]))
        tip = (
            f"Created {inst['created_time']} by {created_by}\n"
            f"{inst['instrument_description']}"
        )
        opts.append({"label": label, "value": val, "title": tip})
    return opts


def book_to_rows(bid_dict_raw, ask_dict_raw):
    """
    Convert two dicts {price_str:qty, ...} →
      • rows: [ { "BidQty", "Price", "AskQty" }, … ]
      • columns: definitions
      • style_data_conditional: colored bars + alignment
    """
    bid_int = {int(p): int(q) for p, q in bid_dict_raw.items()}
    ask_int = {int(p): int(q) for p, q in ask_dict_raw.items()}

    prices = set()
    for px, qty in bid_int.items():
        if qty > 0:
            prices.add(px)
    for px, qty in ask_int.items():
        if qty > 0:
            prices.add(px)
    if not prices:
        return [], [], []

    all_prices = sorted(prices)
    max_bid = max((bid_int.get(px, 0) for px in all_prices), default=1)
    max_ask = max((ask_int.get(px, 0) for px in all_prices), default=1)

    rows = []
    styles = []
    for i, px in enumerate(all_prices):
        bq = bid_int.get(px, 0)
        aq = ask_int.get(px, 0)
        rows.append({
            "BidQty": bq if bq > 0 else "",
            "Price" : no_dollar(px),
            "AskQty": aq if aq > 0 else ""
        })
        if bq > 0:
            pct = min(int(bq / max_bid * 100), 100)
            styles.append({
                "if": {"row_index": i, "column_id": "BidQty"},
                "background": f"linear-gradient(90deg, {BID_BAR} 0%, {BID_BAR} {pct}%, transparent {pct}%)",
                "color": BID_FONT,
                "textAlign": "left"
            })
        if aq > 0:
            pct = min(int(aq / max_ask * 100), 100)
            styles.append({
                "if": {"row_index": i, "column_id": "AskQty"},
                "background": (
                    f"linear-gradient(90deg, transparent 0%, transparent {100-pct}%, "
                    f"{ASK_BAR} {100-pct}%, {ASK_BAR} 100%)"
                ),
                "color": ASK_FONT,
                "textAlign": "right"
            })

    columns = [
        {"name": "BidQty", "id": "BidQty"},
        {"name": "Price" , "id": "Price"},
        {"name": "AskQty", "id": "AskQty"},
    ]
    styles.append({"if": {"column_id": "BidQty"}, "textAlign": "left"})
    styles.append({"if": {"column_id": "Price"},  "textAlign": "center"})
    styles.append({"if": {"column_id": "AskQty"}, "textAlign": "right"})

    return rows, columns, styles


def compute_positions(trades):
    """
    For each party:
      • NetQty = (sum of buys) – (sum of sells)
      • LastTradeTime = timestamp of their last trade
      • AveragePrice = (abs(total dollar value) / abs(NetQty))
    Returns rows: [ { "Party", "NetQty", "LastTradeTime", "AveragePrice" }, ... ]
    """
    pos_map = defaultdict(lambda: {"qty": 0, "value_cents": 0, "last_ts": None})
    for t in trades:
        maker = int(t["maker_party_id"])
        taker = int(t["taker_party_id"])
        qty   = int(t["quantity"])
        px    = int(t["price_cents"])
        ts    = t["timestamp"]

        # Maker side
        if t["maker_is_buyer"]:
            pos_map[maker]["qty"] += qty
            pos_map[maker]["value_cents"] += px * qty
        else:
            pos_map[maker]["qty"] -= qty
            pos_map[maker]["value_cents"] -= px * qty

        # Taker side (opposite of maker)
        if t["maker_is_buyer"]:
            pos_map[taker]["qty"] -= qty
            pos_map[taker]["value_cents"] -= px * qty
        else:
            pos_map[taker]["qty"] += qty
            pos_map[taker]["value_cents"] += px * qty

        for pid in (maker, taker):
            rec = pos_map[pid]
            if rec["last_ts"] is None or ts > rec["last_ts"]:
                rec["last_ts"] = ts

    rows = []
    for pid, rec in pos_map.items():
        net_qty = rec["qty"]
        if net_qty == 0:
            avg_price = 0.0
        else:
            avg_price = abs(rec["value_cents"]) / abs(net_qty) / 100
        rows.append({
            "Party"         : _parties.get(pid, str(pid)),
            "NetQty"        : net_qty,
            "LastTradeTime" : format_dt(rec["last_ts"]),
            "AveragePrice"  : f"{avg_price:,.2f}",
        })
    # Sort descending by absolute net quantity
    rows.sort(key=lambda r: abs(r["NetQty"]), reverse=True)
    return rows


def build_table(title, tbl_id, parent_height):
    """
    Returns a dbc.Card that is exactly parent_height tall,
    whose DataTable flex‐fills the remainder beneath a 2rem header.
    Column headers are center-aligned, and the table is forced to scroll if too many rows.
    """
    HEADER_H = "2rem"
    return dbc.Card(
        [
            # CardHeader (title)
            dbc.CardHeader(
                html.Span(title, style={"fontWeight": "900", "color": ORANGE_TXT}),
                className="bg-dark text-light p-1",
                style={"height": HEADER_H, "lineHeight": HEADER_H}
            ),
            # Div that holds the DataTable and makes it fill the remaining height
            html.Div(
                dash_table.DataTable(
                    id=tbl_id,
                    page_action="none",
                    fixed_rows={"headers": True},
                    style_header={
                        "backgroundColor": BG,
                        "color": ORANGE_TXT,
                        "fontWeight": "900",
                        "border": "none",
                        "fontSize": CELL_FONT_SZ,
                        "textAlign": "center",          # ← center-align headers
                    },
                    style_cell={
                        "backgroundColor": BG,
                        "color": ORANGE_TXT,
                        "border": "none",
                        "fontFamily": FONT_FAMILY,
                        "fontSize": CELL_FONT_SZ,
                        "padding": "3px"
                    },
                    style_table={
                        "maxHeight": "100%",            # ← enforce scroll
                        "overflowY": "auto",
                        "backgroundColor": BG,
                    },
                ),
                style={"flex": "1 1 auto", "height": f"calc(100% - {HEADER_H})"}
            ),
        ],
        className="shadow",
        style={
            "backgroundColor": BG,
            "border": "none",
            "height": parent_height,
            "display": "flex",
            "flexDirection": "column",
        },
    )


# ────────── Build Dash layout ────────────────────────────────────────────
app.layout = dbc.Container(
    [
        # ── Banner: Instrument Name (white) | Description | Last Price (green), etc.
        html.Div(id="banner", className="mb-2"),

        # ── Order Entry + Cancel All
        dbc.Card(
            dbc.CardBody(
                dbc.Row(
                    [
                        dbc.Col(
                            dcc.Dropdown(
                                id="dd-instr",
                                options=dropdown_options(),
                                value=_instruments[0]["instrument_id"],
                                clearable=False,
                                placeholder="Select Instrument"
                            ), width=3
                        ),
                        dbc.Col(dbc.Input(id="in-party", type="number", placeholder="Party ID"), width=1),
                        dbc.Col(dbc.Input(id="in-pwd", type="password", placeholder="Password"), width=1),
                        dbc.Col(dbc.Input(id="in-qty", type="number", placeholder="Quantity"), width=1),
                        dbc.Col(dbc.Input(id="in-price", type="text", placeholder="Price (e.g. 101.23)"), width=1),
                        dbc.Col(
                            dbc.Select(
                                id="in-side",
                                options=[{"label": "BUY", "value": "BUY"},
                                         {"label": "SELL", "value": "SELL"}],
                                value="BUY",
                            ), width=1
                        ),
                        dbc.Col(
                            dbc.Button("SEND GTC", id="btn-gtc", color="success", size="sm", n_clicks=0),
                            width=1
                        ),
                        dbc.Col(
                            dbc.Button("SEND IOC", id="btn-ioc", color="warning", size="sm", n_clicks=0),
                            width=1
                        ),
                        dbc.Col(
                            dbc.Button("Cancel All Open Orders", id="btn-cancel-all", color="danger", size="sm"),
                            width=2
                        ),
                    ],
                    className="gy-1",
                )
            ),
            className="shadow mb-2",
            style={"backgroundColor": BG},
        ),

        html.Small(id="lbl-msg", className="mb-2", style={"color": ORANGE_TXT, "fontSize": "0.8rem"}),

        # ── Row: Order Book (50vh) on Left, Price Chart (40vh) + Recent Trades (30vh) on Right ─────
        dbc.Row(
            [
                # Left: Order Book = 50vh
                dbc.Col(
                    html.Div(
                        build_table("Order Book (prices in cents)", "tbl-book", "100%"),
                        style={"height": BOOK_H}
                    ),
                    width=6
                ),
                # Right: Price Chart (40vh) above Recent Trades (30vh)
                dbc.Col(
                    [
                        # Price Chart (40vh)
                        html.Div(
                            dbc.Card(
                                [
                                    dbc.CardHeader(
                                        html.Span("Price Chart", style={"fontWeight": "900", "color": ORANGE_TXT}),
                                        className="bg-dark text-light p-1",
                                        style={"height": "2rem", "lineHeight": "2rem"}
                                    ),
                                    dcc.Graph(id="fig-price", style={"height": "100%"}),
                                ],
                                className="shadow mb-2",
                                style={"backgroundColor": BG, "border": "none", "height": PRICE_CH_H},
                            ),
                            style={"marginBottom": "0.5rem"}
                        ),
                        # Recent Trades (30vh, scrollable)
                        html.Div(
                            build_table("Recent Trades", "tbl-trades", "100%"),
                            style={"height": TRADES_H}
                        ),
                    ],
                    width=6
                ),
            ],
            className="gx-2 mb-2",
        ),

        # ── Row: Open Orders (Me) on Left (input + table), Positions (Everyone) on Right ─
        dbc.Row(
            [
                # Left: Open Orders (Me)
                dbc.Col(
                    html.Div(
                        [
                            # Card container
                            dbc.Card(
                                [
                                    # Header
                                    dbc.CardHeader(
                                        html.Span("Open Orders (Me)", style={"fontWeight": "900", "color": ORANGE_TXT}),
                                        className="bg-dark text-light p-1",
                                        style={"height": "2rem", "lineHeight": "2rem"}
                                    ),
                                    # Input area (5vh)
                                    html.Div(
                                        dbc.Row(
                                            [
                                                dbc.Col(dbc.Input(id="o_party", type="number", placeholder="Party ID"), width=3),
                                                dbc.Col(dbc.Input(id="o_pwd", type="password", placeholder="Password"), width=3),
                                                dbc.Col(dbc.Button("Fetch My Orders", id="btn-fetch-open", color="primary", size="sm"), width=3),
                                                html.Div(id="open-msg", style={"color": ORANGE_TXT, "fontSize": "0.75rem"}),
                                            ],
                                            className="gx-1",
                                            style={"height": OPEN_INPUT_H, "padding": "0.25rem"}
                                        ),
                                        style={"height": OPEN_INPUT_H}
                                    ),
                                    # Table area (25vh, scrollable)
                                    html.Div(
                                        id="open-orders-table",
                                        style={
                                            "height": OPEN_TABLE_H,
                                            "overflowY": "auto",
                                            "padding": "0.25rem"
                                        }
                                    ),
                                ],
                                className="shadow",
                                style={
                                    "backgroundColor": BG,
                                    "border": "none",
                                    "display": "flex",
                                    "flexDirection": "column",
                                },
                            )
                        ],
                    ),
                    width=6
                ),

                # Right: Positions (Everyone) – 25vh scrollable
                dbc.Col(
                    html.Div(
                        build_table("Positions (Everyone)", "tbl-pos", "100%"),
                        style={"height": POS_H}
                    ),
                    width=6
                ),
            ],
            className="gx-2 mb-2",
        ),

        # Hidden Stores + Interval
        dcc.Interval(id="tick", interval=REFRESH_MS),
        dcc.Store(id="store-book"),
        dcc.Store(id="store-trades"),
        dcc.Store(id="store-open-raw"),
    ],
    fluid=True,
    className="pt-2",
)


# ─────────── Callbacks ──────────────────────────────────────────────────

# 1) Periodically push LIVE.book & LIVE.trades into dcc.Stores
@app.callback(
    Output("store-book", "data"),
    Output("store-trades", "data"),
    Input("tick", "n_intervals"),
    State("dd-instr", "value"),
)
def update_live_data(_, inst_id):
    with LIVE.lock:
        book = LIVE.book[inst_id]
        trades = list(LIVE.trades[inst_id])
    return book, trades


# 2) Render the top Banner (Instrument info + Last Price + LastMaker/LastTaker)
@app.callback(
    Output("banner", "children"),
    Input("store-trades", "data"),
    State("dd-instr", "value"),
)
def render_banner(trades, inst_id):
    info = next((x for x in _instruments if x["instrument_id"] == inst_id), {})
    name = info.get("instrument_name", "")
    desc = info.get("instrument_description", "")
    created_by = _parties.get(info.get("created_by"), str(info.get("created_by")))
    created_time = info.get("created_time", "")

    last = trades[-1] if trades else {}
    lp_cs = int(last.get("price_cents", 0)) if last else 0
    last_price = dollars(lp_cs) if trades else "--"
    last_time = format_dt(last.get("timestamp", 0)) if trades else "--"
    last_maker = _parties.get(last.get("maker_party_id"), str(last.get("maker_party_id")))
    last_taker = _parties.get(last.get("taker_party_id"), str(last.get("taker_party_id")))

    return dbc.Alert(
        children=[
            # Instrument Name (white)
            html.Span(f"{name}", style={"fontWeight": "900", "color": WHITE_TXT, "marginRight": "1rem"}),
            # Description (orange italic)
            html.Span(f"{desc}", style={"fontStyle": "italic", "fontSize": "0.9rem", "color": ORANGE_TXT}),
            # Last Price (green)
            html.Span(f"  | Last Price: ", style={"marginLeft": "2rem", "fontWeight": "500", "color": ORANGE_TXT}),
            html.Span(last_price, style={"color": LASTP_TXT, "fontWeight": "bold"}),
            html.Span(f" @ {last_time}", style={"marginLeft": "0.5rem", "color": ORANGE_TXT}),
            # Maker/Taker (small orange)
            html.Div(
                f" LastMaker: {last_maker}  |  LastTaker: {last_taker}",
                style={"fontSize": "0.85rem", "marginTop": "4px", "color": ORANGE_TXT},
            ),
            # Created info (gray)
            html.Div(
                f"(Created {created_time} by {created_by})",
                style={"fontSize": "0.75rem", "marginTop": "2px", "color": "#888888"},
            ),
        ],
        color="dark",
        className="py-1 px-2",
        style={"backgroundColor": BG, "border": "1px solid #444"},
    )


# 3) Render Order Book
@app.callback(
    Output("tbl-book", "data"),
    Output("tbl-book", "columns"),
    Output("tbl-book", "style_data_conditional"),
    Input("store-book", "data"),
)
def render_book(book_data):
    if not book_data:
        return [], [], []
    bid_dict = book_data["bid"]
    ask_dict = book_data["ask"]
    rows, cols, styles = book_to_rows(bid_dict, ask_dict)
    return rows, cols, styles


# 4) Render Recent Trades & Price Chart
@app.callback(
    Output("tbl-trades", "data"),
    Output("tbl-trades", "columns"),
    Output("fig-price", "figure"),
    Input("store-trades", "data"),
)
def render_trades_and_chart(trades_data):
    rows = []
    xs, ys = [], []
    for t in trades_data[-MAX_TRADES:]:
        ts_str = format_dt(t["timestamp"])
        px    = int(t["price_cents"])
        qty   = int(t["quantity"])
        total = (px * qty) / 100  # in dollars
        rows.append({
            "Time"  : ts_str,
            "Price" : no_dollar(px),
            "Qty"   : qty,
            "Total" : f"{total:,.2f}",
            "Maker" : _parties.get(t["maker_party_id"], str(t["maker_party_id"])),
            "Taker" : _parties.get(t["taker_party_id"], str(t["taker_party_id"])),
        })
        xs.append(datetime.datetime.fromtimestamp(t["timestamp"] / 1e9))
        ys.append(px / 100)

    trade_cols = [
        {"name": "Time" , "id": "Time"},
        {"name": "Price", "id": "Price"},
        {"name": "Qty"  , "id": "Qty"},
        {"name": "Total", "id": "Total"},
        {"name": "Maker", "id": "Maker"},
        {"name": "Taker", "id": "Taker"},
    ]

    fig = go.Figure()
    if xs and ys:
        fig.add_trace(go.Scatter(x=xs, y=ys, mode="lines", name="Price", line=dict(color=LASTP_TXT)))
        if len(ys) >= 20:
            ma = [sum(ys[i - 19:i + 1]) / 20 for i in range(19, len(ys))]
            fig.add_trace(
                go.Scatter(
                    x=xs[19:],
                    y=ma,
                    mode="lines",
                    name="20-MA",
                    line=dict(width=1, dash="dot", color="#888888"),
                )
            )
    fig.update_layout(
        template="plotly_dark",
        margin=dict(l=0, r=0, t=10, b=25),
        xaxis_title="",
        yaxis_title="Price ($)",
        font=dict(family=FONT_FAMILY, color=ORANGE_TXT),
    )

    return rows, trade_cols, fig


# 5) Fetch “Open Orders (Me)” on button click
@app.callback(
    Output("store-open-raw", "data"),
    Output("open-msg", "children"),
    Input("btn-fetch-open", "n_clicks"),
    State("o_party", "value"),
    State("o_pwd", "value"),
    State("dd-instr", "value"),
)
def fetch_open_orders(nc, pid, pwd, inst_id):
    if not nc:
        return dash.no_update, dash.no_update
    if pid is None or pwd is None:
        return dash.no_update, "⚠ Enter Party ID & Password"
    try:
        raw = requests.get(f"{API_URL}/live_orders/{inst_id}", timeout=3).json()
    except Exception as e:
        return dash.no_update, f"❌ Network error: {e}"
    mine = [r for r in raw if int(r["party_id"]) == int(pid)]
    rows = []
    for r in sorted(mine, key=lambda x: x["order_id"]):
        rows.append({
            "OID"   : r["order_id"],
            "Side"  : r["side"],
            "Price" : no_dollar(int(r["price_cents"])),
            "Qty"   : int(r["remaining_quantity"]),
        })
    return rows, ""


# 6) Render “Open Orders (Me)” table with Cancel buttons
@app.callback(
    Output("open-orders-table", "children"),
    Input("store-open-raw", "data"),
)
def render_open_table(open_rows):
    if not open_rows:
        return html.Div("No open orders or not fetched yet.", style={"color": ORANGE_TXT, "fontSize": "0.75rem"})
    # Build an HTML table manually so we can embed a Button in each row
    header = html.Tr([
        html.Th("OID", style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Side", style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Price", style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Qty", style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Action", style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
    ])
    rows = []
    for r in open_rows:
        oid = r["OID"]
        rows.append(
            html.Tr([
                html.Td(oid, style={"textAlign": "center", "fontSize": CELL_FONT_SZ}),
                html.Td(r["Side"], style={"textAlign": "center", "fontSize": CELL_FONT_SZ}),
                html.Td(r["Price"], style={"textAlign": "center", "fontSize": CELL_FONT_SZ}),
                html.Td(r["Qty"], style={"textAlign": "center", "fontSize": CELL_FONT_SZ}),
                html.Td(
                    dbc.Button(
                        "Cancel",
                        id={"type": "cancel-open", "index": oid},
                        color="danger", size="sm", n_clicks=0
                    ),
                    style={"textAlign": "center"}
                ),
            ])
        )
    table = html.Table(
        [html.Thead(header)] + [html.Tbody(rows)],
        style={"width": "100%", "borderSpacing": "0", "tableLayout": "fixed"}
    )
    return table


# 7) Handle “Cancel” clicks in Open Orders (Me)
@app.callback(
    Output("lbl-msg", "children", allow_duplicate=True),
    Input({"type": "cancel-open", "index": ALL}, "n_clicks"),
    State("store-open-raw", "data"),
    State("o_party", "value"),
    State("o_pwd", "value"),
    State("dd-instr", "value"),
    prevent_initial_call=True
)
def cancel_open(n_clicks_list, open_rows, pid, pwd, inst_id):
    ctx = callback_context.triggered
    if not ctx:
        return dash.no_update
    triggered_id = ctx[0]["prop_id"].split(".")[0]
    triggered_id = json.loads(triggered_id)
    oid_clicked  = triggered_id["index"]
    if pid is None or pwd is None:
        return "⚠ Need Party ID & Password to cancel"
    payload = {
        "instrument_id": inst_id,
        "order_id"     : oid_clicked,
        "party_id"     : pid,
        "password"     : pwd,
    }
    try:
        resp = requests.post(f"{API_URL}/cancel", json=payload, timeout=4).json()
    except Exception as e:
        return f"❌ Network Error: {e}"
    if resp.get("status") == "CANCELLED":
        return "✓ Order Cancelled"
    else:
        detail = resp.get("details", resp)
        return f"❌ {detail}"


# 8) Cancel All Open Orders for My Party
@app.callback(
    Output("lbl-msg", "children", allow_duplicate=True),
    Input("btn-cancel-all", "n_clicks"),
    State("store-open-raw", "data"),
    State("o_party", "value"),
    State("o_pwd", "value"),
    State("dd-instr", "value"),
    prevent_initial_call=True,
)
def cancel_all(nc, open_rows, pid, pwd, inst_id):
    if not nc or not open_rows or pid is None or pwd is None:
        return dash.no_update
    errs = []
    for r in open_rows:
        oid = r["OID"]
        payload = {
            "instrument_id": inst_id,
            "order_id"     : oid,
            "party_id"     : pid,
            "password"     : pwd,
        }
        try:
            resp = requests.post(f"{API_URL}/cancel", json=payload, timeout=4).json()
            if resp.get("status") != "CANCELLED":
                errs.append(str(resp.get("details", resp)))
        except Exception as e:
            errs.append(str(e))
    if errs:
        return f"❌ Some cancels failed: {'; '.join(errs)}"
    return "✓ All orders cancelled"


# 9) Send New Order (GTC or IOC)
@app.callback(
    Output("lbl-msg", "children", allow_duplicate=True),
    Input("btn-gtc", "n_clicks"),
    Input("btn-ioc", "n_clicks"),
    State("dd-instr", "value"),
    State("in-party", "value"),
    State("in-pwd", "value"),
    State("in-qty", "value"),
    State("in-price", "value"),
    State("in-side", "value"),
    prevent_initial_call=True,
)
def send_new_order(n_gtc, n_ioc, inst_id, pid, pwd, qty, price_txt, side):
    ctx = callback_context.triggered[0]["prop_id"].split(".")[0]
    typ = "GTC" if ctx == "btn-gtc" else "IOC"
    if None in (pid, pwd, qty, price_txt):
        return "⚠ Please fill Party ID, Password, Qty & Price"
    try:
        px_cs = to_cents(price_txt)
    except Exception:
        return "⚠ Bad price format (e.g. 101.23)"
    payload = {
        "instrument_id": inst_id,
        "side"         : side,
        "order_type"   : typ,
        "quantity"     : int(qty),
        "price_cents"  : px_cs,
        "party_id"     : int(pid),
        "password"     : pwd,
    }
    try:
        resp = requests.post(f"{API_URL}/orders", json=payload, timeout=4).json()
    except Exception as e:
        return f"❌ Network Error: {e}"
    if resp.get("status") == "ACCEPTED":
        return "✓ Order Accepted"
    else:
        detail = resp.get("details", resp)
        return f"❌ {detail}"


# 10) Render Positions (Everyone)
@app.callback(
    Output("tbl-pos", "data"),
    Output("tbl-pos", "columns"),
    Input("store-trades", "data"),
)
def render_positions(trades_data):
    rows = compute_positions(trades_data)
    cols = [
        {"name": "Party"         , "id": "Party"},
        {"name": "NetQty"        , "id": "NetQty"},
        {"name": "LastTradeTime" , "id": "LastTradeTime"},
        {"name": "AveragePrice"  , "id": "AveragePrice"},
    ]
    return rows, cols


# ────────── Run the Dash app ───────────────────────────────────────────
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("DASH_PORT", "8050")), debug=True)

