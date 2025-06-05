# exchange_dash_app.py  –  Redleaf Exchange Dashboard (Bloomberg‐inspired)
import os
import json
import datetime
from collections import defaultdict
from time import time

import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, dash_table, Input, Output, State, callback_context, ALL
import plotly.graph_objects as go
import requests

from apps.trader.click_trader.exchange_dash_app_utils import dollars, no_dollar, to_cents, format_dt

# ╭──────────────────────────────── CONFIG ─────────────────────────────╮
API_URL      = os.getenv("API_URL", "http://localhost:8000")
REFRESH_MS   = 500               # UI refresh interval (ms)
MAX_TRADES   = 800                # Keep last N trades in memory

# Heights:
BOOK_H        = "20vh"            # Order Book
PRICE_CH_H    = "20vh"            # Price Chart
TRADES_H      = "20vh"            # Recent Trades
OPEN_INPUT_H  = "10vh"            # Open Orders (Me) input
OPEN_TABLE_H  = "20vh"            # Open Orders (Me) table
POS_H         = "20vh"            # Positions (Everyone)

# Colors / fonts:
BG            = "#1f2124"         # Soft dark‐gray background
ORANGE_TXT    = "#fb8b1e"         # Bloomberg‐orange
RED_TXT       = "#FF4D4D"         # Bloomberg‐orange
WHITE_TXT     = "#FFFFFF"         # Pure white (instrument name, button text)
LASTP_TXT     = "#00b050"         # Bright green (last price + numeric stats)
BID_BAR       = "rgba(0,176,80,0.35)"   # Semi‐transparent green
ASK_BAR       = "rgba(230,74,25,0.35)"  # Semi‐transparent red
BID_FONT      = "#00b050"
ASK_FONT      = "#e64a19"
FONT_FAMILY   = "'3270', 'IBM Plex Mono', monospace"
CELL_FONT_SZ  = "0.75rem"         # Table cell font size
INPUT_FONT_SZ = "0.9rem"          # Input / dropdown font size


_raw = requests.get(f"{API_URL}/instruments").json()
if not _raw:
    raise SystemExit("❌ No instruments found. Create at least one via API.")

_instruments = sorted(_raw, key=lambda x: x["instrument_id"], reverse=True)
_parties = {p["party_id"]: p["party_name"] for p in requests.get(f"{API_URL}/parties").json()}


# ────────── Dash Stores for live data ──────────────────────────────────
dcc.BookStore       = dcc.Store(id="store-book")
dcc.TradeStore      = dcc.Store(id="store-trades")
dcc.OrderIDsStore   = dcc.Store(id="store-order-ids")
dcc.LastTradeStore  = dcc.Store(id="store-last-trade-ts")
dcc.OpenStore       = dcc.Store(id="store-open-raw")
dcc.StateStore      = dcc.Store(id="old-state")


# ────────── Dash App Setup ─────────────────────────────────────────────
app = dash.Dash(
    __name__,
    title="Redleaf Exchange",
    external_stylesheets=[dbc.themes.SLATE],
    suppress_callback_exceptions=True
)
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
  body {{
    background:{BG};
    color:{ORANGE_TXT};
    font-family:{FONT_FAMILY};
  }}
  .bg-dark   {{ background:{BG} !important; }}
  .text-light{{ color:{ORANGE_TXT} !important; }}

  /* ── Make button text white and prevent overflow ── */
  .btn {{
    color: {WHITE_TXT} !important;
    white-space: nowrap !important;
    overflow: hidden !important;
    text-overflow: ellipsis !important;
    height: 2.5rem !important;   /* match input height */
    font-size: {INPUT_FONT_SZ} !important;
  }}

  /* ── Style all <input> to look like the rest of the theme ── */
  input {{
    background-color:{BG} !important;
    color:{ORANGE_TXT} !important;
    font-size:{INPUT_FONT_SZ} !important;
    border:1px solid #444 !important;
    border-radius:4px !important;
    padding-left:0.5rem !important;
    box-sizing: border-box;
  }}
  input::placeholder {{
    color:{ORANGE_TXT} !important;
  }}
  input:focus {{
    outline: none !important;
    box-shadow: none !important;
  }}

  /* Remove custom dropdown CSS entirely—let them stay white */
  /* (No additional rules here for select/react-select) */

  /* Remove default “Updating…” spinner */
  .dash-loading {{
    visibility: hidden !important;
  }}

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


# ────────────────────────────────────────────────────────────────────────
# ──── Helpers ──────────────────────────────────────────────────────────
# ────────────────────────────────────────────────────────────────────────
def dropdown_options():
    opts = []
    for inst in _instruments:
        label = inst["instrument_name"]
        val   = inst["instrument_id"]
        created_by = _parties.get(inst["created_by"], str(inst["created_by"]))
        tip = (
            f"Instrument created on {inst['created_time']} by {created_by}\n"
            f"{inst['instrument_description']}"
        )
        opts.append({"label": label, "value": val, "title": tip})
    return opts


def book_to_rows(bid_dict_raw, ask_dict_raw):
    bid_int = {int(p): int(q) for p, q in bid_dict_raw.items()}
    ask_int = {int(p): int(q) for p, q in ask_dict_raw.items()}

    prices = {px for px, qty in bid_int.items() if qty > 0} | \
             {px for px, qty in ask_int.items() if qty > 0}

    if not prices:
        return [], [], []

    all_prices = sorted(prices, reverse=True)
    max_bid = max(bid_int.get(px, 0) for px in all_prices) or 1
    max_ask = max(ask_int.get(px, 0) for px in all_prices) or 1

    rows = []
    styles = []
    for i, px in enumerate(all_prices):
        bq = bid_int.get(px, 0)
        aq = ask_int.get(px, 0)
        price_str = f"{(px/100):.2f}"
        rows.append({
            "BidQty": bq if bq > 0 else "",
            "Price" : price_str,
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
        {"name": "Bid Qty",      "id": "BidQty"},
        {"name": "Price",        "id": "Price"},
        {"name": "Ask Qty",      "id": "AskQty"},
    ]
    styles.append({"if": {"column_id": "BidQty"}, "textAlign": "left"})
    styles.append({"if": {"column_id": "Price"},  "textAlign": "center"})
    styles.append({"if": {"column_id": "AskQty"}, "textAlign": "right"})

    return rows, columns, styles


def compute_positions(trades):
    pos_map = defaultdict(lambda: {
        "qty": 0,
        "value_cents": 0,
        "first_ts": None,
        "last_ts": None
    })

    for t in trades:
        maker = str(t["maker_party_id"])
        taker = str(t["taker_party_id"])
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
            if rec["first_ts"] is None or ts < rec["first_ts"]:
                rec["first_ts"] = ts
            if rec["last_ts"] is None or ts > rec["last_ts"]:
                rec["last_ts"] = ts

    rows = []
    for pid, rec in pos_map.items():
        net_qty = rec["qty"]
        if net_qty == 0:
            avg_price = 0.0
        else:
            avg_price = abs(rec["value_cents"]) / abs(net_qty) / 100
        net_value = net_qty * avg_price
        rows.append({
            "Party"           : _parties.get(pid, str(pid)),
            "NetQty"          : net_qty,
            "FirstTradeTime"  : format_dt(rec["first_ts"]),
            "LastTradeTime"   : format_dt(rec["last_ts"]),
            "AveragePrice"    : f"{avg_price:,.2f}",
            "NetValue"        : f"{net_value:,.2f}",
        })

    rows.sort(key=lambda r: abs(r["NetQty"]), reverse=True)
    return rows


def build_table(
    title, tbl_id, parent_min_height,
    data=None, columns=None, style_cond=None
):
    HEADER_H = "2rem"
    table_kwargs = {}
    if data is not None:
        table_kwargs["data"] = data
    if columns is not None:
        table_kwargs["columns"] = columns
    if style_cond is not None:
        table_kwargs["style_data_conditional"] = style_cond

    return dbc.Card(
        [
            dbc.CardHeader(
                html.Span(title, style={"fontWeight": "900", "color": ORANGE_TXT}),
                className="bg-dark text-light p-1",
                style={"height": HEADER_H, "lineHeight": HEADER_H}
            ),
            html.Div(
                dash_table.DataTable(
                    id=tbl_id,
                    page_action="none",
                    fixed_rows={"headers": True},
                    # — Ensure *every* cell is dark by default:
                    style_data={"backgroundColor": BG, "color": ORANGE_TXT},
                    style_header={
                        "backgroundColor": BG,
                        "color": ORANGE_TXT,
                        "fontWeight": "900",
                        "border": "none",
                        "fontSize": CELL_FONT_SZ,
                        "textAlign": "center",
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
                        "backgroundColor": BG,
                        "minHeight": "100%",
                        "overflowY": "auto",
                    },
                    **table_kwargs,  # inject data/columns/style_data_conditional here
                ),
                style={"flex": "1 1 auto", "height": f"calc(100% - {HEADER_H})"}
            ),
        ],
        className="shadow",
        style={
            "backgroundColor": BG,
            "border": "none",
            "minHeight": parent_min_height,
            "display": "flex",
            "flexDirection": "column",
        },
    )


# ────────── Build Dash layout ───────────────────────────────────────────
app.layout = dbc.Container(
    [
        html.Div(id="banner", className="mb-2"),

        dbc.Card(
            dbc.CardBody(
                dbc.Row(
                    [
                        # 1) Instrument selector (plain white dropdown)
                        dbc.Col(
                            dcc.Dropdown(
                                id="dd-instr",
                                options=dropdown_options(),
                                value=_instruments[0]["instrument_id"],
                                searchable=False,
                                clearable=False,
                                placeholder="Select Instrument",
                                # Remove all custom styling—let default be basic white
                                style={
                                    "height": "2.5rem",
                                    "fontSize": INPUT_FONT_SZ
                                }
                            ), width=3
                        ),
                        # 2) Party ID
                        dbc.Col(
                            dbc.Input(
                                id="in-party",
                                type="text",
                                placeholder="Party ID",
                                persistence=True,
                                persistence_type="local",
                                style={
                                    "height": "2.5rem",
                                    "backgroundColor": BG,
                                    "color": ORANGE_TXT,
                                    "border": "1px solid #444",
                                    "borderRadius": "4px",
                                    "paddingLeft": "0.5rem",
                                }
                            ), width=2
                        ),
                        # 3) Password
                        dbc.Col(
                            dbc.Input(
                                id="in-pwd",
                                type="password",
                                placeholder="Password",
                                persistence=True,
                                persistence_type="local",
                                style={
                                    "height": "2.5rem",
                                    "backgroundColor": BG,
                                    "color": ORANGE_TXT,
                                    "border": "1px solid #444",
                                    "borderRadius": "4px",
                                    "paddingLeft": "0.5rem",
                                }
                            ), width=2
                        ),
                        # 4) Quantity
                        dbc.Col(
                            dbc.Input(
                                id="in-qty",
                                type="number",
                                placeholder="Quantity",
                                style={
                                    "height": "2.5rem",
                                    "backgroundColor": BG,
                                    "color": ORANGE_TXT,
                                    "border": "1px solid #444",
                                    "borderRadius": "4px",
                                    "paddingLeft": "0.5rem",
                                }
                            ), width=2
                        ),
                        # 5) Price
                        dbc.Col(
                            dbc.Input(
                                id="in-price",
                                type="text",
                                placeholder="Price (e.g. 101.23)",
                                style={
                                    "height": "2.5rem",
                                    "backgroundColor": BG,
                                    "color": ORANGE_TXT,
                                    "border": "1px solid #444",
                                    "borderRadius": "4px",
                                    "paddingLeft": "0.5rem",
                                }
                            ), width=2
                        ),
                        # 6) OrderType (GTC/IOC) – plain white dropdown
                        dbc.Col(
                            dcc.Dropdown(
                                id="in-otyp",
                                options=[
                                    {"label": "GTC", "value": "GTC"},
                                    {"label": "IOC", "value": "IOC"}
                                ],
                                value="GTC",
                                searchable=False,
                                clearable=False,
                                style={
                                    "height": "2.5rem",
                                    "fontSize": INPUT_FONT_SZ
                                }
                            ), width=2
                        ),
                        # 7) BUY button (green)
                        dbc.Col(
                            dbc.Button(
                                "BUY",
                                id="btn-buy",
                                color="success",
                                style={"width": "100%", "whiteSpace": "nowrap"}
                            ), width=1
                        ),
                        # 8) SELL button (red)
                        dbc.Col(
                            dbc.Button(
                                "SELL",
                                id="btn-sell",
                                color="danger",
                                style={"width": "100%", "whiteSpace": "nowrap"}
                            ), width=1
                        ),
                        # 9) Cancel All – wider
                        dbc.Col(
                            dbc.Button(
                                "Cancel All Open Orders",
                                id="btn-cancel-all",
                                color="dark",
                                style={"width": "100%", "whiteSpace": "nowrap"}
                            ), width=3
                        ),
                    ],
                    className="gy-1",
                )
            ),
            className="shadow mb-2",
            style={"backgroundColor": BG},
        ),

        html.Small(
            id="lbl-msg",
            className="mb-2",
            style={"color": ORANGE_TXT, "fontSize": "0.8rem"}
        ),

        # ── Row: Order Book (50vh) left │ Price Chart (40vh) right ─────────
        dbc.Row(
            [
                # Left: Order Book (50vh)
                dbc.Col(
                    html.Div(
                        id="book-container",  # <-- placeholder
                        style={"minHeight": BOOK_H, "marginBottom": "0.5rem"}
                    ),
                    width=6
                ),

                # Right: Price Chart (40vh)
                dbc.Col(
                    html.Div(
                        dbc.Card(
                            [
                                dbc.CardHeader(
                                    html.Span(
                                        "Price Chart",
                                        style={"fontWeight": "900", "color": ORANGE_TXT}
                                    ),
                                    className="bg-dark text-light p-1",
                                    style={"height": "2rem", "lineHeight": "2rem"}
                                ),
                                dcc.Graph(
                                    id="fig-price",
                                    style={"height": "100%"}
                                ),
                            ],
                            className="shadow",
                            style={
                                "backgroundColor": BG,
                                "border": "none",
                                "minHeight": PRICE_CH_H
                            },
                        ),
                        style={"marginBottom": "0.5rem"}
                    ), width=6
                ),
            ],
            className="gx-2 mb-2",
        ),

        # ── Row: Recent Trades (50vh) │ Open Orders (Me: input 10vh + table 30vh) ─
        dbc.Row(
            [
                # Left: Recent Trades (50vh)
                dbc.Col(
                    html.Div(
                        build_table("Recent Trades", "tbl-trades", TRADES_H),
                        style={"minHeight": TRADES_H, "marginBottom": "0.5rem"}
                    ), width=6
                ),

                # Right: Open Orders (Me)
                dbc.Col(
                    html.Div(
                        dbc.Card(
                            [
                                # Header
                                dbc.CardHeader(
                                    html.Span(
                                        "Open Orders (Me)",
                                        style={"fontWeight": "900", "color": ORANGE_TXT}
                                    ),
                                    className="bg-dark text-light p-1",
                                    style={"height": "2rem", "lineHeight": "2rem"}
                                ),
                                # Input row (10vh)
                                html.Div(
                                    dbc.Row(
                                        [
                                            dbc.Col(
                                                dbc.Input(
                                                    id="o_party",
                                                    type="text",
                                                    placeholder="Party ID",
                                                    persistence=True,
                                                    persistence_type="local",
                                                    style={
                                                        "height": "2.5rem",
                                                        "backgroundColor": BG,
                                                        "color": ORANGE_TXT,
                                                        "border": "1px solid #444",
                                                        "borderRadius": "4px",
                                                        "paddingLeft": "0.5rem",
                                                    }
                                                ), width=4
                                            ),
                                            dbc.Col(
                                                dbc.Input(
                                                    id="o_pwd",
                                                    type="password",
                                                    placeholder="Password",
                                                    persistence=True,
                                                    persistence_type="local",
                                                    style={
                                                        "height": "2.5rem",
                                                        "backgroundColor": BG,
                                                        "color": ORANGE_TXT,
                                                        "border": "1px solid #444",
                                                        "borderRadius": "4px",
                                                        "paddingLeft": "0.5rem",
                                                    }
                                                ), width=4
                                            ),
                                        ],
                                        className="gy-1 px-2",
                                        style={"height": OPEN_INPUT_H}
                                    ),
                                    style={"minHeight": OPEN_INPUT_H, "paddingTop": "0.5rem"}
                                ),
                                # Table (30vh, scrollable)
                                html.Div(
                                    id="open-orders-table",
                                    style={
                                        "minHeight": OPEN_TABLE_H,
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
                    ), width=6
                ),
            ],
            className="gx-2 mb-2",
        ),

        # ── Row: Positions (Everyone) (50vh, full width) ───────────────────
        dbc.Row(
            [
                dbc.Col(
                    html.Div(
                        build_table("Positions (Everyone)", "tbl-pos", POS_H),
                        style={"minHeight": POS_H}
                    ),
                    width=12
                )
            ],
            className="gx-2 mb-2",
        ),

        # ── Hidden Stores + Interval ───────────────────────────────────────
        dcc.Interval(id="tick", interval=REFRESH_MS),
        html.Audio(
            id="refresh-audio",
            src="",            # start empty
            autoPlay=True,     # play whenever `src` changes
            style={"display": "none"},
        ),
        dcc.BookStore,
        dcc.TradeStore,
        dcc.OrderIDsStore,
        dcc.LastTradeStore,
        dcc.OpenStore,
        dcc.Store(id="old-state", data=0),
        dcc.Store(id="sound-played-ts", data=0),
    ],
    fluid=True,
    className="pt-2",
)

@app.callback(
    Output("refresh-audio", "src"),
    Output("sound-played-ts", "data"),
    Input("store-book", "data"),
    State("sound-played-ts", "data"),
    prevent_initial_call=True
)
def play_sound_once_per_half_second(book_data, last_ts):
    if book_data is None:
        return dash.no_update, dash.no_update

    now = time()
    if now - last_ts < 0.5:  # less than half a second
        return dash.no_update, last_ts

    return f"/assets/get-out.mp3?v={int(now*1000)}", now



# ─────────── Callbacks ──────────────────────────────────────────────────
@app.callback(
    Output("store-book",         "data"),
    Output("store-trades",       "data"),
    Output("store-order-ids",    "data"),
    Output("store-last-trade-ts","data"),
    Output("store-open-raw",     "data"),
    Output("old-state",         "data"),
    Input("tick",    "n_intervals"),
    Input("dd-instr", "value"),
    State("old-state","data"),
    State("o_party",         "value"),
    State("o_pwd",           "value"),
)
def update_everything(n_intervals, inst_id, old_state, pid, pwd):
    try:
        resp = requests.get(f"{API_URL}/action_count_seq", timeout=1).json()
        new_state = resp.get("seq")
    except Exception:
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update
    if old_state is not None and int(new_state) == int(old_state):
        return dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update

    print(f"New state for {inst_id}: {new_state}, old state: {old_state}")
    try:
        raw_live = requests.get(f"{API_URL}/live_orders/{inst_id}", timeout=1).json()
    except:
        raw_live = []
    new_book = {"bid": defaultdict(int), "ask": defaultdict(int)}
    new_order_ids = []
    for r in raw_live:
        px = int(r["price_cents"]); qty = int(r["remaining_quantity"])
        side_key = "bid" if r["side"] == "BUY" else "ask"
        new_book[side_key][px] += qty
        new_order_ids.append(r["order_id"])
    new_order_ids.sort()

    try:
        raw_trades = requests.get(f"{API_URL}/trades/{inst_id}", timeout=1).json()
    except:
        raw_trades = []
    sorted_trades = sorted(raw_trades, key=lambda x: x["timestamp"])
    if len(sorted_trades) > MAX_TRADES:
        sorted_trades = sorted_trades[-MAX_TRADES:]
    new_last_ts = sorted_trades[-1]["timestamp"] if sorted_trades else None

    my_open = []
    msg = ""
    if pid and pwd:
        mine = [r for r in raw_live if str(r["party_id"]) == str(pid)]
        my_open = [
            {
                "OID":   r["order_id"],
                "Side":  r["side"],
                "Price": no_dollar(int(r["price_cents"])),
                "Qty":   int(r["remaining_quantity"]),
            }
            for r in sorted(mine, key=lambda x: x["order_id"])
        ]
    else:
        msg = "Enter Party ID & Password below to see your open orders."

    return new_book, sorted_trades, new_order_ids, new_last_ts, my_open, int(new_state)


@app.callback(
    Output("banner", "children"),
    Input("store-trades", "data"),
    Input("store-book", "data"),
    State("dd-instr", "value"),
)
def render_banner(trades, book_data, inst_id):
    info = next((x for x in _instruments if x["instrument_id"] == inst_id), {})
    name = info.get("instrument_name", "")
    desc = info.get("instrument_description", "")
    created_by = _parties.get(info.get("created_by"), str(info.get("created_by")))
    created_time = info.get("created_time", "")

    # Last trade
    last = trades[-1] if trades else {}
    lp_cs = int(last.get("price_cents", 0)) if last else 0
    last_price = dollars(lp_cs) if trades else "--"
    last_time = format_dt(last.get("timestamp", 0)) if trades else "--"
    last_maker = _parties.get(last.get("maker_party_id"), str(last.get("maker_party_id")))
    last_taker = _parties.get(last.get("taker_party_id"), str(last.get("taker_party_id")))

    # Open Interest = sum of all quantities in book
    bid_sum = sum(book_data["bid"].values()) if book_data else 0
    ask_sum = sum(book_data["ask"].values()) if book_data else 0
    open_interest = bid_sum + ask_sum

    total_volume = sum(int(t["quantity"]) for t in trades)
    total_value_cents = sum(int(t["quantity"]) * int(t["price_cents"]) for t in trades)
    total_value = total_value_cents / 100

    return dbc.Card(
        [
            # ── Top row: Name (white) | Desc (orange italic) | Last Price (green) | ...
            dbc.CardBody(
                [
                    html.Span(
                        f"{name}",
                        style={"fontWeight": "900", "color": WHITE_TXT, "fontSize": "1.25rem"}
                    ),
                    html.Span(
                        f"{desc}",
                        style={"fontStyle": "italic", "fontSize": "0.9rem", "color": ORANGE_TXT, "marginLeft": "1rem"}
                    ),
                    html.Span(
                        f"  | Last Price: ",
                        style={"marginLeft": "2rem", "fontWeight": "500", "color": ORANGE_TXT}
                    ),
                    html.Span(
                        last_price,
                        style={"color": LASTP_TXT, "fontWeight": "bold", "fontSize": "1.1rem"}
                    ),
                    html.Span(
                        f" @ {last_time}",
                        style={"marginLeft": "0.5rem", "color": ORANGE_TXT}
                    ),
                    html.Div(
                        f"  LastMaker: {last_maker}  | LastTaker: {last_taker}",
                        style={"fontSize": "0.85rem", "marginTop": "6px", "color": ORANGE_TXT},
                    ),
                    html.Div(
                        f"Instrument created by {created_by} on {created_time}",
                        style={"fontSize": "0.75rem", "marginTop": "2px", "color": ORANGE_TXT},
                    ),
                ],
                style={"paddingBottom": "0.25rem"}
            ),
            # ── Bottom row: Open Interest / Volume / Value
            dbc.CardBody(
                [
                    html.Span(
                        "Open Interest: ",
                        style={"fontWeight": "500", "color": ORANGE_TXT, "fontSize": "0.9rem"}
                    ),
                    html.Span(
                        f"{open_interest}",
                        style={"color": LASTP_TXT, "fontWeight": "500", "fontSize": "0.9rem"}
                    ),
                    html.Span(
                        "  | Total Volume Traded: ",
                        style={"marginLeft": "2rem", "fontWeight": "500", "color": ORANGE_TXT, "fontSize": "0.9rem"}
                    ),
                    html.Span(
                        f"{total_volume}",
                        style={"color": LASTP_TXT, "fontWeight": "500", "fontSize": "0.9rem"}
                    ),
                    html.Span(
                        "  | Total Value Traded: ",
                        style={"marginLeft": "2rem", "fontWeight": "500", "color": ORANGE_TXT, "fontSize": "0.9rem"}
                    ),
                    html.Span(
                        f"{dollars(int(total_value * 100))}",
                        style={"color": LASTP_TXT, "fontWeight": "500", "fontSize": "0.9rem"}
                    ),
                ],
                style={"borderTop": "1px solid #444", "paddingTop": "0.25rem"}
            )
        ],
        className="shadow mb-2",
        style={"backgroundColor": BG, "padding": "0.5rem"}
    )


@app.callback(
    Output("book-container", "children"),
    Input("store-book", "data")
)
def redraw_book_entirely(book_data):
    if not book_data:
        return build_table("Order Book (prices in dollars)", "tbl-book", BOOK_H)

    bid_dict = book_data["bid"]
    ask_dict = book_data["ask"]

    rows, cols, styles = book_to_rows(bid_dict, ask_dict)

    return build_table(
        "Order Book (prices in dollars)",
        "tbl-book",
        BOOK_H,
        data=rows,
        columns=cols,
        style_cond=styles
    )


@app.callback(
    Output("tbl-trades", "data"),
    Output("tbl-trades", "columns"),
    Output("fig-price", "figure"),
    Input("store-trades", "data"),
)
def render_trades_and_chart(trades_data):
    rows = []
    xs, ys = [], []

    for t in reversed(trades_data or []):
        ts_str = format_dt(t["timestamp"])
        px    = int(t["price_cents"])
        qty   = int(t["quantity"])
        total = (px * qty) / 100

        if t.get("maker_is_buyer", False):
            buyer  = _parties.get(t["maker_party_id"], str(t["maker_party_id"]))
            seller = _parties.get(t["taker_party_id"], str(t["taker_party_id"]))
        else:
            buyer  = _parties.get(t["taker_party_id"], str(t["taker_party_id"]))
            seller = _parties.get(t["maker_party_id"], str(t["maker_party_id"]))

        rows.append({
            "Time"   : ts_str,
            "Price"  : no_dollar(px),
            "Qty"    : qty,
            "Total"  : f"{total:,.2f}",
            "Buyer"  : buyer,
            "Seller" : seller,
        })
        xs.append(datetime.datetime.fromtimestamp(t["timestamp"] / 1e9))
        ys.append(px / 100)

    trade_cols = [
        {"name": "Time"  , "id": "Time"},
        {"name": "Price" , "id": "Price"},
        {"name": "Qty"   , "id": "Qty"},
        {"name": "Total" , "id": "Total"},
        {"name": "Buyer" , "id": "Buyer"},
        {"name": "Seller", "id": "Seller"},
    ]

    fig = go.Figure()
    if xs and ys:
        fig.add_trace(
            go.Scatter(
                x=xs, y=ys, mode="lines", name="Price",
                line=dict(color=LASTP_TXT)
            )
        )
        if len(ys) >= 20:
            ma = [sum(ys[i - 19:i + 1]) / 20 for i in range(19, len(ys))]
            fig.add_trace(
                go.Scatter(
                    x=xs[19:], y=ma, mode="lines", name="20-MA",
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



def _get_my_open_orders(pid, inst_id):
    try:
        raw = requests.get(f"{API_URL}/live_orders/{inst_id}", timeout=2).json()
    except Exception:
        raw = []
    mine = [r for r in raw if str(r["party_id"]) == str(pid)]
    rows = [
        {
            "OID":   r["order_id"],
            "Side":  r["side"],
            "Price": no_dollar(int(r["price_cents"])),
            "Qty":   int(r["remaining_quantity"]),
        }
        for r in sorted(mine, key=lambda x: x["order_id"], reverse=True)
    ]
    return rows


@app.callback(
    Output("open-orders-table", "children"),
    Input("store-open-raw", "data"),
)
def render_open_table(open_rows):
    if not open_rows:
        return html.Div(
            "No open orders or not fetched yet.",
            style={"color": ORANGE_TXT, "fontSize": "0.75rem"}
        )

    header = html.Tr([
        html.Th("OID",    style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Side",   style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Price",  style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Qty",    style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
        html.Th("Action", style={"color": ORANGE_TXT, "fontWeight": "900", "textAlign": "center", "fontSize": CELL_FONT_SZ}),
    ])

    rows = []
    for r in open_rows:
        oid = r["OID"]
        rows.append(
            html.Tr([
                html.Td(oid,    style={"textAlign": "center", "fontSize": CELL_FONT_SZ, "color": ORANGE_TXT}),
                html.Td(r["Side"],  style={"textAlign": "center", "fontSize": CELL_FONT_SZ, "color": ORANGE_TXT}),
                html.Td(r["Price"], style={"textAlign": "center", "fontSize": CELL_FONT_SZ, "color": ORANGE_TXT}),
                html.Td(r["Qty"],   style={"textAlign": "center", "fontSize": CELL_FONT_SZ, "color": ORANGE_TXT}),
                html.Td(
                    dbc.Button(
                        "Cancel",
                        id={"type": "cancel-open", "index": oid},
                        color="danger", size="sm",
                        style={"height": "2rem", "fontSize": CELL_FONT_SZ, "whiteSpace": "nowrap"},
                        n_clicks=0
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
    if not ctx:
        return dash.no_update
    triggered = ctx[0]
    btn_id = json.loads(triggered["prop_id"].split(".")[0])
    button_n = triggered["value"]

    if not button_n:
        return dash.no_update
    oid_clicked  = btn_id["index"]
    if pid is None or pwd is None:
        return "cancel_open: ⚠ Need Party ID & Password to cancel"
    payload = {
        "instrument_id": inst_id,
        "order_id"     : oid_clicked,
        "party_id"     : pid,
        "password"     : pwd,
    }
    try:
        resp = requests.post(f"{API_URL}/cancel", json=payload, timeout=4).json()
    except Exception as e:
        return f"cancel_open: ❌ Network Error: {e}"
    if resp.get("status") == "CANCELLED":
        return "✓ Order Cancelled"
    else:
        detail = resp.get("details", resp)
        return f"cancel_open: ❌ {detail}"


@app.callback(
    Output("lbl-msg", "children", allow_duplicate=True),
    Input("btn-cancel-all", "n_clicks"),
    State("o_party", "value"),
    State("o_pwd", "value"),
    State("dd-instr", "value"),
    prevent_initial_call=True,
)
def cancel_all(nc, pid, pwd, inst_id):
    if not nc or pid is None or pwd is None:
        return dash.no_update, dash.no_update
    errs = []
    payload = {
        "instrument_id": inst_id,
        "party_id"     : pid,
        "password"     : pwd,
    }
    try:
        resp = requests.post(f"{API_URL}/cancel_all", json=payload, timeout=4).json()
    except Exception as e:
        return f"cancel_all: ❌ Some cancels failed: {'; '.join(errs)}"
    return f"✓ All orders cancelled: {resp}"


@app.callback(
    Output("lbl-msg", "children", allow_duplicate=True),
    Input("btn-buy", "n_clicks"),
    Input("btn-sell", "n_clicks"),
    State("dd-instr", "value"),
    State("in-party", "value"),
    State("in-pwd", "value"),
    State("in-qty", "value"),
    State("in-price", "value"),
    State("in-otyp", "value"),
    prevent_initial_call=True,
)
def send_new_order(n_buy, n_sell, inst_id, pid, pwd, qty, price_txt, otype):
    ctx = callback_context.triggered[0]["prop_id"].split(".")[0]
    side = "BUY" if ctx == "btn-buy" else "SELL"

    if None in (pid, pwd, qty, price_txt, otype):
        return "send_new_order: ⚠ Please fill Party ID, Password, Qty, Price & OrderType"
    try:
        px_cs = to_cents(price_txt)
    except Exception:
        return "send_new_order: ⚠ Bad price format (e.g. 101.23)"
    payload = {
        "instrument_id": inst_id,
        "side"         : side,
        "order_type"   : otype,
        "quantity"     : int(qty),
        "price_cents"  : px_cs,
        "party_id"     : str(pid),
        "password"     : pwd,
    }
    try:
        resp = requests.post(f"{API_URL}/orders", json=payload, timeout=4).json()
    except Exception as e:
        return f"send_new_order: ❌ Network Error: {e}"
    if resp.get("status") == "ACCEPTED":
        return "✓ Order Accepted"
    else:
        detail = resp.get("details", resp)
        return f"send_new_order: ❌ {detail}"


@app.callback(
    Output("tbl-pos", "data"),
    Output("tbl-pos", "columns"),
    Input("store-trades", "data"),
)
def render_positions(trades_data):
    rows = compute_positions(trades_data)
    cols = [
        {"name": "Party"          , "id": "Party"},
        {"name": "Net Qty"        , "id": "NetQty"},
        {"name": "FirstTradeTime" , "id": "FirstTradeTime"},
        {"name": "LastTradeTime"  , "id": "LastTradeTime"},
        {"name": "AveragePrice"   , "id": "AveragePrice"},
        {"name": "NetValue"       , "id": "NetValue"},
    ]
    return rows, cols


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("DASH_PORT", "8888")), debug=True)
