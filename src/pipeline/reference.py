"""Independent pandas implementation of the same bar metrics.

Used (1) by Spark unit tests and (2) by the end-to-end check as ground truth.
It deliberately shares no code with the Spark job.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

WINDOW_S = 60
BAR_COLUMNS = [
    "symbol",
    "window_start_s",
    "open",
    "high",
    "low",
    "close",
    "vwap",
    "volume",
    "trade_count",
    "realized_vol",
]
_FLOAT_COLUMNS = ["open", "high", "low", "close", "vwap", "volume", "realized_vol"]


def load_csvs(files: list[str]) -> pd.DataFrame:
    frames = [pd.read_csv(f) for f in files]
    return pd.concat(frames, ignore_index=True).rename(
        columns={"agg_trade_id": "trade_id", "transact_time_ms": "trade_time_ms"}
    )


def reference_bars(trades: pd.DataFrame) -> pd.DataFrame:
    """1-minute bars per symbol. `trades` needs symbol, trade_id, price, quantity, trade_time_ms."""
    df = trades.copy()
    df["window_start_s"] = (df["trade_time_ms"] // 1000 // WINDOW_S) * WINDOW_S
    df = df.sort_values(["symbol", "window_start_s", "trade_time_ms", "trade_id"])
    keys = ["symbol", "window_start_s"]
    g = df.groupby(keys, sort=True)

    # Squared log return between consecutive trades *inside the same window*.
    prev_price = g["price"].shift(1)
    df["sq_ret"] = np.where(prev_price.notna(), np.log(df["price"] / prev_price) ** 2, 0.0)
    df["notional"] = df["price"] * df["quantity"]
    g = df.groupby(keys, sort=True)

    out = g.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("quantity", "sum"),
        notional=("notional", "sum"),
        trade_count=("price", "size"),
        sq=("sq_ret", "sum"),
    ).reset_index()
    out["vwap"] = out["notional"] / out["volume"]
    out["realized_vol"] = np.sqrt(out["sq"])
    out["trade_count"] = out["trade_count"].astype("int64")
    return out[BAR_COLUMNS]


def diff_bars(
    expected: pd.DataFrame, actual: pd.DataFrame, rel_tol: float = 1e-9, abs_tol: float = 1e-12
) -> list[str]:
    """Human-readable mismatches between two bar tables; empty list means they agree."""
    keys = ["symbol", "window_start_s"]
    e = expected.set_index(keys).sort_index()
    a = actual.set_index(keys).sort_index()
    problems: list[str] = []
    for k in e.index.difference(a.index):
        problems.append(f"missing window {k}")
    for k in a.index.difference(e.index):
        problems.append(f"unexpected window {k}")
    both = e.index.intersection(a.index)
    for k in both:
        er, ar = e.loc[k], a.loc[k]
        if int(er["trade_count"]) != int(ar["trade_count"]):
            problems.append(f"{k}: trade_count {ar['trade_count']} != {er['trade_count']}")
        for col in _FLOAT_COLUMNS:
            x, y = float(ar[col]), float(er[col])
            if abs(x - y) > max(abs_tol, rel_tol * abs(y)):
                problems.append(f"{k}: {col} {x!r} != {y!r}")
    return problems


def closed_windows(bars: pd.DataFrame, max_event_time_ms: int, watermark_ms: int) -> pd.DataFrame:
    """Bars that an append-mode job with this watermark has emitted once all data is consumed.

    Spark emits a window when watermark >= window end, where watermark = newest
    event time seen (across all symbols and partitions) minus the allowed lateness.
    Windows still open at the end of a finite replay are never emitted.
    """
    watermark_ms_now = max_event_time_ms - watermark_ms
    closed = (bars["window_start_s"] + WINDOW_S) * 1000 <= watermark_ms_now
    return bars[closed].reset_index(drop=True)
