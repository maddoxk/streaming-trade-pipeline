"""Small query/report script over the Postgres sink.

python -m pipeline.report --dsn postgresql://pipeline:pipeline@localhost:5432/pipeline
"""

from __future__ import annotations

import argparse

SUMMARY_SQL = """
SELECT symbol, count(*) AS windows, sum(trade_count) AS trades,
       min(window_start) AS first_window, max(window_start) AS last_window
FROM ohlc_1m GROUP BY symbol ORDER BY symbol
"""

RECENT_SQL = """
SELECT symbol, to_char(window_start AT TIME ZONE 'UTC', 'HH24:MI') AS utc_min,
       open, high, low, close, round(vwap::numeric, 2) AS vwap,
       round(volume::numeric, 3) AS volume, trade_count,
       round((realized_vol * 10000)::numeric, 2) AS vol_bps
FROM ohlc_1m WHERE symbol = %s ORDER BY window_start DESC LIMIT %s
"""

LATENCY_SQL = """
SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY lat) AS p50_ms,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY lat) AS p95_ms,
       max(lat) AS max_ms
FROM (SELECT extract(epoch FROM (updated_at - last_ingest_ts)) * 1000 AS lat FROM ohlc_1m) s
"""


def _print_table(title: str, cols: list[str], rows: list[tuple]) -> None:
    print(f"\n== {title}")
    widths = [
        max(len(c), *(len(str(r[i])) for r in rows)) if rows else len(c) for i, c in enumerate(cols)
    ]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
    for r in rows:
        print("  ".join(str(v).ljust(w) for v, w in zip(r, widths, strict=True)))


def main(argv: list[str] | None = None) -> None:
    import psycopg2

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", required=True)
    p.add_argument("--recent", type=int, default=5, help="latest N windows per symbol")
    a = p.parse_args(argv)

    with psycopg2.connect(a.dsn) as conn, conn.cursor() as cur:
        cur.execute(SUMMARY_SQL)
        summary = cur.fetchall()
        _print_table(
            "per-symbol summary",
            ["symbol", "windows", "trades", "first_window", "last_window"],
            summary,
        )
        for symbol, *_ in summary:
            cur.execute(RECENT_SQL, (symbol, a.recent))
            _print_table(
                f"latest {a.recent} 1-min bars: {symbol} (vol_bps = realized vol x 1e4)",
                [
                    "symbol",
                    "utc_min",
                    "open",
                    "high",
                    "low",
                    "close",
                    "vwap",
                    "volume",
                    "trades",
                    "vol_bps",
                ],
                cur.fetchall(),
            )
        cur.execute(LATENCY_SQL)
        _print_table(
            "result latency: Kafka timestamp of newest trade -> final upsert (ms)",
            ["p50", "p95", "max"],
            [tuple(round(float(x), 1) if x is not None else None for x in cur.fetchone())],
        )


if __name__ == "__main__":
    main()
