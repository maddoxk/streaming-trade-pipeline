"""End-to-end check against a running compose stack.

1. compute the expected bars from the sample CSVs with pandas (independent code),
2. wait for the Spark job to be running,
3. replay the sample into Kafka,
4. wait until Postgres holds every expected window with every trade counted,
5. compare every row/column to the pandas reference and record throughput/latency.

    docker compose up -d --build --wait   # (spark has no healthcheck; script waits on its log)
    python scripts/e2e.py --speedup 0 --label burst --results results/ci_burst.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

from pipeline.producer import make_producer, run
from pipeline.reference import BAR_COLUMNS, closed_windows, diff_bars, load_csvs, reference_bars

ROOT = Path(__file__).resolve().parent.parent
WATERMARK_MS = 30_000  # must match --watermark in docker-compose.yml
DEFAULT_DSN = "postgresql://pipeline:pipeline@localhost:5432/pipeline"


def wait_for_spark(timeout_s: float) -> float:
    """Block until the job logged STREAM_STARTED; return seconds waited."""
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        out = subprocess.run(
            ["docker", "compose", "logs", "spark"], capture_output=True, text=True, cwd=ROOT
        )
        if "STREAM_STARTED" in out.stdout:
            return time.time() - t0
        time.sleep(2)
    raise TimeoutError("Spark job did not report STREAM_STARTED")


def pg_progress(dsn: str) -> tuple[int, int]:
    try:
        with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), coalesce(sum(trade_count), 0) FROM ohlc_1m")
            n, trades = cur.fetchone()
            return int(n), int(trades)
    except psycopg2.Error:
        return 0, 0  # table not created yet


def fetch_actual(dsn: str) -> tuple[pd.DataFrame, list[float]]:
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT symbol, extract(epoch FROM window_start)::bigint, open, high, low, close, "
            "vwap, volume, trade_count, realized_vol, "
            "extract(epoch FROM (updated_at - last_ingest_ts)) * 1000 FROM ohlc_1m"
        )
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=[*BAR_COLUMNS, "latency_ms"])
    return df[BAR_COLUMNS], df["latency_ms"].astype(float).tolist()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dsn", default=DEFAULT_DSN)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="trades")
    p.add_argument("--speedup", type=float, default=0.0)
    p.add_argument("--max-disorder-ms", type=int, default=5000)
    p.add_argument("--label", default="burst")
    p.add_argument("--timeout", type=float, default=300)
    p.add_argument("--results", default=None)
    a = p.parse_args()

    files = sorted(str(f) for f in (ROOT / "data/sample").glob("*.csv"))
    trades = load_csvs(files)
    all_bars = reference_bars(trades)
    # Append mode only emits windows the watermark has closed (see docs/DESIGN.md).
    expected = closed_windows(all_bars, int(trades["trade_time_ms"].max()), WATERMARK_MS)
    total = int(expected["trade_count"].sum())
    n_sent = len(trades)
    print(
        f"{n_sent} trades -> {len(all_bars)} windows; {len(expected)} closed by the watermark "
        f"({total} trades) are expected in Postgres"
    )

    startup_s = wait_for_spark(600)
    print(f"spark ready after {startup_s:.0f}s of waiting")

    t_start = time.time()
    stats = run(
        files,
        a.topic,
        make_producer(a.bootstrap),
        speedup=a.speedup,
        max_disorder_ms=a.max_disorder_ms,
        seed=1,
    )
    print(f"produced {stats.sent} trades in {stats.elapsed_s:.2f}s ({stats.rate_per_s:,.0f}/s)")
    if stats.delivery_errors or stats.sent != n_sent:
        print("producer problem", stats, file=sys.stderr)
        return 1

    while True:
        n, got = pg_progress(a.dsn)
        if n == len(expected) and got == total:
            t_done = time.time()
            break
        if time.time() - t_start > a.timeout:
            print(
                f"TIMEOUT: have {n}/{len(expected)} windows, {got}/{total} trades", file=sys.stderr
            )
            return 1
        time.sleep(0.25)

    actual, latencies = fetch_actual(a.dsn)
    problems = diff_bars(expected, actual)
    lat = pd.Series(latencies)
    elapsed = t_done - t_start
    result = {
        "label": a.label,
        "passed": not problems,
        "mismatches": problems[:20],
        "trades_replayed": n_sent,
        "trades_in_closed_windows": total,
        "windows_total": len(all_bars),
        "windows_closed_and_checked": len(expected),
        "speedup": a.speedup,
        "max_disorder_ms": a.max_disorder_ms,
        "producer_publish_rate_per_s": round(stats.rate_per_s, 1),
        "end_to_end_seconds": round(elapsed, 2),
        "end_to_end_trades_per_s": round(n_sent / elapsed, 1),
        "event_time_span_s": int(
            (trades["trade_time_ms"].max() - trades["trade_time_ms"].min()) / 1000
        ),
        "result_latency_ms": {
            "definition": "Postgres updated_at of the final upsert minus Kafka timestamp of "
            "the newest trade in that window",
            "p50": round(float(lat.quantile(0.5)), 1),
            "p95": round(float(lat.quantile(0.95)), 1),
            "max": round(float(lat.max()), 1),
        },
        "environment": {
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "github_runner_os": os.environ.get("RUNNER_OS"),
            "github_image": os.environ.get("ImageVersion"),
            "kafka": "apache/kafka:3.8.0 (single broker, KRaft)",
            "spark": "pyspark 3.5.3, local[2], 1 s trigger, shuffle partitions 2",
            "postgres": "postgres:16",
        },
    }
    print(json.dumps(result, indent=2))
    if a.results:
        Path(a.results).parent.mkdir(parents=True, exist_ok=True)
        Path(a.results).write_text(json.dumps(result, indent=2) + "\n")
    return 0 if not problems else 1


if __name__ == "__main__":
    raise SystemExit(main())
