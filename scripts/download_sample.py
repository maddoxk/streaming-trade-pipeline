"""Download a slice of Binance public aggTrades data and write a normalized CSV.

Source: https://data.binance.vision (public dumps, no API key). Binance
publishes one zip per symbol per day; we stream the zip into memory, keep only
the trades inside [start, start + minutes) UTC and write them in the
normalized format the rest of this repo reads:

    symbol,agg_trade_id,price,quantity,transact_time_ms,is_buyer_maker

Raw spot files have no header and (since 2025) microsecond timestamps; older
files use milliseconds. We detect the unit and always write milliseconds.

Example:
    python scripts/download_sample.py --symbol BTCUSDT --date 2024-06-03 \
        --start 14:30 --minutes 20 --out data/sample/BTCUSDT.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import urllib.request
import zipfile
from datetime import datetime, timezone

URL = "https://data.binance.vision/data/spot/daily/aggTrades/{sym}/{sym}-aggTrades-{date}.zip"
HEADER = ["symbol", "agg_trade_id", "price", "quantity", "transact_time_ms", "is_buyer_maker"]


def to_ms(raw_ts: int) -> int:
    """Binance spot switched from ms to us timestamps in 2025; normalize to ms."""
    return raw_ts // 1000 if raw_ts > 10**14 else raw_ts


def main() -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--symbol", required=True)
    p.add_argument("--date", required=True, help="UTC day, YYYY-MM-DD")
    p.add_argument("--start", default="14:30", help="UTC start time HH:MM")
    p.add_argument("--minutes", type=int, default=20)
    p.add_argument("--out", required=True)
    a = p.parse_args()

    day = datetime.strptime(a.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    hh, mm = (int(x) for x in a.start.split(":"))
    start_ms = int(day.replace(hour=hh, minute=mm).timestamp() * 1000)
    end_ms = start_ms + a.minutes * 60_000

    url = URL.format(sym=a.symbol, date=a.date)
    print(f"downloading {url}")
    with urllib.request.urlopen(url) as resp:  # noqa: S310 (fixed https host)
        blob = resp.read()
    zf = zipfile.ZipFile(io.BytesIO(blob))
    name = zf.namelist()[0]

    kept = 0
    with zf.open(name) as raw, open(a.out, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(HEADER)
        for row in csv.reader(io.TextIOWrapper(raw)):
            if not row or not row[0].isdigit():  # skip a header if present
                continue
            ts = to_ms(int(row[5]))
            if start_ms <= ts < end_ms:
                # raw columns: id, price, qty, first_id, last_id, time, is_buyer_maker, ...
                w.writerow([a.symbol, row[0], row[1], row[2], ts, row[6].lower()])
                kept += 1
    print(f"wrote {kept} trades to {a.out}")


if __name__ == "__main__":
    main()
