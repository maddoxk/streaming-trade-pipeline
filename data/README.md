# Sample data

`sample/BTCUSDT.csv` and `sample/ETHUSDT.csv` are real Binance spot **aggregated trades**
(`aggTrades`), 15 minutes each (2024-06-03 14:30-14:45 UTC), 31,742 + 23,148 trades, 3.6 MB total.

## Source and terms

- Downloaded from Binance's public data dumps, `https://data.binance.vision/data/spot/daily/aggTrades/`
  (no API key or account). Dump format is documented in the
  [binance-public-data](https://github.com/binance/binance-public-data) repository, which is MIT licensed.
- The market data itself is Binance's; I did not find a separate data license on the dumps site,
  so treat the files as Binance data subject to Binance's terms of use. Only a 15-minute slice is
  committed, for demonstration/testing. If you reuse this repo, re-download with the script below
  rather than redistributing the slice.

## Format

Normalized by `scripts/download_sample.py` (raw dumps have no header; since 2025 they use
microsecond timestamps, which the script converts):

| column | meaning |
|---|---|
| `symbol` | e.g. `BTCUSDT` |
| `agg_trade_id` | Binance aggregate trade id (unique per symbol) |
| `price`, `quantity` | decimal strings from Binance |
| `transact_time_ms` | trade (event) time, ms since epoch UTC |
| `is_buyer_maker` | `true` if the buyer was the maker (i.e. the aggressor sold) |

## Regenerate

```bash
python scripts/download_sample.py --symbol BTCUSDT --date 2024-06-03 --start 14:30 --minutes 15 --out data/sample/BTCUSDT.csv
python scripts/download_sample.py --symbol ETHUSDT --date 2024-06-03 --start 14:30 --minutes 15 --out data/sample/ETHUSDT.csv
```
(each downloads one ~13 MB daily zip).
