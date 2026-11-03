# streaming-trade-pipeline

A small, real streaming pipeline: **Kafka (KRaft) -> Spark Structured Streaming -> Postgres**, computing
per-symbol 1-minute bars (OHLC, VWAP, volume, trade count, realized volatility) from real Binance
trade data. Every piece is covered by unit tests, and CI brings up the whole stack and checks the
Postgres output against an independent pandas calculation.

```mermaid
flowchart LR
    CSV["Binance aggTrades CSV<br/>(data/sample, real)"] --> R["replayer<br/>merge by time, bounded disorder,<br/>speed-up, validation"]
    R -->|"key = symbol<br/>JSON"| K[("Kafka topic trades<br/>apache/kafka, KRaft")]
    K --> S["Spark Structured Streaming (local mode)<br/>parse + validate -> 1-min event-time windows<br/>watermark 30 s, append mode"]
    S -->|"foreachBatch<br/>INSERT ... ON CONFLICT"| P[("Postgres ohlc_1m<br/>PK (symbol, window_start)")]
    P --> Q["report.py"]
    P -.->|"compared in CI"| REF["pandas reference"]
```

## What is real and what is simulated

- **Real**: the trades (Binance public data, 2024-06-03 14:30-14:45 UTC, BTCUSDT + ETHUSDT, 54,890 trades), Kafka,
  Spark, Postgres.
- **Simulated**: arrival timing. It is a *replay* of historical data (optionally sped up, with injected
  out-of-order delivery); there is no live market connection.

## Quick start

Needs Docker (compose v2) and Python 3.10+.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"                 # producer, reference, tests
docker compose up -d --build            # Kafka, Postgres, Spark job (first build downloads ~1 GB)
python -m pipeline.producer --files data/sample/BTCUSDT.csv data/sample/ETHUSDT.csv \
    --speedup 30 --max-disorder-ms 5000
python -m pipeline.report --dsn postgresql://pipeline:pipeline@localhost:5432/pipeline
docker compose down -v
```

Try the producer without Kafka: `python -m pipeline.producer --files data/sample/*.csv --dry-run --limit 5`.
Tests: `pip install -e ".[dev,spark]"` (Spark tests need Java 17), then `pytest`. Postgres tests run when
`PG_DSN` is set. The compose credentials are throwaway local values.

Note: with the sample, the final ~1.5 minutes of event time are not emitted (windows still open when the
data ends, see DESIGN.md section 4); 28 of 30 windows appear in Postgres.

## Measured results

From the CI `e2e` job (GitHub Actions `ubuntu-latest`, 2 vCPU, single Kafka broker, Spark `local[2]`, one run;
raw JSON in [`results/`](results/), commands in [`results/README.md`](results/README.md)). In both runs all 28
closed windows (53,938 trades) matched the pandas reference.

| scenario | trades replayed | producer publish rate | end-to-end time | end-to-end rate | result latency p50 / p95 |
|---|---|---|---|---|---|
| burst (speedup 0, 5 s disorder) | 54,890 | 10,215 /s | 16.5 s | 3,331 trades/s | 12.4 s / 15.8 s |
| paced 30x real time (5 s disorder) | 54,890 | 1,829 /s | 32.3 s | 1,702 trades/s | 4.4 s / 12.3 s |

- *End-to-end time*: from the start of the replay until Postgres holds every expected window, so it includes
  the producer, Kafka, micro-batch scheduling and upserts (not Spark start-up). In the burst run the
  whole input is consumed in roughly one micro-batch, so the rate reflects one large batch, not steady state.
- *Result latency*: Postgres `updated_at` of the window's final upsert minus the Kafka timestamp of the
  newest trade in that window. In append mode a bar cannot appear before the watermark passes its end, so
  the 30 s event-time watermark delay is part of it (about 1 s of wall time at 30x). I did not break the
  rest down further, so treat these as indicative of this small setup, not as a benchmark.

## Layout

| path | what |
|---|---|
| `src/pipeline/trade.py`, `replay.py`, `producer.py` | validation, replay generators, Kafka producer |
| `src/pipeline/transforms.py`, `spark_job.py` | windowed aggregation, streaming job |
| `src/pipeline/sink.py`, `report.py` | Postgres schema/upsert, query script |
| `src/pipeline/reference.py` | independent pandas implementation used as ground truth |
| `scripts/download_sample.py`, `scripts/e2e.py` | fetch data; end-to-end check |
| `docs/DESIGN.md` | architecture, watermark choice, delivery semantics |

## Limitations

- **Replay, not live**; one 15-minute, two-symbol sample. No claim about behaviour at other scales.
- Spark runs in **local mode**, Kafka is a **single broker** with two partitions; no cluster, security or
  monitoring. Numbers are from a shared 2-vCPU CI runner and a single run each.
- **Not end-to-end exactly-once**: idempotent producer within a run, at-least-once Spark with an
  idempotent sink. Re-running the replayer duplicates trades in Kafka and Spark does not de-duplicate them.
  Crash recovery (killing Spark mid-run) is **not tested**.
- Invalid records and watermark-dropped late rows are counted/logged but **not dead-lettered** or stored.
- Windows still open when a finite replay ends are never emitted (append mode); the 30 s watermark is a
  chosen constant, not measured from a live feed.
- Realized volatility is a tick-return estimate over one minute, not annualized, and affected by
  microstructure noise. Each window holds its ticks in memory (see DESIGN.md).
- JSON messages only (no Avro / schema registry). The Postgres write collects each batch to the driver.

## Data and license

Code: MIT. Trade data: Binance public data dumps; see [`data/README.md`](data/README.md) for source and terms.
