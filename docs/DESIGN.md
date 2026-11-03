# Design notes

Read this before an interview. It explains what each piece does, why it is that way, and where the
guarantees stop.

## 1. Data flow

```
Binance CSV --> replay.py --> producer.py --> Kafka topic `trades` --> spark_job.py --> Postgres `ohlc_1m`
(real trades)  merge+disorder  idempotent     key = symbol            event-time windows   upsert by
               +pacing         producer       2 partitions            + watermark          (symbol, window_start)
```

- **Source**: real historical aggTrades from Binance's public dumps (2024-06-03 14:30-14:45 UTC, BTCUSDT
  and ETHUSDT). It is a *replay*, not a live feed: the data is real, the arrival timing is simulated.
- **Event time vs processing time**: every trade carries its own exchange timestamp (`trade_time_ms`).
  Windows are defined on that, so replaying 15 minutes in 2 seconds gives the same bars as real time.
- **Message format**: JSON, validated against `schemas/trade.schema.json` (and the equivalent Python
  rules in `trade.py`). The producer rejects bad rows; the Spark job drops invalid records again
  (never trust the topic). No Avro/schema registry: more moving parts than the project needs.

## 2. Producer (`replay.py`, `producer.py`)

- Files are merged by event time (`heapq.merge`). This matters because Spark's watermark is **global**:
  if all BTC trades were published before any ETH trade, the ETH trades would look 15 minutes late.
- `--speedup N` sleeps so that event time advances N times faster than wall time (0 = no sleeping).
- `--max-disorder-ms D` adds a random delay in [0, D) to each trade and re-sorts by (time + delay), so
  no trade is more than D ms behind the newest one already sent. This produces *bounded out-of-order
  arrival* to exercise the watermark; the e2e job uses 5 s.
- Key = symbol, so one symbol's trades stay in one partition, in order. Idempotent producer
  (`enable.idempotence`, `acks=all`) stops broker-side duplicates from internal retries.

## 3. Windows and metrics (`transforms.py`)

Tumbling 1-minute event-time windows per symbol:

| metric | definition |
|---|---|
| open / close | price of the first / last trade ordered by (time, trade id) |
| high / low | max / min price |
| volume | sum of quantity |
| VWAP | sum(price x quantity) / sum(quantity) |
| trade count | number of aggregated trades (Binance "aggTrades", not individual fills) |
| realized volatility | sqrt of the sum of squared log returns between consecutive trades in the window; **not** annualized; 0 for a single-trade window |

**Why `collect_list`?** Open, close and realized volatility depend on trade *order*. Spark's
`first()/last()` are not deterministic in streaming aggregations, and a second aggregation over the
first is not allowed in append mode. So each window keeps its ticks (time, id, price, quantity) in a
list, sorted once when the window is read; `zip_with` pairs each tick with its predecessor to get the
returns. Cost: state is O(trades per window). For two symbols and about 2k trades/minute that is
tiny; for hundreds of symbols or far busier markets it would need a different design (e.g. per-second
sub-bars as inputs to a second job, or approximate volatility).

Correctness is checked against `reference.py`, an independent pandas implementation that shares no code
with the Spark path.

## 4. Watermarks and late data

A watermark is Spark's rule for "how long do I wait for stragglers": `watermark = newest event time seen
- delay`. A window is finalized (and its state freed) when the watermark passes its end; rows for
windows already finalized are dropped.

**Choice: 30 seconds, append mode.**

- Delay: the injected disorder in e2e is 5 s, so 30 s leaves a wide margin (and the e2e result is
  exactly the reference, i.e. nothing legitimate was dropped). For a live exchange feed, real lateness is
  usually small, but I have **not measured it**; 30 s is a judgment call, not derived from data. Larger
  delay = more complete bars, but each bar is published 30 s of event time after its minute ends and
  state is held longer. Smaller delay = fresher bars, more dropped stragglers.
- **Append vs update mode**: update mode emits a window every time it changes (early partial results)
  but Spark only drops late rows on a best-effort basis there. The unit test
  `test_late_data_behaviour_with_watermark` caught this: in update mode a trade for a window that was long
  closed was still aggregated. Append mode emits each window exactly once, after it closes, and drops late
  rows deterministically. The price: no partial results, and the windows still open when a finite replay
  ends (any window whose end is less than 30 s of event time before the newest trade) are never
  emitted; for the sample that is the last minute, 14:44, of each symbol. The e2e check therefore compares
  only the windows the watermark has closed (`reference.closed_windows`).
- **Subtle semantics, verified in CI**: late rows in batch N are judged against the watermark that was in
  force when batch N *started* (the one computed from batch N-1), so the test must put the late row in a
  later micro-batch than the one that advances the watermark.
- **Visibility**: the job logs `WATERMARK_DROPPED n` from the query progress
  (`numRowsDroppedByWatermark`). Dropped rows are *not* stored anywhere (no side output) - see Limitations.
- **Multi-partition caveat**: the watermark is the max over everything one micro-batch read, across
  partitions and symbols. If one partition were read much further ahead in event time than another, the
  slower one's data could be dropped as late. The replayer avoids this by publishing in event-time order;
  with independent real-time producers per symbol this is the first thing to check.

## 5. Delivery semantics: what is and is not exactly-once

| hop | guarantee | how |
|---|---|---|
| replayer -> Kafka | no duplicates from producer retries, **within one run** | idempotent producer. Running the replayer twice publishes every trade twice; there are no transactions and no dedupe key across runs |
| Kafka -> Spark | at-least-once processing with replayable batches | offsets and state are checkpointed; after a crash Spark re-runs the failed micro-batch over the same offset range |
| Spark -> Postgres | at-least-once writes, **idempotent**, so the table converges to the same content | `foreachBatch` may run again for the same batch (crash between the write and the checkpoint commit); the upsert keyed by `(symbol, window_start)` makes that harmless |

So the *result table* behaves as if each window were written once, provided (a) the input topic has each
trade once and (b) re-executed batches produce the same rows (true here: append output of a window is a
pure function of the state and the offsets). It is "effectively-once via idempotence", not Kafka
transactions end to end. Things I did **not** do or test: killing the Spark container mid-run and
checking recovery, deduplicating trades by `(symbol, trade_id)` inside Spark (`dropDuplicates` with the
watermark would do it), and transactional producer/consumer.

## 6. Sink (`sink.py`)

`INSERT ... ON CONFLICT (symbol, window_start) DO UPDATE` in one transaction per micro-batch. The
batch is collected to the driver: in append mode a batch only contains the windows the watermark just
closed (a few rows), so this avoids JDBC jar/config complexity. It would not be appropriate for a
batch with millions of rows; use `df.write.jdbc` into a staging table plus a `MERGE`/upsert statement
instead. Two bookkeeping columns exist for measurement: `last_ingest_ts` (Kafka timestamp of the newest
trade in the window) and `updated_at` (time of the upsert); their difference is the "result latency"
in the README.

## 7. Spark configuration notes

- `spark.sql.session.timeZone=UTC` so window boundaries and timestamps are unambiguous.
- `spark.sql.shuffle.partitions=2`: the default of 200 would create 200 state-store shards for a
  handful of keys and dominate the run time.
- Local mode (`local[2]`) in one container. The code is cluster-agnostic (nothing depends on local
  mode), but I have not run it on a cluster.

## 8. Testing strategy

| layer | what | where |
|---|---|---|
| parsing/validation | good/bad rows, schema agreement | `tests/test_trade.py` |
| replay mechanics | merge order, bounded disorder, pacing with a fake clock | `tests/test_replay.py` |
| producer | keys, JSON, retry on full queue, delivery errors (fake producer) | `tests/test_producer.py` |
| transforms | Spark vs pandas reference on 54,890 real trades; unsorted input; invalid records; late data | `tests/test_transforms_spark.py` |
| sink | idempotent upsert, overwrite, report (real Postgres service in CI) | `tests/test_sink_pg.py` |
| whole system | compose stack, replay with disorder, every closed window compared to pandas | `scripts/e2e.py` in CI |
