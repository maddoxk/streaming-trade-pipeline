"""Kafka producer: replays trade CSVs into a topic, keyed by symbol.

    python -m pipeline.producer --files data/sample/BTCUSDT.csv data/sample/ETHUSDT.csv \
        --bootstrap localhost:9092 --topic trades --speedup 60
    python -m pipeline.producer --files data/sample/*.csv --dry-run --limit 5
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
import time
from dataclasses import asdict, dataclass

from pipeline.replay import ReadStats, apply_disorder, merge_sorted, paced, read_trades


@dataclass
class ReplayStats:
    sent: int = 0
    invalid_rows: int = 0
    delivery_errors: int = 0
    elapsed_s: float = 0.0

    @property
    def rate_per_s(self) -> float:
        return self.sent / self.elapsed_s if self.elapsed_s > 0 else 0.0


def make_producer(bootstrap: str):
    """Create a confluent-kafka producer (imported lazily so unit tests need no librdkafka)."""
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            # Idempotent producer: broker de-duplicates retried batches per
            # partition, so a network retry cannot create duplicate records.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


def run(
    files: list[str],
    topic: str,
    producer=None,
    speedup: float = 0.0,
    max_disorder_ms: int = 0,
    seed: int = 0,
    limit: int | None = None,
    dry_run: bool = False,
) -> ReplayStats:
    """Replay `files`; `producer` is injectable so tests can pass a fake."""
    read_stats = ReadStats()
    stream = merge_sorted(read_trades(f, read_stats) for f in files)
    stream = apply_disorder(stream, max_disorder_ms, seed)
    stream = paced(stream, speedup)
    if limit is not None:
        stream = itertools.islice(stream, limit)

    stats = ReplayStats()
    errors: list[object] = []

    def on_delivery(err, _msg) -> None:
        if err is not None:
            errors.append(err)

    started = time.monotonic()
    for trade in stream:
        payload = trade.to_json()
        if dry_run:
            print(f"{trade.symbol}\t{payload}")
        else:
            while True:
                try:
                    # key = symbol -> all trades of a symbol go to one partition,
                    # preserving their relative order within that partition.
                    producer.produce(
                        topic,
                        key=trade.symbol.encode(),
                        value=payload.encode(),
                        on_delivery=on_delivery,
                    )
                    break
                except BufferError:  # local queue full: serve callbacks and retry
                    producer.poll(0.5)
            producer.poll(0)  # serve delivery callbacks
        stats.sent += 1
    if producer is not None and not dry_run:
        producer.flush()
    stats.elapsed_s = time.monotonic() - started
    stats.invalid_rows = read_stats.invalid
    stats.delivery_errors = len(errors)
    return stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Replay Binance aggTrades CSVs into Kafka")
    p.add_argument("--files", nargs="+", required=True)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="trades")
    p.add_argument(
        "--speedup", type=float, default=0.0, help="x real time; 0 = as fast as possible"
    )
    p.add_argument("--max-disorder-ms", type=int, default=0, help="inject bounded out-of-order")
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--dry-run", action="store_true", help="print messages, do not use Kafka")
    p.add_argument("--stats-out", help="write replay stats as JSON to this path")
    a = p.parse_args(argv)

    producer = None if a.dry_run else make_producer(a.bootstrap)
    stats = run(
        a.files, a.topic, producer, a.speedup, a.max_disorder_ms, a.seed, a.limit, a.dry_run
    )
    out = {**asdict(stats), "rate_per_s": round(stats.rate_per_s, 1)}
    print(json.dumps(out), file=sys.stderr)
    if a.stats_out:
        with open(a.stats_out, "w") as f:
            json.dump(out, f)
    return 1 if stats.delivery_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
