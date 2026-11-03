"""Pure-Python building blocks of the replayer (no Kafka needed, easy to unit test).

Pipeline of generators:  read_trades -> merge_sorted -> apply_disorder -> paced
"""

from __future__ import annotations

import csv
import heapq
import random
import time
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

from pipeline.trade import Trade, ValidationError, parse_row


@dataclass
class ReadStats:
    valid: int = 0
    invalid: int = 0


def read_trades(path: str, stats: ReadStats | None = None) -> Iterator[Trade]:
    """Yield validated trades from a normalized CSV; skip (and count) bad rows."""
    stats = stats if stats is not None else ReadStats()
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                trade = parse_row(row)
            except ValidationError:
                stats.invalid += 1
                continue
            stats.valid += 1
            yield trade


def merge_sorted(streams: Iterable[Iterable[Trade]]) -> Iterator[Trade]:
    """Merge per-symbol files (each already time-ordered) into one time-ordered stream.

    Interleaving by event time matters: Spark's watermark is global across
    Kafka partitions, so feeding all of BTC first and then all of ETH would
    make the ETH data look hopelessly late.
    """
    return heapq.merge(*streams, key=lambda t: t.sort_key)


def apply_disorder(trades: Iterable[Trade], max_disorder_ms: int, seed: int = 0) -> Iterator[Trade]:
    """Re-emit a time-ordered stream with bounded out-of-orderness.

    Each trade gets a random delay in [0, max_disorder_ms) and is emitted in
    order of (event time + delay). Any trade is therefore emitted at most
    max_disorder_ms "late" relative to the newest trade already emitted, which
    is exactly the situation a watermark delay is meant to absorb.
    """
    if max_disorder_ms <= 0:
        yield from trades
        return
    rng = random.Random(seed)
    heap: list[tuple[float, int, Trade]] = []
    for seq, trade in enumerate(trades):
        heapq.heappush(heap, (trade.trade_time_ms + rng.random() * max_disorder_ms, seq, trade))
        # Input is time-ordered, so every future trade has emit key >= this
        # trade's event time; anything with a smaller key is safe to release.
        while heap and heap[0][0] <= trade.trade_time_ms:
            yield heapq.heappop(heap)[2]
    while heap:
        yield heapq.heappop(heap)[2]


def paced(
    trades: Iterable[Trade],
    speedup: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[Trade]:
    """Yield trades no faster than `speedup` x real time. speedup <= 0 means no pacing."""
    if speedup <= 0:
        yield from trades
        return
    wall_start = clock()
    first_ts: int | None = None
    newest_ts = 0
    for trade in trades:
        if first_ts is None:
            first_ts = newest_ts = trade.trade_time_ms
        # Pace on the newest event time seen so far so that injected disorder
        # never makes the schedule run backwards.
        newest_ts = max(newest_ts, trade.trade_time_ms)
        due = wall_start + (newest_ts - first_ts) / 1000.0 / speedup
        delay = due - clock()
        if delay > 0:
            sleep(delay)
        yield trade
