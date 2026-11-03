import csv

from pipeline.replay import ReadStats, apply_disorder, merge_sorted, paced, read_trades
from pipeline.trade import Trade


def mk(i: int, t: int, sym: str = "BTCUSDT") -> Trade:
    return Trade(sym, i, 100.0 + i, 1.0, 1_700_000_000_000 + t, False)


def test_read_trades_skips_and_counts_invalid(tmp_path):
    p = tmp_path / "t.csv"
    with open(p, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            ["symbol", "agg_trade_id", "price", "quantity", "transact_time_ms", "is_buyer_maker"]
        )
        w.writerow(["BTCUSDT", 1, "10", "1", 1_700_000_000_000, "false"])
        w.writerow(["BTCUSDT", 2, "-1", "1", 1_700_000_000_001, "false"])  # bad price
        w.writerow(["BTCUSDT", 3, "11", "1", 1_700_000_000_002, "true"])
    stats = ReadStats()
    trades = list(read_trades(str(p), stats))
    assert [t.trade_id for t in trades] == [1, 3]
    assert (stats.valid, stats.invalid) == (2, 1)


def test_read_sample_files_are_clean_and_sorted(sample_files):
    for path in sample_files:
        stats = ReadStats()
        times = [t.trade_time_ms for t in read_trades(path, stats)]
        assert stats.invalid == 0 and stats.valid > 1000
        assert times == sorted(times)


def test_merge_sorted_interleaves_by_time():
    btc = [mk(1, 0), mk(2, 30), mk(3, 60)]
    eth = [mk(1, 10, "ETHUSDT"), mk(2, 40, "ETHUSDT")]
    merged = list(merge_sorted([btc, eth]))
    assert [t.trade_time_ms - 1_700_000_000_000 for t in merged] == [0, 10, 30, 40, 60]


def test_disorder_is_permutation_with_bounded_lateness():
    base = [mk(i, i * 7) for i in range(2000)]
    out = list(apply_disorder(base, max_disorder_ms=500, seed=3))
    assert sorted(t.trade_id for t in out) == list(range(2000))
    assert [t.trade_id for t in out] != list(range(2000))  # actually reordered
    newest = 0
    for t in out:
        assert newest - t.trade_time_ms < 500
        newest = max(newest, t.trade_time_ms)


def test_disorder_is_deterministic_and_zero_is_identity():
    base = [mk(i, i * 7) for i in range(200)]
    assert list(apply_disorder(base, 300, seed=1)) == list(apply_disorder(base, 300, seed=1))
    assert list(apply_disorder(base, 0)) == base


def test_paced_sleeps_to_schedule_and_respects_speedup():
    trades = [mk(1, 0), mk(2, 1000), mk(3, 3000)]  # event-time gaps: 1 s, 2 s
    now = [0.0]
    slept: list[float] = []

    def sleep(s: float) -> None:
        slept.append(s)
        now[0] += s

    out = list(paced(trades, speedup=2.0, clock=lambda: now[0], sleep=sleep))
    assert out == trades
    assert slept == [0.5, 1.0]  # 1 s and 2 s of event time at 2x speed

    slept.clear()
    list(paced(trades, speedup=0, clock=lambda: now[0], sleep=sleep))
    assert slept == []
