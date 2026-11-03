import math

import pandas as pd

from pipeline.reference import closed_windows, diff_bars, load_csvs, reference_bars

T0 = 1_717_425_000_000  # minute-aligned


def frame(rows):
    return pd.DataFrame(rows, columns=["symbol", "trade_id", "price", "quantity", "trade_time_ms"])


def test_hand_computed_bar():
    # Deliberately unsorted input; open/close must follow (time, id) order.
    df = frame(
        [
            ("BTCUSDT", 3, 110.0, 1.0, T0 + 30_000),
            ("BTCUSDT", 1, 100.0, 2.0, T0 + 1_000),
            ("BTCUSDT", 2, 90.0, 1.0, T0 + 10_000),
            ("BTCUSDT", 4, 105.0, 4.0, T0 + 60_000),  # next minute
        ]
    )
    bars = reference_bars(df)
    assert len(bars) == 2
    first = bars.iloc[0]
    assert (first.open, first.high, first.low, first.close) == (100.0, 110.0, 90.0, 110.0)
    assert first.trade_count == 3 and first.volume == 4.0
    assert math.isclose(first.vwap, (100 * 2 + 90 + 110) / 4)
    expected_vol = math.sqrt(math.log(90 / 100) ** 2 + math.log(110 / 90) ** 2)
    assert math.isclose(first.realized_vol, expected_vol)
    # A single-trade window has no returns -> zero volatility.
    assert bars.iloc[1].realized_vol == 0.0 and bars.iloc[1].trade_count == 1


def test_symbols_are_independent_and_sample_loads(sample_files):
    bars = reference_bars(load_csvs(sample_files))
    assert set(bars.symbol) == {"BTCUSDT", "ETHUSDT"}
    # 15 minutes of data starting 14:30:00 UTC for both symbols (a window or two
    # at the edges may be partial, but there are always 15 distinct minutes).
    assert bars.groupby("symbol").size().to_dict() == {"BTCUSDT": 15, "ETHUSDT": 15}
    assert (bars.high >= bars.low).all() and (bars.realized_vol > 0).all()
    assert bars.trade_count.sum() == len(load_csvs(sample_files))


def test_diff_bars_detects_problems():
    bars = reference_bars(load_csvs_small())
    assert diff_bars(bars, bars) == []
    bad = bars.copy()
    bad.loc[0, "close"] += 1.0
    bad.loc[1, "trade_count"] += 1
    problems = diff_bars(bars, bad)
    assert any("close" in p for p in problems) and any("trade_count" in p for p in problems)
    assert any("missing window" in p for p in diff_bars(bars, bars.iloc[:-1]))
    assert any("unexpected window" in p for p in diff_bars(bars.iloc[:-1], bars))
    # tiny float noise is tolerated
    noisy = bars.copy()
    noisy["vwap"] *= 1 + 1e-12
    assert diff_bars(bars, noisy) == []


def load_csvs_small():
    return frame(
        [
            ("BTCUSDT", 1, 100.0, 1.0, T0 + 1_000),
            ("BTCUSDT", 2, 101.0, 1.0, T0 + 2_000),
            ("BTCUSDT", 3, 99.0, 2.0, T0 + 61_000),
            ("BTCUSDT", 4, 98.0, 2.0, T0 + 62_000),
        ]
    )


def test_closed_windows_follow_the_watermark():
    bars = reference_bars(load_csvs_small())  # windows [T0, T0+60s) and [T0+60s, T0+120s)
    newest = T0 + 62_000
    # watermark = newest - 30 s = T0+32 s: neither window has ended yet
    assert len(closed_windows(bars, newest, 30_000)) == 0
    # watermark = T0+62 s - 1 s = T0+61 s: first window (end T0+60 s) is closed
    assert len(closed_windows(bars, newest, 1_000)) == 1
    # a window closes exactly when watermark == window end
    assert len(closed_windows(bars, T0 + 60_000, 0)) == 1
