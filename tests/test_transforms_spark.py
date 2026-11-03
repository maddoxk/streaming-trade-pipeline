"""Spark tests (local SparkSession; needs Java 17 + pyspark, so skipped if pyspark is absent)."""

import json
import os

import pandas as pd
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from pipeline.reference import BAR_COLUMNS, diff_bars, load_csvs, reference_bars  # noqa: E402
from pipeline.transforms import (  # noqa: E402
    aggregate_windows,
    parse_trades,
    to_sink_columns,
)

T0 = 1_717_425_000_000  # 2024-06-03 14:30:00 UTC, minute aligned


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield s
    s.stop()


def trade_json(symbol="BTCUSDT", trade_id=1, price=100.0, qty=1.0, t_ms=T0, maker=False):
    return json.dumps(
        {
            "symbol": symbol,
            "trade_id": trade_id,
            "price": price,
            "quantity": qty,
            "trade_time_ms": t_ms,
            "is_buyer_maker": maker,
        }
    )


def raw_df(spark, payloads):
    """Mimic the Kafka source schema: value + timestamp."""
    return spark.createDataFrame([(p,) for p in payloads], ["value"]).withColumn(
        "timestamp", F.current_timestamp()
    )


def collect_bars(bars) -> pd.DataFrame:
    return to_sink_columns(bars).toPandas()[BAR_COLUMNS]


def test_parse_trades_drops_invalid_records(spark):
    payloads = [
        trade_json(trade_id=1),
        "this is not json",
        trade_json(trade_id=2, price=-1.0),
        trade_json(trade_id=3, qty=0.0),
        trade_json(trade_id=4, symbol="bad symbol"),
        trade_json(trade_id=5, t_ms=T0 * 1000),  # microseconds mistaken for ms
        json.dumps({"symbol": "BTCUSDT", "trade_id": 6, "price": 1.0}),  # missing fields
        trade_json(trade_id=7),
    ]
    out = parse_trades(raw_df(spark, payloads))
    assert sorted(r.trade_id for r in out.collect()) == [1, 7]
    row = out.where("trade_id = 1").collect()[0]
    assert row.event_time is not None and row.price == 100.0


def test_bar_handles_unsorted_input_and_single_tick(spark):
    payloads = [
        trade_json(trade_id=3, price=110.0, t_ms=T0 + 30_000),
        trade_json(trade_id=1, price=100.0, qty=2.0, t_ms=T0 + 1_000),
        trade_json(trade_id=2, price=90.0, t_ms=T0 + 10_000),
        trade_json(trade_id=4, price=105.0, qty=4.0, t_ms=T0 + 60_000),
    ]
    bars = collect_bars(aggregate_windows(parse_trades(raw_df(spark, payloads))))
    bars = bars.sort_values("window_start_s").reset_index(drop=True)
    expected = reference_bars(
        pd.DataFrame(
            [
                ("BTCUSDT", 3, 110.0, 1.0, T0 + 30_000),
                ("BTCUSDT", 1, 100.0, 2.0, T0 + 1_000),
                ("BTCUSDT", 2, 90.0, 1.0, T0 + 10_000),
                ("BTCUSDT", 4, 105.0, 4.0, T0 + 60_000),
            ],
            columns=["symbol", "trade_id", "price", "quantity", "trade_time_ms"],
        )
    )
    assert diff_bars(expected, bars) == []
    assert bars.loc[0, ["open", "close"]].tolist() == [100.0, 110.0]
    assert bars.loc[1, "realized_vol"] == 0.0


def test_sample_data_matches_pandas_reference(spark, sample_files):
    trades = load_csvs(sample_files)
    payloads = [
        trade_json(
            r.symbol, int(r.trade_id), float(r.price), float(r.quantity), int(r.trade_time_ms)
        )
        for r in trades.itertuples()
    ]
    bars = collect_bars(aggregate_windows(parse_trades(raw_df(spark, payloads))))
    assert diff_bars(reference_bars(trades), bars) == []


def run_file_stream(spark, tmp_path, batches):
    """Feed one file per micro-batch through the real streaming plan; return last row per window."""
    in_dir = tmp_path / "in"
    in_dir.mkdir()
    for i, payloads in enumerate(batches):
        f = in_dir / f"batch{i}.json"
        f.write_text("\n".join(payloads) + "\n")
        os.utime(f, (1_000_000 + i, 1_000_000 + i))  # force processing order

    final: dict[tuple, dict] = {}

    def collect(batch_df, _batch_id):
        for r in to_sink_columns(batch_df).collect():
            final[(r.symbol, r.window_start_s)] = r.asDict()

    raw = (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", 1)
        .load(str(in_dir))
        .withColumn("timestamp", F.current_timestamp())
    )
    q = (
        aggregate_windows(parse_trades(raw), watermark="30 seconds")
        .writeStream.outputMode("update")
        .foreachBatch(collect)
        .option("checkpointLocation", str(tmp_path / "ckpt"))
        .trigger(availableNow=True)
        .start()
    )
    q.awaitTermination()
    return final


def test_late_data_behaviour_with_watermark(spark, tmp_path):
    w0 = T0 // 1000  # window starts, epoch seconds
    w9, w10 = w0 + 9 * 60, w0 + 10 * 60
    final = run_file_stream(
        spark,
        tmp_path,
        [
            # Batch 0: two trades in minute 0, one in minute 10 -> max event time T0+605s,
            # so the watermark becomes T0+575s once this batch has finished.
            [
                trade_json(trade_id=1, t_ms=T0 + 1_000),
                trade_json(trade_id=2, t_ms=T0 + 20_000),
                trade_json(trade_id=3, t_ms=T0 + 605_000),
            ],
            # Batch 1: a trade for minute 0 (window already closed -> dropped as too late)
            # and a trade for minute 9 at T0+580s (25 s behind the newest data, but
            # inside the 30 s allowed lateness -> accepted).
            [
                trade_json(trade_id=4, t_ms=T0 + 30_000),
                trade_json(trade_id=5, t_ms=T0 + 580_000),
            ],
            # Batch 2: ordinary on-time trade in minute 10.
            [trade_json(trade_id=6, t_ms=T0 + 620_000)],
        ],
    )
    counts = {k[1]: v["trade_count"] for k, v in final.items()}
    assert counts[w0] == 2, "late trade 4 must not be added to the closed window"
    assert counts[w9] == 1, "trade 5 is late but within the watermark delay"
    assert counts[w10] == 2
