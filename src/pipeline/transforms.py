"""Spark transformations: JSON parsing/validation and 1-minute event-time bars.

Everything here is a pure DataFrame -> DataFrame function, so the same code
runs on a streaming Kafka source in production and on a plain batch DataFrame
in unit tests.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Mirrors schemas/trade.schema.json.
TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("trade_id", LongType()),
        StructField("price", DoubleType()),
        StructField("quantity", DoubleType()),
        StructField("trade_time_ms", LongType()),
        StructField("is_buyer_maker", BooleanType()),
    ]
)

# Allowed lateness. See docs/DESIGN.md ("Watermark choice") for the reasoning.
DEFAULT_WATERMARK = "30 seconds"
DEFAULT_WINDOW = "1 minute"

# Same sanity bounds as pipeline.trade (2017-01-01 .. 2100-01-01 UTC in ms).
_MIN_TS_MS = 1_483_228_800_000
_MAX_TS_MS = 4_102_444_800_000


def parse_trades(raw: DataFrame) -> DataFrame:
    """Turn raw Kafka-style rows (`value` JSON, `timestamp`) into typed, valid trades.

    Rows that are not valid JSON or violate the trade schema are dropped.
    (A production system would route them to a dead-letter topic; here they
    are simply filtered, see Limitations in the README.)

    Output columns: symbol, trade_id, price, quantity, trade_time_ms,
    is_buyer_maker, ingest_ts (Kafka record timestamp), event_time (timestamp
    derived from trade_time_ms).
    """
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), TRADE_SCHEMA).alias("t"),
        F.col("timestamp").alias("ingest_ts"),
    )
    valid = (
        F.col("symbol").rlike("^[A-Z0-9]{3,20}$")
        & (F.col("trade_id") >= 0)
        & (F.col("price") > 0)
        & ~F.isnan("price")
        & (F.col("quantity") > 0)
        & ~F.isnan("quantity")
        & F.col("trade_time_ms").between(_MIN_TS_MS, _MAX_TS_MS)
        & F.col("is_buyer_maker").isNotNull()
    )  # comparisons against NULL are NULL, so missing fields are filtered too
    return (
        parsed.where(F.col("t").isNotNull())
        .select("t.*", "ingest_ts")
        .where(valid)
        .withColumn("event_time", (F.col("trade_time_ms") / 1000).cast("timestamp"))
    )


# Realized volatility = sqrt(sum of squared tick-to-tick log returns) inside the
# window. `ticks` is sorted by (trade_time_ms, trade_id): zip_with pairs each
# tick with its predecessor, aggregate() sums the squared log returns.
_REALIZED_VOL = """
CASE WHEN size(ticks) < 2 THEN 0.0 ELSE sqrt(aggregate(
    zip_with(slice(ticks, 2, size(ticks) - 1), slice(ticks, 1, size(ticks) - 1),
             (a, b) -> pow(ln(a.price / b.price), 2)),
    cast(0.0 as double),
    (acc, x) -> acc + x)) END
"""


def aggregate_windows(
    trades: DataFrame,
    watermark: str = DEFAULT_WATERMARK,
    window: str = DEFAULT_WINDOW,
) -> DataFrame:
    """Per-symbol tumbling-window bars keyed by (symbol, window).

    Open/close/volatility depend on trade *order*, which a plain streaming
    aggregate cannot express (first()/last() are non-deterministic). So each
    window keeps its ticks in a list, sorted by (time, id) once the aggregate
    is read. Memory per window is O(trades in the window), fine for one-minute
    windows of a couple of symbols; see DESIGN.md for the trade-off.
    """
    tick = F.struct("trade_time_ms", "trade_id", "price", "quantity")
    agg = (
        trades.withWatermark("event_time", watermark)
        # Keep the default column name `window`: Spark tags that column with the
        # watermark metadata, which it needs for late-data dropping and state
        # eviction. (Aliasing it loses the tag and the watermark silently does nothing;
        # tests/test_transforms_spark.py::test_late_data_behaviour_with_watermark covers this.)
        .groupBy("symbol", F.window("event_time", window))
        .agg(
            F.collect_list(tick).alias("unsorted_ticks"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.sum("quantity").alias("volume"),
            F.sum(F.col("price") * F.col("quantity")).alias("notional"),
            F.count(F.lit(1)).alias("trade_count"),
            F.max("ingest_ts").alias("last_ingest_ts"),
        )
        .withColumn("ticks", F.array_sort("unsorted_ticks"))  # struct order: time, then id
    )
    return agg.select(
        "symbol",
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.expr("ticks[0].price").alias("open"),
        "high",
        "low",
        F.expr("element_at(ticks, -1).price").alias("close"),
        (F.col("notional") / F.col("volume")).alias("vwap"),
        "volume",
        "trade_count",
        F.expr(_REALIZED_VOL).alias("realized_vol"),
        "last_ingest_ts",
    )


def to_sink_columns(bars: DataFrame) -> DataFrame:
    """Timestamps -> epoch numbers so rows cross the JVM/Python boundary unambiguously."""
    return bars.select(
        "symbol",
        F.col("window_start").cast("long").alias("window_start_s"),
        F.col("window_end").cast("long").alias("window_end_s"),
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "volume",
        F.col("trade_count").cast("long").alias("trade_count"),
        "realized_vol",
        F.col("last_ingest_ts").cast("double").alias("last_ingest_epoch_s"),
    )
