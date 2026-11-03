"""Spark Structured Streaming job: Kafka `trades` -> 1-minute bars -> Postgres.

Run (inside the compose `spark` service, see docker/spark/Dockerfile):
    python -m pipeline.spark_job --bootstrap kafka:19092 --topic trades \
        --pg-dsn postgresql://pipeline:pipeline@postgres:5432/pipeline
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQueryListener

from pipeline.sink import ensure_schema, upsert_rows
from pipeline.transforms import (
    DEFAULT_WATERMARK,
    aggregate_windows,
    parse_trades,
    to_sink_columns,
)

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def build_spark(master: str = "local[2]") -> SparkSession:
    return (
        SparkSession.builder.master(master)
        .appName("trade-bars")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.jars.ivy", "/opt/ivy")
        .config("spark.sql.session.timeZone", "UTC")
        # Default is 200 shuffle partitions (200 state shards): far too many for 2 symbols.
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def make_batch_writer(pg_dsn: str):
    """Return the foreachBatch callback that upserts one micro-batch into Postgres."""
    import psycopg2

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        # Append output mode: the batch holds the windows that the watermark just closed.
        # That is a handful of rows (a few per symbol per trigger), so collecting to the
        # driver and doing one multi-row upsert is fine.
        rows = [tuple(r) for r in to_sink_columns(batch).collect()]
        if not rows:
            return
        conn = psycopg2.connect(pg_dsn)
        try:
            upsert_rows(conn, rows)
        finally:
            conn.close()
        print(f"batch {batch_id}: upserted {len(rows)} windows", flush=True)

    return write_batch


class DroppedLateRows(StreamingQueryListener):
    """Make late-data handling visible: log how many rows the watermark discarded."""

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        dropped = sum(op.numRowsDroppedByWatermark for op in event.progress.stateOperators)
        if dropped:
            print(
                f"WATERMARK_DROPPED {dropped} late rows in batch {event.progress.batchId}",
                flush=True,
            )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="trades")
    p.add_argument("--pg-dsn", required=True)
    p.add_argument("--checkpoint", default="/tmp/checkpoints/trade-bars")
    p.add_argument("--watermark", default=DEFAULT_WATERMARK)
    p.add_argument("--trigger", default="1 second", help="micro-batch interval")
    p.add_argument("--max-offsets-per-trigger", type=int, default=None)
    a = p.parse_args(argv)

    import psycopg2

    with psycopg2.connect(a.pg_dsn) as conn:
        ensure_schema(conn)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(DroppedLateRows())

    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", a.bootstrap)
        .option("subscribe", a.topic)
        .option("startingOffsets", "earliest")
    )
    if a.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", a.max_offsets_per_trigger)
    raw = reader.load()  # columns: key, value, topic, partition, offset, timestamp, ...

    bars = aggregate_windows(parse_trades(raw), watermark=a.watermark)
    query = (
        # Append mode: a window is emitted exactly once, when the watermark passes its end.
        # (Update mode would emit early partial results but cannot promise to drop late data.)
        bars.writeStream.outputMode("append")
        .foreachBatch(make_batch_writer(a.pg_dsn))
        .option("checkpointLocation", a.checkpoint)
        .trigger(processingTime=a.trigger)
        .start()
    )
    print("STREAM_STARTED", flush=True)  # the e2e script waits for this line
    query.awaitTermination()


if __name__ == "__main__":
    main()
