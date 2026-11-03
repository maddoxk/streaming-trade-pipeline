"""Postgres sink: schema and idempotent upsert for 1-minute bars."""

from __future__ import annotations

DDL = """
CREATE TABLE IF NOT EXISTS ohlc_1m (
    symbol          TEXT             NOT NULL,
    window_start    TIMESTAMPTZ      NOT NULL,
    window_end      TIMESTAMPTZ      NOT NULL,
    open            DOUBLE PRECISION NOT NULL,
    high            DOUBLE PRECISION NOT NULL,
    low             DOUBLE PRECISION NOT NULL,
    close           DOUBLE PRECISION NOT NULL,
    vwap            DOUBLE PRECISION NOT NULL,
    volume          DOUBLE PRECISION NOT NULL,
    trade_count     BIGINT           NOT NULL,
    realized_vol    DOUBLE PRECISION NOT NULL,
    last_ingest_ts  TIMESTAMPTZ      NOT NULL,  -- Kafka timestamp of newest trade in the window
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (symbol, window_start)
)
"""

# Row layout produced by transforms.to_sink_columns (timestamps as epoch seconds).
_INSERT_COLUMNS = (
    "symbol, window_start, window_end, open, high, low, close, vwap, volume, "
    "trade_count, realized_vol, last_ingest_ts"
)
_VALUES_TEMPLATE = (
    "(%s, to_timestamp(%s), to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))"
)

# The primary key (symbol, window_start) makes the write idempotent: applying
# the same batch twice leaves the table unchanged (apart from updated_at), and
# a later, more complete version of a window simply overwrites the earlier one.
UPSERT_SQL = f"""
INSERT INTO ohlc_1m ({_INSERT_COLUMNS}) VALUES %s
ON CONFLICT (symbol, window_start) DO UPDATE SET
    window_end     = EXCLUDED.window_end,
    open           = EXCLUDED.open,
    high           = EXCLUDED.high,
    low            = EXCLUDED.low,
    close          = EXCLUDED.close,
    vwap           = EXCLUDED.vwap,
    volume         = EXCLUDED.volume,
    trade_count    = EXCLUDED.trade_count,
    realized_vol   = EXCLUDED.realized_vol,
    last_ingest_ts = EXCLUDED.last_ingest_ts,
    updated_at     = clock_timestamp()
"""


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def upsert_rows(conn, rows: list[tuple]) -> int:
    """Upsert rows (tuples in to_sink_columns order) in one transaction."""
    if not rows:
        return 0
    from psycopg2.extras import execute_values

    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, template=_VALUES_TEMPLATE)
    conn.commit()
    return len(rows)
