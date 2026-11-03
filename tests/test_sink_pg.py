"""Postgres sink tests. Run only when PG_DSN is set (CI provides a Postgres service)."""

import os

import pytest

PG_DSN = os.environ.get("PG_DSN")
pytestmark = pytest.mark.skipif(not PG_DSN, reason="PG_DSN not set")

T0 = 1_717_425_000


def row(symbol="BTCUSDT", start=T0, close=101.0, count=3):
    # to_sink_columns order: symbol, start_s, end_s, o, h, l, c, vwap, vol, n, rv, ingest_s
    return (symbol, start, start + 60, 100.0, 102.0, 99.0, close, 100.5, 5.0, count, 0.001, T0 + 61)


@pytest.fixture
def conn():
    import psycopg2

    from pipeline.sink import ensure_schema

    c = psycopg2.connect(PG_DSN)
    ensure_schema(c)
    with c.cursor() as cur:
        cur.execute("TRUNCATE ohlc_1m")
    c.commit()
    yield c
    c.close()


def fetch(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT symbol, extract(epoch FROM window_start)::bigint, close, trade_count "
            "FROM ohlc_1m ORDER BY 1, 2"
        )
        return cur.fetchall()


def test_upsert_is_idempotent(conn):
    from pipeline.sink import upsert_rows

    batch = [row(), row(start=T0 + 60), row(symbol="ETHUSDT")]
    assert upsert_rows(conn, batch) == 3
    before = fetch(conn)
    upsert_rows(conn, batch)  # replaying the same batch (e.g. after a restart)
    assert fetch(conn) == before and len(before) == 3


def test_upsert_overwrites_with_newer_window_state(conn):
    from pipeline.sink import upsert_rows

    upsert_rows(conn, [row(close=101.0, count=3)])
    upsert_rows(conn, [row(close=105.0, count=7)])  # late data arrived; window recomputed
    assert fetch(conn) == [("BTCUSDT", T0, 105.0, 7)]


def test_empty_batch_is_noop(conn):
    from pipeline.sink import upsert_rows

    assert upsert_rows(conn, []) == 0
    assert fetch(conn) == []


def test_report_runs(conn, capsys):
    from pipeline.report import main
    from pipeline.sink import upsert_rows

    upsert_rows(conn, [row(), row(start=T0 + 60)])
    main(["--dsn", PG_DSN, "--recent", "2"])
    out = capsys.readouterr().out
    assert "per-symbol summary" in out and "BTCUSDT" in out and "latency" in out
