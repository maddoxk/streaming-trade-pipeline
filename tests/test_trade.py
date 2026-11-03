import json

import pytest
from jsonschema import Draft202012Validator

from pipeline.trade import Trade, ValidationError, parse_row
from tests.conftest import ROOT

GOOD = {
    "symbol": "BTCUSDT",
    "agg_trade_id": "42",
    "price": "69815.87000000",
    "quantity": "0.00033000",
    "transact_time_ms": "1717425000009",
    "is_buyer_maker": "true",
}


def test_parse_good_row():
    t = parse_row(GOOD)
    assert t == Trade("BTCUSDT", 42, 69815.87, 0.00033, 1717425000009, True)


@pytest.mark.parametrize(
    "field,value",
    [
        ("symbol", "btc usdt"),
        ("agg_trade_id", "-1"),
        ("agg_trade_id", "abc"),
        ("price", "0"),
        ("price", "-5"),
        ("price", "nan"),
        ("quantity", "0"),
        ("quantity", "inf"),
        ("transact_time_ms", "1717425000009000"),  # microseconds mistaken for ms
        ("transact_time_ms", "12"),
        ("is_buyer_maker", "maybe"),
    ],
)
def test_parse_rejects_bad_values(field, value):
    with pytest.raises(ValidationError):
        parse_row({**GOOD, field: value})


def test_parse_rejects_missing_column():
    row = dict(GOOD)
    del row["price"]
    with pytest.raises(ValidationError):
        parse_row(row)


def test_json_roundtrip_matches_schema():
    schema = json.loads((ROOT / "schemas/trade.schema.json").read_text())
    validator = Draft202012Validator(schema)
    payload = json.loads(parse_row(GOOD).to_json())
    assert list(validator.iter_errors(payload)) == []
    # The schema also rejects what the Python validator rejects.
    assert list(validator.iter_errors({**payload, "price": 0}))
    assert list(validator.iter_errors({**payload, "symbol": "bad symbol"}))
    assert list(validator.iter_errors({k: v for k, v in payload.items() if k != "price"}))
