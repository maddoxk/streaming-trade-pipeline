"""Trade record: parsing from CSV rows, validation, JSON encoding.

The same rules are written down declaratively in schemas/trade.schema.json
(a test checks that the two agree) and re-applied inside the Spark job
(transforms.parse_trades), so bad data is rejected at both ends.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass

SYMBOL_RE = re.compile(r"^[A-Z0-9]{3,20}$")
# Sanity bounds on event time (2017-01-01 .. 2100-01-01 UTC, in ms). They catch
# unit mistakes such as microsecond timestamps being read as milliseconds.
MIN_TS_MS = 1_483_228_800_000
MAX_TS_MS = 4_102_444_800_000


class ValidationError(ValueError):
    """Raised when a raw row cannot be turned into a valid Trade."""


@dataclass(frozen=True, slots=True)
class Trade:
    symbol: str
    trade_id: int
    price: float
    quantity: float
    trade_time_ms: int
    is_buyer_maker: bool

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @property
    def sort_key(self) -> tuple[int, str, int]:
        """Deterministic total order used when merging several files."""
        return (self.trade_time_ms, self.symbol, self.trade_id)


def _parse_bool(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    raise ValidationError(f"is_buyer_maker must be true/false, got {text!r}")


def validate(t: Trade) -> Trade:
    """Return `t` if it satisfies the schema, else raise ValidationError."""
    if not SYMBOL_RE.match(t.symbol):
        raise ValidationError(f"bad symbol {t.symbol!r}")
    if t.trade_id < 0:
        raise ValidationError(f"negative trade_id {t.trade_id}")
    # `not x > 0` (rather than `x <= 0`) also rejects NaN.
    if not (t.price > 0 and math.isfinite(t.price)):
        raise ValidationError(f"price must be a positive finite number, got {t.price}")
    if not (t.quantity > 0 and math.isfinite(t.quantity)):
        raise ValidationError(f"quantity must be a positive finite number, got {t.quantity}")
    if not (MIN_TS_MS <= t.trade_time_ms <= MAX_TS_MS):
        raise ValidationError(f"trade_time_ms {t.trade_time_ms} outside sane range")
    return t


def parse_row(row: dict[str, str]) -> Trade:
    """Build a validated Trade from one normalized-CSV row (see data/README.md)."""
    try:
        trade = Trade(
            symbol=row["symbol"].strip(),
            trade_id=int(row["agg_trade_id"]),
            price=float(row["price"]),
            quantity=float(row["quantity"]),
            trade_time_ms=int(row["transact_time_ms"]),
            is_buyer_maker=_parse_bool(row["is_buyer_maker"]),
        )
    except (KeyError, TypeError, AttributeError, ValueError) as exc:
        if isinstance(exc, ValidationError):
            raise
        raise ValidationError(f"unparseable row {row!r}: {exc}") from exc
    return validate(trade)
