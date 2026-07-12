from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


class AsterContractError(ValueError):
    pass


def validate_aster_contract(kind: str, payload: Any) -> None:
    """Fail closed when Aster diverges from the explicitly tested wire contract."""
    required: dict[str, set[str]] = {
        "exchange_info": {"symbols"},
        "funding_history_item": {"symbol", "fundingRate", "fundingTime"},
        "open_interest": {"symbol", "openInterest", "time"},
        "book_snapshot": {"lastUpdateId", "bids", "asks"},
        "depth20": {"e", "E", "s", "u", "b", "a"},
        "agg_trade": {"e", "E", "s", "a", "p", "q", "T", "m"},
        "book_ticker": {"u", "s", "b", "B", "a", "A"},
    }
    if kind == "klines":
        if not isinstance(payload, Sequence) or any(
            not isinstance(row, Sequence) or len(row) < 11 for row in payload
        ):
            raise AsterContractError("invalid klines schema")
        return
    if kind in {"rate_limit", "error", "maintenance", "symbol_disabled"}:
        if not isinstance(payload, Mapping) or not ({"code", "msg"} <= payload.keys()):
            raise AsterContractError(f"invalid {kind} response")
        return
    fields = required.get(kind)
    if fields is None:
        raise AsterContractError(f"unknown Aster contract: {kind}")
    if not isinstance(payload, Mapping) or not fields <= payload.keys():
        raise AsterContractError(f"invalid {kind} schema")


def validate_pagination_boundary(previous_last: int, next_first: int, interval_ms: int) -> None:
    if next_first != previous_last + interval_ms:
        raise AsterContractError("pagination boundary has a gap or duplicate")
