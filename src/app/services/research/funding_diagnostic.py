from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from typing import Any

import httpx


class FundingHistoryEmptyReason(StrEnum):
    API_EMPTY = "API_EMPTY"
    SYMBOL_NOT_FOUND = "SYMBOL_NOT_FOUND"
    PARAMETER_INVALID = "PARAMETER_INVALID"
    PAGINATION_REQUIRED = "PAGINATION_REQUIRED"
    CHECKPOINT_FILTERED_ALL = "CHECKPOINT_FILTERED_ALL"
    NORMALIZATION_REJECTED_ALL = "NORMALIZATION_REJECTED_ALL"
    DUPLICATED_ALL = "DUPLICATED_ALL"


@dataclass(frozen=True)
class NormalizedFundingItem:
    venue: str
    instrument: str
    funding_effective_at: datetime
    rate: str


@dataclass(frozen=True)
class FundingHistoryDiagnostic:
    request_endpoint: str
    sanitized_parameters: dict[str, object]
    http_status: int
    raw_response_item_count: int
    normalized_item_count: int
    deduplicated_item_count: int
    oldest_effective_time: datetime | None
    newest_effective_time: datetime | None
    response_ordering: str
    pagination_cursor: str | None
    api_limit: int
    symbol_mapping: str
    funding_interval_metadata: str
    rejection_reasons: tuple[str, ...]
    empty_reason: FundingHistoryEmptyReason | None


@dataclass(frozen=True)
class FundingHistoryDiagnosticResult:
    diagnostic: FundingHistoryDiagnostic
    items: tuple[NormalizedFundingItem, ...]
    raw_responses: tuple[object, ...]

    def write(self, directory: Path) -> Path:
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "raw-response.json").write_text(
            json.dumps(_sanitize(self.raw_responses), indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        path = directory / "diagnostic.json"
        path.write_text(
            json.dumps(asdict(self.diagnostic), indent=2, sort_keys=True, default=str) + "\n",
            encoding="utf-8",
        )
        return path


SECRET_TOKENS = ("secret", "token", "authorization", "api_key", "apikey", "password")


def _sanitize(value: object) -> object:
    if isinstance(value, dict):
        return {
            str(key): (
                "***"
                if any(token in str(key).lower() for token in SECRET_TOKENS)
                else _sanitize(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_sanitize(item) for item in value]
    return value


def deduplicate_funding_items(
    items: tuple[NormalizedFundingItem, ...],
) -> tuple[NormalizedFundingItem, ...]:
    by_identity = {(item.venue, item.instrument, item.funding_effective_at): item for item in items}
    return tuple(by_identity[key] for key in sorted(by_identity, key=lambda item: item[2]))


def apply_funding_checkpoint(
    items: tuple[NormalizedFundingItem, ...], checkpoint: datetime | None
) -> tuple[tuple[NormalizedFundingItem, ...], FundingHistoryEmptyReason | None]:
    if checkpoint is None:
        return items, None
    retained = tuple(item for item in items if item.funding_effective_at > checkpoint)
    reason = FundingHistoryEmptyReason.CHECKPOINT_FILTERED_ALL if items and not retained else None
    return retained, reason


def _ordering(times: tuple[datetime, ...]) -> str:
    if not times:
        return "EMPTY"
    if len(times) == 1:
        return "SINGLE"
    if all(current >= previous for previous, current in pairwise(times)):
        return "ASCENDING"
    if all(current <= previous for previous, current in pairwise(times)):
        return "DESCENDING"
    return "MIXED"


async def diagnose_funding_history(
    *,
    venue: str,
    instrument: str,
    client: httpx.AsyncClient | None = None,
    checkpoint: datetime | None = None,
) -> FundingHistoryDiagnosticResult:
    now = datetime.now(UTC)
    owned = client is None
    http = client or httpx.AsyncClient(timeout=20)
    raw_responses: list[object] = []
    raw_items: list[dict[str, Any]] = []
    rejection_reasons: list[str] = []
    status = 0
    cursor: str | None = None
    if venue == "hyperliquid":
        endpoint = "https://api.hyperliquid.xyz/info"
        symbol = instrument
        limit = 500
        parameters: dict[str, str | int] = {
            "type": "fundingHistory",
            "coin": symbol,
            "startTime": int((now - timedelta(days=7)).timestamp() * 1000),
            "endTime": int(now.timestamp() * 1000),
        }
        interval = "3600 seconds (documented hourly schedule)"
        try:
            response = await http.post(endpoint, json=parameters)
            status = response.status_code
            body = response.json()
            raw_responses.append(body)
            if isinstance(body, list):
                raw_items.extend(item for item in body if isinstance(item, dict))
            if len(raw_items) >= limit:
                cursor = str(raw_items[-1].get("time"))
        except (httpx.HTTPError, ValueError) as exc:
            rejection_reasons.append(f"{type(exc).__name__}: {exc}")
    elif venue == "bitget":
        endpoint = "https://api.bitget.com/api/v2/mix/market/history-fund-rate"
        symbol = f"{instrument}USDT"
        limit = 100
        parameters = {
            "symbol": symbol,
            "productType": "USDT-FUTURES",
            "pageSize": limit,
            "pageNo": 1,
        }
        interval = "28800 seconds (documented eight-hour schedule)"
        try:
            response = await http.get(endpoint, params=parameters)
            status = response.status_code
            body = response.json()
            raw_responses.append(body)
            data = body.get("data", []) if isinstance(body, dict) else []
            if isinstance(data, list):
                raw_items.extend(item for item in data if isinstance(item, dict))
            if len(raw_items) >= limit:
                cursor = "pageNo=2"
        except (httpx.HTTPError, ValueError) as exc:
            rejection_reasons.append(f"{type(exc).__name__}: {exc}")
    else:
        raise ValueError(f"unsupported funding diagnostic venue: {venue}")
    if owned:
        await http.aclose()

    normalized: list[NormalizedFundingItem] = []
    raw_times: list[datetime] = []
    for index, item in enumerate(raw_items):
        try:
            timestamp = item["time"] if venue == "hyperliquid" else item["fundingTime"]
            rate = item["fundingRate"]
            effective = datetime.fromtimestamp(int(timestamp) / 1000, tz=UTC)
            raw_times.append(effective)
            normalized.append(NormalizedFundingItem(venue, instrument, effective, str(rate)))
        except (KeyError, TypeError, ValueError) as exc:
            rejection_reasons.append(f"item[{index}] {type(exc).__name__}: {exc}")
    deduplicated = deduplicate_funding_items(tuple(normalized))
    filtered, checkpoint_reason = apply_funding_checkpoint(deduplicated, checkpoint)
    empty_reason: FundingHistoryEmptyReason | None = checkpoint_reason
    body_text = json.dumps(raw_responses, default=str).lower()
    if empty_reason is None and not filtered:
        if status in {400, 422}:
            empty_reason = FundingHistoryEmptyReason.PARAMETER_INVALID
        elif status == 404 or ("symbol" in body_text and "not" in body_text):
            empty_reason = FundingHistoryEmptyReason.SYMBOL_NOT_FOUND
        elif raw_items and not normalized:
            empty_reason = FundingHistoryEmptyReason.NORMALIZATION_REJECTED_ALL
        elif normalized and not deduplicated:
            empty_reason = FundingHistoryEmptyReason.DUPLICATED_ALL
        else:
            empty_reason = FundingHistoryEmptyReason.API_EMPTY
    elif cursor is not None:
        rejection_reasons.append(FundingHistoryEmptyReason.PAGINATION_REQUIRED.value)
    diagnostic = FundingHistoryDiagnostic(
        request_endpoint=endpoint,
        sanitized_parameters=_sanitize(parameters),  # type: ignore[arg-type]
        http_status=status,
        raw_response_item_count=len(raw_items),
        normalized_item_count=len(normalized),
        deduplicated_item_count=len(filtered),
        oldest_effective_time=min((item.funding_effective_at for item in filtered), default=None),
        newest_effective_time=max((item.funding_effective_at for item in filtered), default=None),
        response_ordering=_ordering(tuple(raw_times)),
        pagination_cursor=cursor,
        api_limit=limit,
        symbol_mapping=f"{instrument}->{symbol}",
        funding_interval_metadata=interval,
        rejection_reasons=tuple(rejection_reasons),
        empty_reason=empty_reason,
    )
    return FundingHistoryDiagnosticResult(diagnostic, filtered, tuple(raw_responses))
