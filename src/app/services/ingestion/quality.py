from dataclasses import dataclass
from datetime import timedelta
from itertools import pairwise
from statistics import median

from app.domain.market_data.models import OHLCV, DataQualityIssue

TIMEFRAME_SECONDS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}


@dataclass(frozen=True)
class ValidationResult:
    accepted: tuple[OHLCV, ...]
    rejected: tuple[OHLCV, ...]
    issues: tuple[DataQualityIssue, ...]
    quality_score: float


def validate_ohlcv(rows: list[OHLCV], volume_outlier_multiple: float = 20.0) -> ValidationResult:
    if not rows:
        return ValidationResult(
            (),
            (),
            (DataQualityIssue(code="EMPTY", severity="fatal", reason="no observations"),),
            0.0,
        )
    ordered = sorted(rows, key=lambda row: row.timestamp)
    issues: list[DataQualityIssue] = []
    accepted: list[OHLCV] = []
    rejected: list[OHLCV] = []
    seen: set[tuple[str, str, str, object]] = set()
    positive_volumes = [float(row.volume) for row in ordered if row.volume > 0]
    typical_volume = median(positive_volumes) if positive_volumes else None
    for row in ordered:
        key = (row.exchange, row.symbol, row.timeframe, row.timestamp)
        if key in seen:
            rejected.append(row)
            issues.append(
                DataQualityIssue(
                    code="DUPLICATE",
                    severity="error",
                    timestamp=row.timestamp,
                    reason="duplicate natural key",
                )
            )
            continue
        seen.add(key)
        if typical_volume and float(row.volume) > typical_volume * volume_outlier_multiple:
            rejected.append(row)
            issues.append(
                DataQualityIssue(
                    code="VOLUME_OUTLIER",
                    severity="error",
                    timestamp=row.timestamp,
                    field="volume",
                    original_value=str(row.volume),
                    reason="exceeds robust multiple of median",
                )
            )
            continue
        accepted.append(row)
    if accepted:
        step = timedelta(seconds=TIMEFRAME_SECONDS[accepted[0].timeframe])
        for previous, current in pairwise(accepted):
            if current.timestamp - previous.timestamp > step:
                issues.append(
                    DataQualityIssue(
                        code="GAP",
                        severity="error",
                        timestamp=current.timestamp,
                        reason=f"expected interval {step}",
                    )
                )
        if rows != ordered:
            issues.append(
                DataQualityIssue(
                    code="OUT_OF_ORDER",
                    severity="warning",
                    reason="input reordered; original data retained in audit source",
                )
            )
    penalty = sum(0.25 if issue.severity == "error" else 0.05 for issue in issues)
    return ValidationResult(
        tuple(accepted), tuple(rejected), tuple(issues), max(0.0, min(1.0, 1.0 - penalty))
    )
