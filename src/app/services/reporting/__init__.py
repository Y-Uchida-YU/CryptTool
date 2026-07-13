"""Auditable performance metrics and report artifact generation."""

from app.services.reporting.metrics import (
    PerformanceMetrics,
    aggregate_trade_performance,
    calculate_performance_metrics,
    drawdown_series,
    monthly_returns,
    regime_distribution,
    regime_transition_matrix,
)
from app.services.reporting.report import (
    AcceptanceAssessment,
    AcceptanceCheck,
    ReportArtifacts,
    evaluate_legacy_acceptance,
    generate_report,
)
from app.services.research.pipeline import evaluate_acceptance

__all__ = [
    "AcceptanceAssessment",
    "AcceptanceCheck",
    "PerformanceMetrics",
    "ReportArtifacts",
    "aggregate_trade_performance",
    "calculate_performance_metrics",
    "drawdown_series",
    "evaluate_acceptance",
    "evaluate_legacy_acceptance",
    "generate_report",
    "monthly_returns",
    "regime_distribution",
    "regime_transition_matrix",
]
