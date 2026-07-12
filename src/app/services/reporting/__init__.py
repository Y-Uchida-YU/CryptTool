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
    evaluate_acceptance,
    generate_report,
)

__all__ = [
    "AcceptanceAssessment",
    "AcceptanceCheck",
    "PerformanceMetrics",
    "ReportArtifacts",
    "aggregate_trade_performance",
    "calculate_performance_metrics",
    "drawdown_series",
    "evaluate_acceptance",
    "generate_report",
    "monthly_returns",
    "regime_distribution",
    "regime_transition_matrix",
]
