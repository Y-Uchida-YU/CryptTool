# Phase 7 validation methodology

All splits are chronological. Hyperparameters are chosen on train/validation only; the final test
window remains untouched. Purge removes labels or positions overlapping a fold boundary and
embargo prevents near-boundary information reuse. Anchored and rolling walk-forward results are
reported individually, including losing windows.

Monte Carlo uses deterministic seeded block/bootstrap resampling and explicitly stresses fees,
slippage, fill rate and trade ordering. Sensitivity reports the entire predefined parameter grid,
not only the maximum. A candidate must occupy a stable neighborhood; isolated peaks fail.

Reports include core return/risk/trade/cost metrics, regime/strategy/symbol strata, 100/300/1,000
capital feasibility, leave-one-period/asset-out checks, ruin estimates and overfitting diagnostics.
PBO and deflated-Sharpe-style estimates are diagnostic, not proof of an edge. With no external
historical dataset in the repository, synthetic runs verify mechanics only and cannot satisfy the
economic acceptance criteria.
