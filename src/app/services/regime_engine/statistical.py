from dataclasses import dataclass

import numpy as np
from sklearn.mixture import GaussianMixture  # type: ignore[import-untyped]
from sklearn.preprocessing import StandardScaler  # type: ignore[import-untyped]


@dataclass(frozen=True)
class StatisticalPrediction:
    state: int
    probability: float
    probabilities: tuple[float, ...]


class GaussianMixtureRegimeModel:
    """Deterministic baseline model. State labels remain statistical, not semantic."""

    def __init__(self, n_components: int = 3, random_state: int = 17) -> None:
        self.scaler = StandardScaler()
        self.model = GaussianMixture(
            n_components=n_components,
            covariance_type="full",
            random_state=random_state,
            n_init=10,
            reg_covar=1e-6,
        )
        self.is_fitted = False

    def fit(self, values: np.ndarray) -> "GaussianMixtureRegimeModel":
        if (
            values.ndim != 2
            or len(values) < max(30, self.model.n_components * 10)
            or not np.isfinite(values).all()
        ):
            raise ValueError("finite two-dimensional training history with >=30 rows is required")
        self.model.fit(self.scaler.fit_transform(values))
        self.is_fitted = True
        return self

    def predict(self, observation: np.ndarray) -> StatisticalPrediction:
        if not self.is_fitted:
            raise RuntimeError("model is not fitted")
        probabilities = self.model.predict_proba(self.scaler.transform(np.atleast_2d(observation)))[
            0
        ]
        state = int(np.argmax(probabilities))
        return StatisticalPrediction(
            state, float(probabilities[state]), tuple(float(value) for value in probabilities)
        )
