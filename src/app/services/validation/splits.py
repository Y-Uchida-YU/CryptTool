"""Chronological data splits with explicit leakage-control gaps."""

from dataclasses import dataclass

import numpy as np
from numpy.typing import NDArray

IndexArray = NDArray[np.int64]


@dataclass(frozen=True)
class DatasetSplit:
    """Train, validation and final out-of-sample observation indices."""

    train: IndexArray
    validation: IndexArray
    out_of_sample: IndexArray
    purge_size: int
    embargo_size: int


@dataclass(frozen=True)
class PurgedFold:
    """One cross-validation fold with observations around the test fold removed."""

    train: IndexArray
    test: IndexArray
    purged: IndexArray
    embargoed: IndexArray


@dataclass(frozen=True)
class WalkForwardWindow:
    """One immutable train/validation/test walk-forward window."""

    number: int
    train: IndexArray
    validation: IndexArray
    out_of_sample: IndexArray
    anchored: bool


def _validate_gap(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")


def chronological_split(
    n_samples: int,
    *,
    train_fraction: float = 0.6,
    validation_fraction: float = 0.2,
    purge_size: int = 0,
    embargo_size: int = 0,
) -> DatasetSplit:
    """Split ordered observations without shuffling.

    ``purge_size`` removes the tail of train and validation sets, where labels may
    overlap the following period. ``embargo_size`` removes the head of validation
    and out-of-sample sets. Gaps are never reassigned to another split.
    """

    if isinstance(n_samples, bool) or not isinstance(n_samples, int) or n_samples < 3:
        raise ValueError("n_samples must be an integer of at least three")
    if not 0 < train_fraction < 1:
        raise ValueError("train_fraction must be between zero and one")
    if not 0 < validation_fraction < 1 or train_fraction + validation_fraction >= 1:
        raise ValueError("validation_fraction must leave a non-empty out-of-sample share")
    _validate_gap(purge_size, "purge_size")
    _validate_gap(embargo_size, "embargo_size")

    train_boundary = int(np.floor(n_samples * train_fraction))
    validation_boundary = int(np.floor(n_samples * (train_fraction + validation_fraction)))
    train = np.arange(0, train_boundary - purge_size, dtype=np.int64)
    validation = np.arange(
        train_boundary + embargo_size,
        validation_boundary - purge_size,
        dtype=np.int64,
    )
    out_of_sample = np.arange(
        validation_boundary + embargo_size,
        n_samples,
        dtype=np.int64,
    )
    if not train.size or not validation.size or not out_of_sample.size:
        raise ValueError("purge/embargo settings leave an empty chronological split")
    return DatasetSplit(train, validation, out_of_sample, purge_size, embargo_size)


def purged_kfold(
    n_samples: int,
    *,
    n_splits: int = 5,
    purge_size: int = 0,
    embargo_size: int = 0,
) -> tuple[PurgedFold, ...]:
    """Return deterministic contiguous folds for purged cross-validation.

    Training observations in ``[test_start - purge_size, test_end + embargo_size)``
    are excluded. This method is intended for research diagnostics; final model
    selection still requires a later untouched out-of-sample period.
    """

    if isinstance(n_samples, bool) or not isinstance(n_samples, int) or n_samples < 2:
        raise ValueError("n_samples must be an integer of at least two")
    if (
        isinstance(n_splits, bool)
        or not isinstance(n_splits, int)
        or not 2 <= n_splits <= n_samples
    ):
        raise ValueError("n_splits must be between two and n_samples")
    _validate_gap(purge_size, "purge_size")
    _validate_gap(embargo_size, "embargo_size")

    observations = np.arange(n_samples, dtype=np.int64)
    test_blocks = np.array_split(observations, n_splits)
    folds: list[PurgedFold] = []
    for test in test_blocks:
        test_start, test_end = int(test[0]), int(test[-1]) + 1
        purge_start = max(0, test_start - purge_size)
        embargo_end = min(n_samples, test_end + embargo_size)
        mask = np.ones(n_samples, dtype=np.bool_)
        mask[purge_start:embargo_end] = False
        train = observations[mask]
        if not train.size:
            raise ValueError("purge/embargo settings leave an empty training fold")
        purged = np.arange(purge_start, test_start, dtype=np.int64)
        embargoed = np.arange(test_end, embargo_end, dtype=np.int64)
        folds.append(PurgedFold(train, test.copy(), purged, embargoed))
    return tuple(folds)


def walk_forward_splits(
    n_samples: int,
    *,
    train_size: int,
    validation_size: int,
    out_of_sample_size: int,
    step_size: int | None = None,
    anchored: bool = False,
    purge_size: int = 0,
    embargo_size: int = 0,
) -> tuple[WalkForwardWindow, ...]:
    """Build rolling or anchored walk-forward windows in chronological order.

    Sizes describe nominal blocks. Purging shortens the preceding block and each
    embargo adds a gap before the next block. With ``anchored=True`` the training
    start remains zero and its end expands by ``step_size`` each iteration.
    """

    if isinstance(n_samples, bool) or not isinstance(n_samples, int) or n_samples < 1:
        raise ValueError("n_samples must be a positive integer")
    sizes = {
        "train_size": train_size,
        "validation_size": validation_size,
        "out_of_sample_size": out_of_sample_size,
    }
    for name, value in sizes.items():
        minimum = 0 if name == "validation_size" else 1
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"{name} must be an integer of at least {minimum}")
    _validate_gap(purge_size, "purge_size")
    _validate_gap(embargo_size, "embargo_size")
    step = out_of_sample_size if step_size is None else step_size
    if isinstance(step, bool) or not isinstance(step, int) or step < 1:
        raise ValueError("step_size must be a positive integer")
    if train_size <= purge_size or (validation_size and validation_size <= purge_size):
        raise ValueError("purge_size must leave non-empty train and validation blocks")

    windows: list[WalkForwardWindow] = []
    offset = 0
    while True:
        train_start = 0 if anchored else offset
        nominal_train_end = train_size + offset
        train = np.arange(train_start, nominal_train_end - purge_size, dtype=np.int64)

        if validation_size:
            validation_start = nominal_train_end + embargo_size
            nominal_validation_end = validation_start + validation_size
            validation = np.arange(
                validation_start,
                nominal_validation_end - purge_size,
                dtype=np.int64,
            )
            out_of_sample_start = nominal_validation_end + embargo_size
        else:
            validation = np.array([], dtype=np.int64)
            out_of_sample_start = nominal_train_end + embargo_size
        out_of_sample_end = out_of_sample_start + out_of_sample_size
        if out_of_sample_end > n_samples:
            break
        out_of_sample = np.arange(
            out_of_sample_start,
            out_of_sample_end,
            dtype=np.int64,
        )
        windows.append(
            WalkForwardWindow(
                number=len(windows),
                train=train,
                validation=validation,
                out_of_sample=out_of_sample,
                anchored=anchored,
            )
        )
        offset += step
    if not windows:
        raise ValueError("not enough observations for one walk-forward window")
    return tuple(windows)


def rolling_walk_forward(
    n_samples: int,
    *,
    train_size: int,
    validation_size: int,
    out_of_sample_size: int,
    step_size: int | None = None,
    purge_size: int = 0,
    embargo_size: int = 0,
) -> tuple[WalkForwardWindow, ...]:
    """Convenience wrapper for fixed-length rolling training windows."""

    return walk_forward_splits(
        n_samples,
        train_size=train_size,
        validation_size=validation_size,
        out_of_sample_size=out_of_sample_size,
        step_size=step_size,
        anchored=False,
        purge_size=purge_size,
        embargo_size=embargo_size,
    )


def anchored_walk_forward(
    n_samples: int,
    *,
    train_size: int,
    validation_size: int,
    out_of_sample_size: int,
    step_size: int | None = None,
    purge_size: int = 0,
    embargo_size: int = 0,
) -> tuple[WalkForwardWindow, ...]:
    """Convenience wrapper for expanding anchored training windows."""

    return walk_forward_splits(
        n_samples,
        train_size=train_size,
        validation_size=validation_size,
        out_of_sample_size=out_of_sample_size,
        step_size=step_size,
        anchored=True,
        purge_size=purge_size,
        embargo_size=embargo_size,
    )
