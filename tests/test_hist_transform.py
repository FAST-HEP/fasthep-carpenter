from __future__ import annotations

import awkward as ak
import pytest

from fasthep_carpenter.operations.hist import run_make_hist


def test_hist_fills_jagged_values_without_weights() -> None:
    h = _hist(
        ak.Array({"Muon_Pt": [[10, 20], [30], [], [40, 50, 60]]}),
        storage="count",
    )

    assert h.values().sum() == 6


def test_hist_broadcasts_event_weights_to_jagged_values() -> None:
    h = _hist(
        ak.Array(
            {
                "Muon_Pt": [[10, 20], [30], [], [40, 50, 60]],
                "EventWeight": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        storage="weighted",
        weight_expr="EventWeight",
    )

    assert h.values().sum() == 16.0


def test_hist_accepts_matching_jagged_weights() -> None:
    h = _hist(
        ak.Array(
            {
                "Muon_Pt": [[10, 20], [30], [], [40, 50, 60]],
                "ObjectWeight": [[1.0, 1.1], [2.0], [], [4.0, 4.1, 4.2]],
            }
        ),
        storage="weighted",
        weight_expr="ObjectWeight",
    )

    assert h.values().sum() == pytest.approx(16.4)


def test_hist_rejects_incompatible_jagged_weights() -> None:
    with pytest.raises(ValueError, match="could not broadcast weight expression"):
        _hist(
            ak.Array(
                {
                    "Muon_Pt": [[10, 20], [30], [], [40, 50, 60]],
                    "BadWeight": [[1.0], [2.0], [], [4.0, 4.1]],
                }
            ),
            storage="weighted",
            weight_expr="BadWeight",
        )


def test_hist_flat_values_with_flat_weights_remains_unchanged() -> None:
    h = _hist(
        ak.Array({"Muon_Pt": [10, 20, 30], "EventWeight": [1.0, 2.0, 3.0]}),
        storage="weighted",
        weight_expr="EventWeight",
    )

    assert h.values().sum() == 6.0


def _hist(events: ak.Array, *, storage: str, weight_expr: str | None = None):
    params = {
        "axes": [
            {
                "name": "pt",
                "type": "regular",
                "source": "Muon_Pt",
                "bins": {"nbins": 10, "low": 0, "high": 100},
            }
        ],
        "storage": storage,
    }
    if weight_expr is not None:
        params["weight_expr"] = weight_expr

    return run_make_hist(
        data={"events": events},
        params=params,
        ctx={"primary_stream": "events", "dataset_name": "sample"},
    )["hist"]
