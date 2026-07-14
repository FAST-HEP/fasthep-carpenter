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


def test_hist_variations_fill_mc_weights_on_one_variation_axis() -> None:
    h = _variation_hist(
        ak.Array(
            {
                "PV_npvs": [10, 20, 30],
                "weight_pu_nominal": [1.0, 1.0, 1.0],
                "weight_pu_up": [2.0, 2.0, 2.0],
                "weight_pu_down": [0.5, 0.5, 0.5],
            }
        ),
        dataset_name="dy",
        eventtype="mc",
    )

    assert list(h.axes["dataset"]) == ["dy"]
    assert list(h.axes["variation"]) == ["nominal", "up", "down"]
    assert h[{"dataset": "dy", "variation": "nominal"}].values().sum() == 3.0
    assert h[{"dataset": "dy", "variation": "up"}].values().sum() == 6.0
    assert h[{"dataset": "dy", "variation": "down"}].values().sum() == 1.5


def test_hist_variations_fill_data_nominal_with_unit_weight() -> None:
    h = _variation_hist(
        ak.Array({"PV_npvs": [10, 20, 30]}),
        dataset_name="data",
        eventtype="data",
    )

    assert list(h.axes["dataset"]) == ["data"]
    assert h[{"dataset": "data", "variation": "nominal"}].values().sum() == 3.0
    assert h[{"dataset": "data", "variation": "up"}].values().sum() == 0.0
    assert h[{"dataset": "data", "variation": "down"}].values().sum() == 0.0


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


def _variation_hist(events: ak.Array, *, dataset_name: str, eventtype: str):
    return run_make_hist(
        data={"events": events},
        params={
            "axes": [
                {
                    "name": "dataset",
                    "type": "category",
                    "source": "dataset_name",
                    "bins": None,
                },
                {
                    "name": "PV_npvs",
                    "type": "regular",
                    "source": "PV_npvs",
                    "bins": {"nbins": 10, "low": 0, "high": 100},
                },
                {
                    "name": "variation",
                    "type": "category",
                    "source": "__variation__",
                    "bins": ["nominal", "up", "down"],
                },
            ],
            "storage": "weighted",
            "variations": {
                "axis": "variation",
                "apply_to": {"eventtype": "mc"},
                "weights": {
                    "nominal": "weight_pu_nominal",
                    "up": "weight_pu_up",
                    "down": "weight_pu_down",
                },
            },
        },
        ctx={
            "primary_stream": "events",
            "dataset_name": dataset_name,
            "dataset": {"eventtype": eventtype},
        },
    )["hist"]
