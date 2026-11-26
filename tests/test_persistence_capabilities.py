from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import hist
import numpy as np
import pytest

from fasthep_carpenter.products.persistence import (
    HISTOGRAM_FORMAT_VERSION,
    load_histogram,
    save_histogram,
)

IDENTITY: dict[str, Any] = {"producer_node": "stage.H", "output_name": "hist"}


def _round_trip(h: hist.Hist, tmp_path: Path) -> hist.Hist:
    outcome = save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)
    paths = [tmp_path / payload.path for payload in outcome.payloads]
    return load_histogram(tmp_path, paths, params={}, variant=None, locator=None)


def _same(a: hist.Hist, b: hist.Hist) -> None:
    assert [type(x) for x in a.axes] == [type(x) for x in b.axes]
    for left, right in zip(a.axes, b.axes, strict=True):
        assert (left.name, left.label) == (right.name, right.label)
        assert left.traits == right.traits
        assert list(left) == list(right)
    assert a.storage_type is b.storage_type
    np.testing.assert_array_equal(a.view(flow=True), b.view(flow=True))


def test_round_trip_regular_double(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Regular(5, 0.0, 10.0, name="x", label="X [GeV]"))
    h.fill(x=[1.0, 1.0, 3.5, 9.9])

    _same(h, _round_trip(h, tmp_path))


def test_round_trip_weight_keeps_variances(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(
        hist.axis.Regular(4, 0, 4, name="x"), storage=hist.storage.Weight()
    )
    h.fill(x=[0.5, 0.5, 2.5], weight=[0.5, 2.0, 3.0])

    back = _round_trip(h, tmp_path)

    _same(h, back)
    np.testing.assert_array_equal(h.variances(flow=True), back.variances(flow=True))


def test_flow_bins_are_kept(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Regular(2, 0, 2, name="x"))
    h.fill(x=[-5.0, -1.0, 0.5, 7.0, 8.0, 9.0])

    back = _round_trip(h, tmp_path)

    assert list(back.values(flow=True)) == [2.0, 1.0, 0.0, 3.0]
    _same(h, back)


def test_category_axes_round_trip(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(
        hist.axis.StrCategory([], growth=True, name="channel"),
        hist.axis.StrCategory(["a", "b"], name="fixed"),
        hist.axis.IntCategory([], growth=True, name="njets"),
        hist.axis.IntCategory([0, 1], name="flag"),
        hist.axis.Regular(3, 0, 3, name="x"),
    )
    h.fill(
        channel=["mu", "el", "mu"],
        fixed=["a", "b", "a"],
        njets=[2, 3, 2],
        flag=[0, 1, 1],
        x=[0.5, 1.5, 2.5],
    )

    back = _round_trip(h, tmp_path)

    _same(h, back)
    assert list(back.axes[0]) == ["mu", "el"]


def test_axis_names_labels_and_growth_survive(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(
        hist.axis.Regular(3, 0, 3, name="x", label="x label", growth=True)
    )
    h.fill(x=[10.0])

    back = _round_trip(h, tmp_path)

    assert back.axes[0].label == "x label"
    assert back.axes[0].traits.growth
    _same(h, back)


def test_variable_axis_is_refused(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Variable([0, 1, 5], name="x"))

    with pytest.raises(ValueError, match="Variable"):
        save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)


def test_unsupported_storage_is_refused(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(
        hist.axis.Regular(2, 0, 2, name="x"), storage=hist.storage.Int64()
    )

    with pytest.raises(ValueError, match="storage"):
        save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)


def test_non_finite_values_round_trip(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Regular(3, 0, 3, name="x"))
    h.view(flow=True)[...] = [0.0, math.nan, math.inf, -math.inf, 2.0]

    back = _round_trip(h, tmp_path)

    np.testing.assert_array_equal(h.view(flow=True), back.view(flow=True))


def test_payload_is_plain_json_no_pickle(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Regular(2, 0, 2, name="x"))
    save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)

    data = json.loads((tmp_path / "hist.json").read_text(encoding="utf-8"))

    assert data["format_version"] == HISTOGRAM_FORMAT_VERSION == "1"
    assert data["storage"] == "double"
    assert data["axes"][0]["type"] == "regular"


def test_outcome_names_the_payload(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Regular(2, 0, 2, name="x"))

    outcome = save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)

    assert [(p.path, p.role) for p in outcome.payloads] == [("hist.json", "data")]
