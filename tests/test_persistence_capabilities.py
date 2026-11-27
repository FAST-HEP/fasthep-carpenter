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
    assert a == b


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


# -- cutflow -----------------------------------------------------------------


def _cutflow_round_trip(value: dict[str, Any], tmp_path: Path) -> dict[str, Any]:
    from fasthep_carpenter.products.persistence import (  # noqa: PLC0415
        load_cutflow,
        save_cutflow,
    )

    outcome = save_cutflow(value, root=tmp_path, params={}, identity=IDENTITY)
    paths = [tmp_path / payload.path for payload in outcome.payloads]
    return load_cutflow(tmp_path, paths, params={}, variant=None, locator=None)


def test_cutflow_round_trip_keeps_order_and_number_kinds(tmp_path: Path) -> None:
    value = {
        "dataset": "Zmumu",
        "cuts": [
            {"name": "b", "n_in": 10, "n_out": 5, "sumw_out": 4.5},
            {"name": "a", "n_in": 5, "n_out": 5, "sumw_out": 5.0},
        ],
    }

    back = _cutflow_round_trip(value, tmp_path)

    assert back == value
    assert [row["name"] for row in back["cuts"]] == ["b", "a"]
    assert isinstance(back["cuts"][0]["n_in"], int)
    assert isinstance(back["cuts"][1]["sumw_out"], float)


def test_cutflow_non_finite_floats_are_refused(tmp_path: Path) -> None:
    value = {"cuts": [{"name": "x", "sumw": math.nan}]}

    with pytest.raises(ValueError, match="cutflow"):
        _cutflow_round_trip(value, tmp_path)


def test_cutflow_numpy_scalars_are_stored_as_numbers(tmp_path: Path) -> None:
    value = {"cuts": [{"name": "x", "n": np.int64(3), "sumw": np.float64(1.5)}]}

    back = _cutflow_round_trip(value, tmp_path)

    assert back == {"cuts": [{"name": "x", "n": 3, "sumw": 1.5}]}


def test_cutflow_rejects_values_that_are_not_data(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cutflow"):
        _cutflow_round_trip({"cuts": [object()]}, tmp_path)


def test_cutflow_must_be_a_dict(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="dict"):
        _cutflow_round_trip([1, 2], tmp_path)  # type: ignore[arg-type]


# -- registry and Flow ---------------------------------------------------------


def _carpenter_registry() -> Any:
    from importlib import resources  # noqa: PLC0415

    import yaml  # noqa: PLC0415
    from hepflow.registry.loaders import resolve_runtime_registry  # noqa: PLC0415

    text = (
        resources.files("fasthep_carpenter.profiles")
        .joinpath("registry.yaml")
        .read_text(encoding="utf-8")
    )
    return resolve_runtime_registry(yaml.safe_load(text)["registry"])


def test_registry_declares_both_capabilities() -> None:
    handlers = _carpenter_registry().product_handlers

    histogram = handlers["histogram"].persistence["histogram.data"]
    cutflow = handlers["cutflow"].persistence["cutflow.json"]
    assert (histogram.version, histogram.container) == ("1", "json")
    assert (cutflow.version, cutflow.container) == ("1", "json")
    assert histogram.kind == "histogram"
    assert cutflow.kind == "cutflow"
    for capability in (histogram, cutflow):
        assert callable(capability.save)
        assert callable(capability.load)
        assert capability.payload_roles == ("data",)
        assert capability.fidelity


def test_persist_and_restore_through_flow(tmp_path: Path) -> None:
    from hepflow.api import restore_products  # noqa: PLC0415
    from hepflow.model.persistence import PersistenceSettings  # noqa: PLC0415
    from hepflow.model.plan import ExecutionPlan  # noqa: PLC0415
    from hepflow.model.products import BoundaryProduct  # noqa: PLC0415
    from hepflow.runtime.persistence.save import (  # noqa: PLC0415
        persist_boundary_products,
    )
    from hepflow.runtime.persistence.select import (  # noqa: PLC0415
        PersistenceSelection,
    )

    registry = _carpenter_registry()
    h: hist.Hist = hist.Hist(
        hist.axis.Regular(3, 0, 3, name="x"), storage=hist.storage.Weight()
    )
    h.fill(x=[0.5, 1.5, 7.0], weight=[1.0, 2.0, 3.0])
    product = BoundaryProduct(
        node_id="stage.H",
        output_name="hist",
        kind="histogram",
        dataset="Zmumu",
        partition_id="p0",
        representation="persist",
        value=h,
    )
    capability = registry.product_handlers["histogram"].persistence["histogram.data"]

    [saved] = persist_boundary_products(
        [product],
        PersistenceSelection(persisted={product.key(): capability}),
        plan=ExecutionPlan(),
        settings=PersistenceSettings(mode="all"),
        outdir=tmp_path,
    )
    restored = restore_products(saved.metadata["persisted"], registry=registry)

    [back] = restored.products.values()
    _same(h, back)


# -- review fixes -----------------------------------------------------------------


def _carpenter_histogram() -> hist.Hist:
    from fasthep_carpenter.operations.hist import _make_hist  # noqa: PLC0415

    return _make_hist(
        [
            {"type": "category", "name": "dataset"},
            {"type": "int", "name": "njets"},
            {"type": "bool", "name": "flag"},
            {"type": "regular", "name": "x", "bins": {"nbins": 4, "low": 0, "high": 4}},
        ],
        "weighted",
    )


def test_a_restored_histogram_equals_and_merges_with_a_fresh_one(
    tmp_path: Path,
) -> None:
    from fasthep_carpenter.products import merge_histogram_products  # noqa: PLC0415

    h = _carpenter_histogram()
    h.fill(dataset=["a"], njets=[2], flag=[1], x=[1.5], weight=[2.0])

    back = _round_trip(h, tmp_path)

    assert h == back
    merged = merge_histogram_products([h, back], node=None, output_name="hist")
    assert merged.values(flow=True).sum() == 2 * h.values(flow=True).sum()


def test_an_empty_histogram_round_trips(tmp_path: Path) -> None:
    h = _carpenter_histogram()

    back = _round_trip(h, tmp_path)

    assert h == back
    assert back.view(flow=True).shape == h.view(flow=True).shape


def test_regular_axis_with_a_transform_is_refused(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(
        hist.axis.Regular(4, 1, 1000, name="x", transform=hist.axis.transform.log)
    )

    with pytest.raises(ValueError, match="transform"):
        save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)


def test_circular_axis_is_refused(tmp_path: Path) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Regular(4, 0, 1, name="x", circular=True))

    with pytest.raises(ValueError, match="circular"):
        save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)


def test_histogram_name_label_and_axis_attributes_survive(tmp_path: Path) -> None:
    axis = hist.axis.Regular(2, 0, 2, name="x")
    axis.note = "kept"
    h: hist.Hist = hist.Hist(axis, name="hname", label="hlabel")

    back = _round_trip(h, tmp_path)

    assert (back.name, back.label) == ("hname", "hlabel")
    assert back.axes[0].note == "kept"
    assert h == back


@pytest.mark.parametrize(
    "value",
    [
        {1: "a"},
        {"cuts": (1, 2)},
        {"cuts": [{"a": 1}, {1: 2}]},
    ],
)
def test_cutflow_refuses_what_json_would_change(tmp_path: Path, value: Any) -> None:
    with pytest.raises(ValueError, match="cutflow"):
        _cutflow_round_trip(value, tmp_path)


@pytest.mark.parametrize("metadata", [{"a": (1, 2)}, {1: "x"}, {"a": math.nan}])
def test_metadata_json_would_change_is_refused(tmp_path: Path, metadata: Any) -> None:
    h: hist.Hist = hist.Hist(hist.axis.Regular(2, 0, 2, name="x"))
    h.metadata = metadata

    with pytest.raises(ValueError, match="metadata"):
        save_histogram(h, root=tmp_path, params={}, identity=IDENTITY)
