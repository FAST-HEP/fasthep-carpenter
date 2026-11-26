"""Persistence capabilities for carpenter's products (see fasthep-flow ``persistence.md``).

Histograms are stored as data-only JSON (``histogram.data`` version 1): the axes, the
storage type, and the values and variances including flow bins. Nothing is pickled, and
an axis or storage type carpenter does not create is refused rather than approximated.
"""

from __future__ import annotations

import json
import math
import warnings
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import hist
import numpy as np
from hepflow.model.persistence import SavedPayload, SaveOutcome

HISTOGRAM_FORMAT_VERSION = "1"
HISTOGRAM_PAYLOAD = "hist.json"
CUTFLOW_FORMAT_VERSION = "1"
CUTFLOW_PAYLOAD = "cutflow.json"

_NON_FINITE = {"nan": math.nan, "inf": math.inf, "-inf": -math.inf}
_HIST_INTERNAL = {"_variance_known", "metadata"}


def save_histogram(
    value: hist.Hist,
    *,
    root: Path,
    params: dict[str, Any],
    identity: dict[str, Any],
) -> SaveOutcome:
    """Write ``value`` as ``hist.json`` below ``root``."""
    del params, identity
    storage = _storage_name(value)
    document: dict[str, Any] = {
        "format_version": HISTOGRAM_FORMAT_VERSION,
        "storage": storage,
        "axes": [_axis_to_dict(axis) for axis in value.axes],
    }
    if storage == "weight":
        view = value.view(flow=True)
        document["values"] = _encode(np.asarray(view["value"]))
        document["variances"] = _encode(np.asarray(view["variance"]))
    else:
        document["values"] = _encode(np.asarray(value.view(flow=True)))
    raw = {k: v for k, v in vars(value).items() if k not in _HIST_INTERNAL}
    document["name"] = raw.pop("name", None)
    document["label"] = raw.pop("label", None)
    if raw:
        document["attributes"] = _strict(raw, "histogram attributes")
    metadata = _histogram_metadata(value)
    if metadata is not None:
        document["metadata"] = _strict(metadata, "histogram metadata")
    root.mkdir(parents=True, exist_ok=True)
    (root / HISTOGRAM_PAYLOAD).write_text(
        json.dumps(document, sort_keys=True), encoding="utf-8"
    )
    return SaveOutcome(payloads=(SavedPayload(HISTOGRAM_PAYLOAD, role="data"),))


def load_histogram(
    root: Path,
    payloads: Sequence[Path],
    *,
    params: dict[str, Any],
    variant: str | None,
    locator: str | None,
) -> hist.Hist:
    """Rebuild the histogram from the ``hist.json`` payload."""
    del root, params, variant, locator
    document = json.loads(Path(payloads[0]).read_text(encoding="utf-8"))
    if document.get("format_version") != HISTOGRAM_FORMAT_VERSION:
        raise ValueError(
            f"histogram.data format_version {document.get('format_version')!r} is not "
            f"{HISTOGRAM_FORMAT_VERSION!r}"
        )
    axes = [_axis_from_dict(item) for item in document["axes"]]
    storage = document["storage"]
    options = {"name": document.get("name"), "label": document.get("label")}
    h: hist.Hist
    if storage == "weight":
        h = hist.Hist(*axes, storage=hist.storage.Weight(), **options)
        view = h.view(flow=True)
        view["value"] = _decode(document["values"], view.shape)
        view["variance"] = _decode(document["variances"], view.shape)
    elif storage == "double":
        h = hist.Hist(*axes, storage=hist.storage.Double(), **options)
        view = h.view(flow=True)
        view[...] = _decode(document["values"], view.shape)
    else:
        raise ValueError(f"histogram.data storage {storage!r} is not supported")
    for key, item in document.get("attributes", {}).items():
        setattr(h, key, item)
    if "metadata" in document:
        h.metadata = document["metadata"]
    return h


def _storage_name(value: hist.Hist) -> str:
    storage = value.storage_type
    if storage is hist.storage.Double:
        return "double"
    if storage is hist.storage.Weight:
        return "weight"
    raise ValueError(
        f"histogram.data supports Double and Weight storage, not {storage.__name__}"
    )


def _axis_to_dict(axis: Any) -> dict[str, Any]:
    kind = type(axis).__name__
    traits = axis.traits
    if traits.circular:
        raise ValueError(
            f"histogram.data does not store circular axes (axis {axis.name!r})"
        )
    raw = dict(vars(axis))
    common: dict[str, Any] = {
        "name": axis.name,
        "label": raw.pop("label", ""),  # raw: ``axis.label`` falls back to the name
        "underflow": bool(traits.underflow),
        "overflow": bool(traits.overflow),
        "growth": bool(traits.growth),
    }
    raw.pop("name", None)
    if raw:
        common["attributes"] = _strict(raw, f"axis {axis.name!r} attributes")
    if axis.metadata is not None:
        common["metadata"] = _strict(axis.metadata, f"axis {axis.name!r} metadata")
    if kind == "Regular":
        if getattr(axis, "transform", None) is not None:
            raise ValueError(
                f"histogram.data does not store a Regular axis with a transform "
                f"(axis {axis.name!r})"
            )
        edges = axis.edges
        return {
            **common,
            "type": "regular",
            "bins": int(axis.size),
            "start": float(edges[0]),
            "stop": float(edges[-1]),
        }
    if kind == "StrCategory":
        return {**common, "type": "str_category", "categories": [str(c) for c in axis]}
    if kind == "IntCategory":
        return {**common, "type": "int_category", "categories": [int(c) for c in axis]}
    raise ValueError(
        f"histogram.data supports Regular, StrCategory and IntCategory axes, not "
        f"{kind} (axis {axis.name!r})"
    )


def _axis_from_dict(item: dict[str, Any]) -> Any:
    options: dict[str, Any] = {
        "name": item["name"],
        "label": item["label"],
        "underflow": item["underflow"],
        "overflow": item["overflow"],
        "growth": item["growth"],
    }
    if item["type"] == "regular":
        axis: Any = hist.axis.Regular(
            item["bins"], item["start"], item["stop"], **options
        )
    elif item["type"] == "str_category":
        options.pop("underflow")
        axis = hist.axis.StrCategory(item["categories"], **options)
    elif item["type"] == "int_category":
        options.pop("underflow")
        axis = hist.axis.IntCategory(item["categories"], **options)
    else:
        raise ValueError(f"histogram.data axis type {item['type']!r} is not supported")
    for key, value in item.get("attributes", {}).items():
        setattr(axis, key, value)
    if "metadata" in item:
        axis.metadata = item["metadata"]
    return axis


def _encode(array: np.ndarray) -> Any:
    """Nested lists; non-finite floats become the strings ``nan``, ``inf``, ``-inf``."""
    nested = array.tolist()
    if np.all(np.isfinite(array)):
        return nested
    return _walk(nested, _encode_scalar)


def _decode(nested: Any, shape: tuple[int, ...]) -> np.ndarray:
    # reshape: tolist() of an empty (0, n) array is [], which loses the inner shape
    return np.asarray(_walk(nested, _decode_scalar), dtype=float).reshape(shape)


def _walk(value: Any, convert: Any) -> Any:
    if isinstance(value, list):
        return [_walk(item, convert) for item in value]
    return convert(value)


def _encode_scalar(value: float) -> Any:
    if math.isnan(value):
        return "nan"
    if math.isinf(value):
        return "inf" if value > 0 else "-inf"
    return value


def _decode_scalar(value: Any) -> Any:
    return _NON_FINITE[value] if isinstance(value, str) else value


def _histogram_metadata(value: hist.Hist) -> Any:
    # boost-histogram warns (and returns None) when no metadata was ever set
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        return value.metadata


def _strict(value: Any, where: str) -> Any:
    """``value`` if JSON stores it unchanged; otherwise refuse (no silent change)."""
    try:
        if json.loads(json.dumps(value, allow_nan=False)) == value:
            return value
    except (TypeError, ValueError):
        pass
    raise ValueError(
        f"histogram.data cannot store {where} unchanged as JSON (tuples, non-string "
        f"keys, non-finite numbers and objects are refused): {value!r}"
    )


def save_cutflow(
    value: dict[str, Any],
    *,
    root: Path,
    params: dict[str, Any],
    identity: dict[str, Any],
) -> SaveOutcome:
    """Write a cutflow dict as ``cutflow.json``, keeping key and row order."""
    del params, identity
    if not isinstance(value, dict):
        raise ValueError(f"cutflow.json stores a dict, not {type(value).__name__}")
    plain = _plain(value)
    try:
        unchanged = json.loads(json.dumps(plain, allow_nan=False)) == plain
    except (TypeError, ValueError) as exc:
        raise ValueError(f"cutflow.json cannot store this cutflow: {exc}") from exc
    if not unchanged:
        raise ValueError(
            "cutflow.json cannot store this cutflow unchanged as JSON (tuples and "
            "non-string keys are refused)"
        )
    root.mkdir(parents=True, exist_ok=True)
    (root / CUTFLOW_PAYLOAD).write_text(
        json.dumps(
            {"format_version": CUTFLOW_FORMAT_VERSION, "cutflow": plain},
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    return SaveOutcome(payloads=(SavedPayload(CUTFLOW_PAYLOAD, role="data"),))


def load_cutflow(
    root: Path,
    payloads: Sequence[Path],
    *,
    params: dict[str, Any],
    variant: str | None,
    locator: str | None,
) -> dict[str, Any]:
    """Read a cutflow dict back from ``cutflow.json``."""
    del root, params, variant, locator
    document = json.loads(Path(payloads[0]).read_text(encoding="utf-8"))
    if document.get("format_version") != CUTFLOW_FORMAT_VERSION:
        raise ValueError(
            f"cutflow.json format_version {document.get('format_version')!r} is not "
            f"{CUTFLOW_FORMAT_VERSION!r}"
        )
    cutflow = document["cutflow"]
    if not isinstance(cutflow, dict):
        raise ValueError("cutflow.json does not hold a dict")
    return cutflow


def _plain(value: Any) -> Any:
    """NumPy scalars as plain numbers, recursively; everything else as it is."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {_plain(key): _plain(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_plain(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_plain(item) for item in value)
    return value
