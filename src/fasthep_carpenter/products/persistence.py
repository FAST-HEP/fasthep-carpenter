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

_NON_FINITE = {"nan": math.nan, "inf": math.inf, "-inf": -math.inf}


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
    metadata = _histogram_metadata(value)
    if metadata is not None:
        document["metadata"] = _json_metadata(metadata, "histogram")
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
    h: hist.Hist
    if storage == "weight":
        h = hist.Hist(*axes, storage=hist.storage.Weight())
        view = h.view(flow=True)
        view["value"] = _decode(document["values"])
        view["variance"] = _decode(document["variances"])
    elif storage == "double":
        h = hist.Hist(*axes, storage=hist.storage.Double())
        h.view(flow=True)[...] = _decode(document["values"])
    else:
        raise ValueError(f"histogram.data storage {storage!r} is not supported")
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
    common: dict[str, Any] = {
        "name": axis.name,
        "label": axis.label,
        "underflow": bool(traits.underflow),
        "overflow": bool(traits.overflow),
        "growth": bool(traits.growth),
    }
    if axis.metadata is not None:
        common["metadata"] = _json_metadata(axis.metadata, f"axis {axis.name!r}")
    if kind == "Regular":
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
    if "metadata" in item:
        axis.metadata = item["metadata"]
    return axis


def _encode(array: np.ndarray) -> Any:
    """Nested lists; non-finite floats become the strings ``nan``, ``inf``, ``-inf``."""
    nested = array.tolist()
    if np.all(np.isfinite(array)):
        return nested
    return _walk(nested, _encode_scalar)


def _decode(nested: Any) -> np.ndarray:
    return np.asarray(_walk(nested, _decode_scalar), dtype=float)


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


def _json_metadata(metadata: Any, where: str) -> Any:
    try:
        json.dumps(metadata)
    except TypeError as exc:
        raise ValueError(
            f"histogram.data cannot store non-JSON metadata on {where}: {exc}"
        ) from exc
    return metadata
