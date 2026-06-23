from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import awkward as ak
import numpy as np
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID
from hepflow.runtime.stream_readers import get_stream_array

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)

LOOKUP_CSV_SPEC = {
    "name": "hep.weights.lookup_csv",
    "kind": "transform",
    "version": "1.0",
    "dependencies": {
        "parser": "fasthep_carpenter.operations.weights.lookup_csv:parse_lookup_csv_dependencies",
    },
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "path": {"type": "string", "required": True},
        "variable": {"type": "string", "required": True},
        "bins": {"type": "mapping", "required": False},
        "values": {"type": "mapping", "required": True},
        "outputs": {"type": "mapping", "required": True},
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream with CSV lookup weight fields added.",
    },
    "requires": {
        "symbols": [
            {"from": "params.variable", "kind": "expr_or_field"},
        ]
    },
    "provides": {
        "symbols": [
            {"from": "params.outputs.*", "kind": "field_list"},
        ]
    },
}


def parse_lookup_csv_dependencies(
    params: dict[str, Any],
    **_: Any,
) -> DataDependencyResult:
    result = DataDependencyResult()
    variable = params.get("variable")
    if isinstance(variable, str) and variable:
        result.consumes.add(variable)

    outputs = params.get("outputs") or {}
    if isinstance(outputs, dict):
        for output in outputs.values():
            if isinstance(output, str) and output:
                result.produces.add(output)

    return result


def run_lookup_csv(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    events = get_stream_array(
        data, ctx.get("primary_stream", DEFAULT_PRIMARY_STREAM_ID)
    )
    out = apply_lookup_csv(
        events,
        path=params["path"],
        variable=params["variable"],
        bins=dict(params.get("bins") or {}),
        values=dict(params["values"]),
        outputs=dict(params["outputs"]),
    )
    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_lookup_csv_transform(
    *,
    stream: Any,
    path: str,
    variable: str,
    values: dict[str, str],
    outputs: dict[str, str],
    bins: dict[str, str] | None = None,
    ctx: dict[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, ak.Array]:
    stream = unwrap_legacy_data_envelope(stream)
    legacy_data = legacy_data_envelope(stream)
    out = run_lookup_csv(
        data=legacy_data,
        params={
            "path": path,
            "variable": variable,
            "bins": dict(bins or {}),
            "values": values,
            "outputs": outputs,
        },
        ctx=dict(ctx or {}),
        **kwargs,
    )
    return {"events": get_stream_array(out, DEFAULT_PRIMARY_STREAM_ID)}


def apply_lookup_csv(
    events: ak.Array,
    *,
    path: str,
    variable: str,
    bins: dict[str, str],
    values: dict[str, str],
    outputs: dict[str, str],
) -> ak.Array:
    """
    Add lookup weights from a one-dimensional CSV bin table.

    First-pass assumptions:
    - ``variable`` names one scalar numeric event field.
    - bins are half-open ``lower <= value < upper`` ranges.
    - bin column names default to ``pt_min``/``pt_max``; if ``bins.column`` is
      provided, ``<column>_min``/``<column>_max`` are used.
    - ``values`` maps labels such as ``nominal``/``up``/``down`` to CSV columns.
    - ``outputs`` maps those same labels to event field names.
    """
    table = _load_lookup_table(path, bins=bins, value_columns=values)
    variable_values = _flat_numeric_event_field(events, variable)

    out = events
    for label, output_name in outputs.items():
        value_column = values.get(label)
        if value_column is None:
            raise ValueError(f"outputs.{label} has no matching values entry")
        looked_up = _lookup_values(variable_values, table, value_column)
        out = ak.with_field(out, ak.Array(looked_up), output_name)

    return out


def _load_lookup_table(
    path: str, *, bins: dict[str, str], value_columns: dict[str, str]
) -> dict[str, np.ndarray]:
    lower_column, upper_column = _bin_columns(bins)
    required = {lower_column, upper_column, *value_columns.values()}

    rows: list[dict[str, str]] = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV lookup table missing columns: {sorted(missing)}")
        rows.extend(reader)

    if not rows:
        raise ValueError("CSV lookup table has no rows")

    table: dict[str, np.ndarray] = {}
    for column in required:
        table[column] = np.asarray([float(row[column]) for row in rows], dtype=float)
    table["__lower__"] = table[lower_column]
    table["__upper__"] = table[upper_column]
    return table


def _bin_columns(bins: dict[str, str]) -> tuple[str, str]:
    lower = bins.get("lower") or bins.get("min")
    upper = bins.get("upper") or bins.get("max")
    if lower and upper:
        return str(lower), str(upper)

    column = bins.get("column")
    if column:
        return f"{column}_min", f"{column}_max"

    return "pt_min", "pt_max"


def _flat_numeric_event_field(events: ak.Array, field: str) -> np.ndarray:
    values = events[field]
    try:
        return np.asarray(ak.to_numpy(values), dtype=float)
    except Exception as exc:
        raise ValueError(
            f"lookup_csv variable {field!r} must be one scalar numeric value per event"
        ) from exc


def _lookup_values(
    variable_values: np.ndarray, table: dict[str, np.ndarray], value_column: str
) -> np.ndarray:
    lower = table["__lower__"]
    upper = table["__upper__"]
    values = table[value_column]

    output = np.empty(len(variable_values), dtype=float)
    for index, value in enumerate(variable_values):
        matches = np.nonzero((lower <= value) & (value < upper))[0]
        if len(matches) == 0:
            raise ValueError(f"No CSV lookup bin matched value {value}")
        output[index] = values[int(matches[0])]
    return output
