from __future__ import annotations

from typing import Any

import awkward as ak
from hepflow.model.data_flow import DataDependencyResult
from hepflow.model.defaults import DEFAULT_PRIMARY_STREAM_ID

from fasthep_carpenter.runtime.compat import (
    legacy_data_envelope,
    unwrap_legacy_data_envelope,
)
from fasthep_carpenter.runtime.stream_readers import get_stream_array

PDF_ENVELOPE_SPEC = {
    "name": "hep.weights.pdf_envelope",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "inputs": {"type": "string", "required": True},
        "outputs": {"type": "mapping", "required": True},
    },
    "result": {
        "kind": "event_stream",
        "description": "Event stream with PDF envelope weight fields added.",
    },
    "requires": {
        "symbols": [
            {"from": "params.inputs", "kind": "expr_or_field"},
        ]
    },
    "provides": {
        "symbols": [
            {"from": "params.outputs.*", "kind": "field_list"},
        ]
    },
}


def parse_pdf_envelope_dependencies(
    params: dict[str, Any],
    **_: Any,
) -> DataDependencyResult:
    result = DataDependencyResult()
    inputs = params.get("inputs")
    if isinstance(inputs, str) and inputs:
        result.consumes.add(inputs)

    outputs = params.get("outputs") or {}
    if isinstance(outputs, dict):
        for output in outputs.values():
            if isinstance(output, str) and output:
                result.produces.add(output)

    return result


def run_pdf_envelope(
    data: dict[str, Any], params: dict[str, Any], ctx: dict[str, Any]
) -> dict[str, Any]:
    events = get_stream_array(
        data, ctx.get("primary_stream", DEFAULT_PRIMARY_STREAM_ID)
    )
    out = apply_pdf_envelope(
        events,
        inputs=params["inputs"],
        outputs=dict(params["outputs"]),
    )
    return {DEFAULT_PRIMARY_STREAM_ID: out}


def run_pdf_envelope_transform(
    *,
    stream: Any,
    inputs: str,
    outputs: dict[str, str],
    ctx: dict[str, Any] | None = None,
    **kwargs: Any,
) -> dict[str, ak.Array]:
    stream = unwrap_legacy_data_envelope(stream)
    legacy_data = legacy_data_envelope(stream)
    out = run_pdf_envelope(
        data=legacy_data,
        params={"inputs": inputs, "outputs": outputs},
        ctx=dict(ctx or {}),
        **kwargs,
    )
    return {"events": get_stream_array(out, DEFAULT_PRIMARY_STREAM_ID)}


def apply_pdf_envelope(
    events: ak.Array, *, inputs: str, outputs: dict[str, str]
) -> ak.Array:
    """
    Add per-event PDF envelope fields.

    First-pass assumptions:
    - ``inputs`` names an array-like field containing PDF weights per event.
    - ``outputs.up`` receives ``max(inputs)`` per event.
    - ``outputs.down`` receives ``min(inputs)`` per event.
    """
    weights = events[inputs]
    out = events

    up = outputs.get("up")
    if up:
        out = ak.with_field(out, ak.max(weights, axis=1), up)

    down = outputs.get("down")
    if down:
        out = ak.with_field(out, ak.min(weights, axis=1), down)

    return out
