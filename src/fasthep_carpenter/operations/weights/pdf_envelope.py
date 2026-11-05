from __future__ import annotations

from typing import Any

import awkward as ak
from hepflow.model.data_flow import DataDependencyResult

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
    events: ak.Array, params: dict[str, Any], ctx: dict[str, Any]
) -> ak.Array:
    del ctx
    return apply_pdf_envelope(
        events,
        inputs=params["inputs"],
        outputs=dict(params["outputs"]),
    )


def run_pdf_envelope_transform(
    *,
    stream: Any,
    inputs: str,
    outputs: dict[str, str],
    ctx: dict[str, Any] | None = None,
    **kwargs: Any,
) -> ak.Array:
    return run_pdf_envelope(
        stream,
        {"inputs": inputs, "outputs": outputs},
        dict(ctx or {}),
    )


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
