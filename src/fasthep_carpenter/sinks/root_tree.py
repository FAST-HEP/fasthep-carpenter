from __future__ import annotations

from contextlib import suppress
from pathlib import Path
from typing import Any

import awkward as ak
import uproot
from hepflow.model.io import OutputResult

from fasthep_carpenter.runtime.compat import unwrap_legacy_data_envelope

ROOT_TREE_WRITE_SPEC = {
    "name": "root_tree",
    "kind": "writer",
    "version": "1.0",
    "input": {"name": "target", "kind": "event_stream", "required": True},
    "params": {
        "path": {"type": "string", "required": True},
        "tree": {"type": "string", "required": False, "default": "events"},
        "keep": {"type": "list[string]", "required": False, "default": None},
        "format": {
            "type": "string",
            "required": False,
            "default": "rntuple",
            "allowed": ["rntuple", "ttree"],
        },
        "compression": {
            "type": "string",
            "required": False,
            "default": "zlib",
            "allowed": ["zlib", "lz4", "zstd", "none"],
        },
        "compression_level": {"type": "integer", "required": False, "default": 1},
        "mode": {
            "type": "string",
            "required": False,
            "default": "recreate",
            "allowed": ["recreate"],
        },
    },
    "result": {
        "kind": "artifact",
        "description": "A written ROOT file containing an RNTuple or TTree.",
    },
    "requires": {
        "symbols": [
            {
                "from": "params.keep",
                "kind": "field_list",
            }
        ]
    },
}


def run_root_tree_write(
    target: Any,
    *,
    path: str,
    tree: str = "events",
    keep: list[str] | None = None,
    format: str = "rntuple",
    compression: str = "zlib",
    compression_level: int = 1,
    mode: str = "recreate",
    ctx: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> OutputResult:
    """
    Write an event-like stream to a ROOT RNTuple or TTree.

    Initial assumptions:
      - target is awkward.Array or dict[str, array-like]
      - fields on the awkward record correspond to output branches
      - keep, when provided, is a flat list of existing field names
    """
    if mode != "recreate":
        raise ValueError(
            f"Unsupported mode for root_tree writer: {mode!r}. "
            "Only 'recreate' is currently supported."
        )
    output_format = _normalise_format(format)

    array = _normalise_target(unwrap_legacy_data_envelope(target))

    if keep is not None:
        missing = [field for field in keep if field not in array.fields]
        if missing:
            raise KeyError(
                f"Requested branches are missing from the input stream: {missing}"
            )
        array = array[keep]

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    compression_arg = _resolve_compression(compression, compression_level)

    payload = {field: array[field] for field in array.fields}

    with uproot.recreate(output_path, compression=compression_arg) as fout:
        if output_format == "rntuple":
            fout.mkrntuple(tree, payload)
        elif output_format == "ttree":
            fout.mktree(tree, payload)

    root_classname = _root_classname(output_format)

    entries = _safe_len(array)
    manifest_record = _manifest_record(
        output_path=output_path,
        entries=entries,
        tree=tree,
        output_format=output_format,
        root_classname=root_classname,
        ctx=dict(ctx or {}),
        meta=dict(meta or {}),
    )

    runtime_ctx = dict(ctx or {})
    runtime_meta = dict(meta or {})
    result = OutputResult(
        kind="artifact",
        path=str(output_path),
        format="root",
        metadata={
            "tree": tree,
            "format": output_format,
            "root_classname": root_classname,
            "entries": entries,
            "branches": list(array.fields),
            "compression": compression,
            "compression_level": compression_level,
            "partition_context": dict(runtime_ctx.get("partition") or {}),
            "writer_manifest": manifest_record,
        },
    )
    _set_reference_field(result, "producer_node", str(runtime_meta.get("node_id") or ""))
    _set_reference_field(result, "output_name", "artifact")
    _set_reference_field(result, "dataset_name", manifest_record["dataset"])
    _set_reference_field(result, "partition_id", manifest_record.get("partition_id"))
    _set_reference_field(result, "partition_index", manifest_record["partition"])
    return result


def _manifest_record(
    *,
    output_path: Path,
    entries: int | None,
    tree: str,
    output_format: str,
    root_classname: str,
    ctx: dict[str, Any],
    meta: dict[str, Any],
) -> dict[str, Any]:
    partition = dict(ctx.get("partition") or {})
    path, path_type = manifest_path(
        output_path,
        Path(str(ctx.get("outdir") or ".")),
    )
    return {
        "kind": "root_tree",
        "name": str(meta.get("writer_name") or output_path.stem),
        "node_id": str(meta.get("node_id") or ""),
        "input_node": str(meta.get("input_node") or ""),
        "tree": tree,
        "format": output_format,
        "root_classname": root_classname,
        "path": path,
        "path_type": path_type,
        "dataset": str(ctx.get("dataset_name") or partition.get("dataset") or "dataset"),
        "partition_id": partition.get("id"),
        "partition": _partition_index(partition),
        "attempt": int(ctx.get("attempt") or 0),
        "entries": entries,
        "size_bytes": output_path.stat().st_size,
    }


def manifest_path(path: Path, outdir: Path) -> tuple[str, str]:
    resolved = path.resolve()
    resolved_outdir = outdir.resolve()
    try:
        return resolved.relative_to(resolved_outdir).as_posix(), "relative_to_outdir"
    except ValueError:
        return resolved.as_posix(), "absolute"


def _set_reference_field(result: OutputResult, name: str, value: Any) -> None:
    with suppress(AttributeError):
        setattr(result, name, value)


def _partition_index(partition: dict[str, Any]) -> int:
    part = str(partition.get("part") or "0")
    try:
        return int(part.rsplit("_", 1)[-1])
    except ValueError:
        return 0


def _normalise_target(target: Any) -> ak.Array:
    if isinstance(target, ak.Array):
        return target

    if isinstance(target, dict):
        return ak.Array(target)

    raise TypeError(
        f"root_tree writer expects an awkward.Array or dict[str, array-like], found {type(target)}"
    )


def _resolve_compression(name: str, level: int) -> Any:
    if name == "none":
        return None
    if name == "zlib":
        return uproot.ZLIB(level)
    if name == "lz4":
        return uproot.LZ4(level)
    if name == "zstd":
        return uproot.ZSTD(level)

    raise ValueError(f"Unsupported ROOT compression algorithm: {name!r}")


def _normalise_format(name: str) -> str:
    output_format = str(name).strip().lower()
    if output_format in {"rntuple", "ttree"}:
        return output_format
    raise ValueError(
        f"Unsupported ROOT output format: {name!r}. "
        "Expected one of 'rntuple' or 'ttree'."
    )


def _root_classname(output_format: str) -> str:
    if output_format == "rntuple":
        return "ROOT::RNTuple"
    return "TTree"


def _safe_len(array: ak.Array) -> int | None:
    try:
        return len(array)
    except Exception:
        return None
