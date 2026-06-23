from __future__ import annotations

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
        "description": "A written ROOT file containing a TTree.",
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
    compression: str = "zlib",
    compression_level: int = 1,
    mode: str = "recreate",
    ctx: dict[str, Any] | None = None,
    meta: dict[str, Any] | None = None,
) -> OutputResult:
    """
    Write an event-like stream to a ROOT TTree.

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
        fout[tree] = payload

    entries = _safe_len(array)
    manifest_record = _manifest_record(
        output_path=output_path,
        entries=entries,
        tree=tree,
        ctx=dict(ctx or {}),
        meta=dict(meta or {}),
    )

    return OutputResult(
        kind="artifact",
        path=str(output_path),
        format="root",
        metadata={
            "tree": tree,
            "entries": entries,
            "branches": list(array.fields),
            "compression": compression,
            "compression_level": compression_level,
            "writer_manifest": manifest_record,
        },
    )


def _manifest_record(
    *,
    output_path: Path,
    entries: int | None,
    tree: str,
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
        "path": path,
        "path_type": path_type,
        "dataset": str(ctx.get("dataset_name") or partition.get("dataset") or "dataset"),
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


def _safe_len(array: ak.Array) -> int | None:
    try:
        return len(array)
    except Exception:
        return None
