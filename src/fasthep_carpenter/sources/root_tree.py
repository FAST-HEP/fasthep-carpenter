from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import awkward as ak
import uproot

ROOT_TREE_SOURCE_SPEC = {
    "name": "root_tree",
    "kind": "source",
    "version": "1.0",
    "input": None,
    "params": {
        "datasets": {"type": "list[mapping]", "required": True},
        "defaults": {"type": "mapping", "required": False, "default": {}},
        "tree": {"type": "string", "required": True},
        "stream_type": {
            "type": "string",
            "required": False,
            "default": "event_stream",
        },
        "branches": {"type": "list[string]", "required": False, "default": None},
        "missing_branches": {
            "type": "string",
            "required": False,
            "default": "error",
            "allowed": ["error", "ignore"],
        },
        "start": {"type": "integer", "required": False, "default": None},
        "stop": {"type": "integer", "required": False, "default": None},
        "metadata_only": {"type": "boolean", "required": False, "default": False},
    },
    "result": {
        "kind": "event_stream",
        "description": "Loaded ROOT tree event stream.",
    },
}


@dataclass(frozen=True, slots=True)
class RootTreeSchema:
    """Metadata-only ROOT tree schema descriptor.

    This intentionally carries branch metadata without materialising event arrays.
    Curator's schema observer consumes it via duck typing so Flow can inspect
    remote ROOT schemas without forcing ``TTree.arrays()``.
    """

    fields: list[str]
    awkward_type: dict[str, str]
    entry_count: int | None = None


def run_root_tree_source(
    *,
    datasets: list[dict[str, Any]],
    defaults: dict[str, Any] | None = None,
    tree: str,
    stream_type: str | None = None,
    branches: list[str] | None = None,
    missing_branches: str = "error",
    start: int | None = None,
    stop: int | None = None,
    metadata_only: bool = False,
    ctx: dict[str, Any] | None = None,
) -> ak.Array | RootTreeSchema | dict[str, ak.Array | RootTreeSchema]:
    """
    Load ROOT TTrees for each dataset and return a dataset-keyed event stream.

    Current representation:
      {dataset_name: awkward.Array}
    """
    del stream_type
    defaults = dict(defaults or {})
    ctx = dict(ctx or {})
    partition = ctx.get("partition")

    if partition is not None:
        partition_start = partition.get("start")
        partition_stop = partition.get("stop")
        return _read_one_file(
            str(partition["file"]),
            tree=tree,
            branches=branches,
            missing_branches=missing_branches,
            start=start if partition_start is None else partition_start,
            stop=stop if partition_stop is None else partition_stop,
            metadata_only=metadata_only,
        )

    out: dict[str, ak.Array | RootTreeSchema] = {}

    for ds in datasets:
        name = str(ds["name"])
        files = list(ds.get("files") or [])
        if not files:
            raise ValueError(f"Dataset '{name}' has no files")

        arrays = [
            _read_one_file(
                path,
                tree=tree,
                branches=branches,
                missing_branches=missing_branches,
                start=start,
                stop=stop,
                metadata_only=metadata_only,
            )
            for path in files
        ]

        if metadata_only:
            merged = arrays[0]
        else:
            merged = arrays[0] if len(arrays) == 1 else ak.concatenate(arrays, axis=0)

        out[name] = merged

    return out


def _read_one_file(
    path: str,
    *,
    tree: str,
    branches: list[str] | None,
    missing_branches: str,
    start: int | None,
    stop: int | None,
    metadata_only: bool = False,
) -> ak.Array | RootTreeSchema:
    file_path = Path(path)
    if not _is_remote_uri(path) and not file_path.exists():
        raise FileNotFoundError(f"ROOT input file does not exist: {path}")

    with uproot.open(path if _is_remote_uri(path) else file_path) as fin:
        try:
            t = fin[tree]
        except KeyError as exc:
            raise KeyError(f"Tree '{tree}' not found in ROOT file: {path}") from exc

        missing_policy = _missing_branch_policy(missing_branches)
        if branches and missing_policy == "ignore":
            available = set(_tree_keys(t))
            branches = [str(branch) for branch in branches if str(branch) in available]

        if metadata_only:
            return _inspect_tree_schema(t, branches=branches, start=start, stop=stop)

        if not branches:
            return t.arrays(
                entry_start=start,
                entry_stop=stop,
                library="ak",
            )

        safe: list[str] = []
        opaque: list[str] = []

        for b in branches:
            if _is_opaque_for_uproot_arrays(b):
                opaque.append(b)
            else:
                safe.append(b)

        out: dict[str, Any] = {}

        if safe:
            arrs = t.arrays(
                safe,
                entry_start=start,
                entry_stop=stop,
                library="ak",
            )
            for k in arrs.fields:
                out[k] = arrs[k]

        for b in opaque:
            out[b] = t[b].array(
                entry_start=start,
                entry_stop=stop,
                library="ak",
            )

        return ak.zip(out, depth_limit=1)


def _inspect_tree_schema(
    tree: Any,
    *,
    branches: list[str] | None,
    start: int | None,
    stop: int | None,
) -> RootTreeSchema:
    fields = [str(field) for field in _tree_keys(tree)]
    if branches:
        requested = [str(branch) for branch in branches]
        missing = [branch for branch in requested if branch not in fields]
        if missing:
            raise KeyError(f"Branches not found in ROOT tree: {missing}")
        fields = requested

    typenames = _tree_typenames(tree)
    interpretations = _tree_interpretations(tree)
    awkward_type = {
        field: _field_schema_type(field, typenames=typenames, interpretations=interpretations)
        for field in fields
    }
    return RootTreeSchema(
        fields=fields,
        awkward_type=awkward_type,
        entry_count=_selected_entry_count(tree, start=start, stop=stop),
    )


def _tree_keys(tree: Any) -> list[str]:
    keys = tree.keys() if callable(getattr(tree, "keys", None)) else []
    return list(keys)


def _missing_branch_policy(value: str) -> str:
    policy = str(value)
    if policy not in {"error", "ignore"}:
        raise ValueError(
            "root_tree missing_branches must be one of ['error', 'ignore'], "
            f"got {policy!r}"
        )
    return policy


def _tree_typenames(tree: Any) -> dict[str, Any]:
    typenames = getattr(tree, "typenames", None)
    if callable(typenames):
        return dict(typenames())
    if isinstance(typenames, dict):
        return dict(typenames)
    return {}


def _tree_interpretations(tree: Any) -> dict[str, Any]:
    interpretations = getattr(tree, "interpretations", None)
    if callable(interpretations):
        return dict(interpretations())
    if isinstance(interpretations, dict):
        return dict(interpretations)
    return {}


def _field_schema_type(
    field: str,
    *,
    typenames: dict[str, Any],
    interpretations: dict[str, Any],
) -> str:
    if field in typenames:
        return str(typenames[field])
    if field in interpretations:
        return str(interpretations[field])
    return "unknown"


def _selected_entry_count(
    tree: Any,
    *,
    start: int | None,
    stop: int | None,
) -> int | None:
    num_entries = getattr(tree, "num_entries", None)
    if num_entries is None:
        return None
    try:
        total = int(num_entries)
    except Exception:
        return None
    selected_start = max(0, int(start or 0))
    selected_stop = total if stop is None else min(total, int(stop))
    return max(0, selected_stop - selected_start)


def _is_opaque_for_uproot_arrays(expr: str) -> bool:
    """
    uproot's TTree.arrays(expressions=[...]) uses an expression parser.
    Some branch names are not valid expressions and must be read directly
    via TBranch access.
    """
    s = str(expr)
    return ("./" in s) or ("/" in s and "." in s)


def _is_remote_uri(path: str) -> bool:
    return bool(urlparse(path).scheme)
