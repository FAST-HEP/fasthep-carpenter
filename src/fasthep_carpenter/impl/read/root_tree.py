from __future__ import annotations

from pathlib import Path
from typing import Any

import awkward as ak
import uproot


def run_root_tree_source(
    *,
    datasets: list[dict[str, Any]],
    defaults: dict[str, Any] | None = None,
    tree: str,
    branches: list[str] | None = None,
    start: int | None = None,
    stop: int | None = None,
    ctx: dict[str, Any] | None = None,
) -> ak.Array | dict[str, ak.Array]:
    """
    Load ROOT TTrees for each dataset and return a dataset-keyed event stream.

    Current representation:
      {dataset_name: awkward.Array}
    """
    defaults = dict(defaults or {})
    ctx = dict(ctx or {})
    partition = ctx.get("partition")

    if partition is not None:
        return _read_one_file(
            str(partition["file"]),
            tree=tree,
            branches=branches,
            start=partition.get("start", start),
            stop=partition.get("stop", stop),
        )

    out: dict[str, ak.Array] = {}

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
                start=start,
                stop=stop,
            )
            for path in files
        ]

        merged = arrays[0] if len(arrays) == 1 else ak.concatenate(arrays, axis=0)

        out[name] = merged

    return out


def _read_one_file(
    path: str,
    *,
    tree: str,
    branches: list[str] | None,
    start: int | None,
    stop: int | None,
) -> ak.Array:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"ROOT input file does not exist: {path}")

    with uproot.open(file_path) as fin:
        try:
            t = fin[tree]
        except KeyError as exc:
            raise KeyError(f"Tree '{tree}' not found in ROOT file: {path}") from exc

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


def _is_opaque_for_uproot_arrays(expr: str) -> bool:
    """
    uproot's TTree.arrays(expressions=[...]) uses an expression parser.
    Some branch names are not valid expressions and must be read directly
    via TBranch access.
    """
    s = str(expr)
    return ("./" in s) or ("/" in s and "." in s)
