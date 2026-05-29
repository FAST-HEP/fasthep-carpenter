from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from hepflow.build_layout import artifact_family_dir
from hepflow.runtime.materialize import product_id
from hepflow.utils import write_json, write_pickle


def merge_histogram_products(
    values: list[Any],
    *,
    node: Any,
    output_name: str,
    dataset_name: str | None = None,
) -> Any:
    del node, output_name, dataset_name
    it = iter(values)
    acc = next(it)
    for value in it:
        acc = acc + value
    return acc


def materialize_histogram_product(
    value: Any,
    *,
    node: Any,
    output_name: str,
    outdir: str | Path,
) -> dict[str, Any]:
    del output_name
    histograms_dir = artifact_family_dir(outdir, "histograms")
    histograms_dir.mkdir(parents=True, exist_ok=True)
    histogram_id = product_id(node)
    relative_path = Path("artifacts") / "histograms" / f"{histogram_id}.pkl"
    write_pickle(value, Path(outdir) / relative_path)
    item = {
        "id": histogram_id,
        "path": relative_path.as_posix(),
        "producer": node.id,
    }
    _update_manifest(histograms_dir, "histograms", item)
    return {"value": value, "items": [item]}


def merge_cutflow_products(
    values: list[dict[str, Any]],
    *,
    node: Any,
    output_name: str,
    dataset_name: str | None = None,
) -> dict[str, Any]:
    del node, output_name
    out_by_name: dict[str, dict[str, Any]] = {}
    for cutflow in values:
        for row in cutflow.get("cuts", []):
            name = row["name"]
            target = out_by_name.setdefault(name, _empty_cutflow_row(row))
            for field in ("n_in", "n_out"):
                target[field] += float(
                    row.get(field, row.get("sumw", row.get("n", 0)))
                )
            for field in ("n_unweighted_in", "n_unweighted_out"):
                target[field] += int(row.get(field, row.get("n", 0)))
            for field in ("sumw_in", "sumw_out"):
                target[field] += float(row.get(field, row.get("sumw", row.get("n", 0))))
            for field in ("sumw2_in", "sumw2_out"):
                target[field] += float(row.get(field, row.get("sumw2", row.get("n", 0))))
            target["n"] = target["n_out"]
            target["sumw"] = target["sumw_out"]
            target["sumw2"] = target["sumw2_out"]

    merged: dict[str, Any] = {"cuts": list(out_by_name.values())}
    if dataset_name is not None:
        merged["dataset"] = dataset_name
    return merged


def materialize_cutflow_product(
    value: Any,
    *,
    node: Any,
    output_name: str,
    outdir: str | Path,
) -> dict[str, Any]:
    del output_name
    cutflows_dir = artifact_family_dir(outdir, "cutflows")
    cutflows_dir.mkdir(parents=True, exist_ok=True)
    cutflow_id = product_id(node)
    relative_path = Path("artifacts") / "cutflows" / f"{cutflow_id}.json"
    graph = canonical_cutflow_graph(
        producer_id=node.id,
        params=dict(node.params or {}),
        product=value,
    )
    write_json(graph, Path(outdir) / relative_path)
    item = {
        "id": cutflow_id,
        "path": relative_path.as_posix(),
        "producer": node.id,
    }
    _update_manifest(cutflows_dir, "cutflows", item)
    return {"value": graph, "items": [item]}


def canonical_cutflow_graph(
    *,
    producer_id: str,
    params: dict[str, Any],
    product: Any,
) -> dict[str, Any]:
    if isinstance(product, dict) and product.get("kind") == "cutflow":
        return _json_safe(product)

    datasets = _cutflow_datasets(product)
    stats = _cutflow_stats_by_dataset(product)
    graph: dict[str, Any] = {
        "version": "1.0",
        "kind": "cutflow",
        "producer": producer_id,
        "datasets": datasets,
        "nodes": [],
        "edges": [],
    }

    for selection_name, steps, parents in _selection_groups(params.get("selection")):
        previous: str | None = None
        for index, step in enumerate(steps):
            node_id = f"{selection_name}[{index}]"
            node_parents = [previous] if previous is not None else parents
            graph["nodes"].append(
                {
                    "id": node_id,
                    "selection": selection_name,
                    "index": index,
                    "label": _cut_label(step),
                    "expr": _json_safe(_cut_expr(step)),
                    "kind": _cut_kind(step),
                    "parents": node_parents,
                    "stats": {
                        dataset: _selection_stats(stats.get(dataset, {}).get(node_id, {}))
                        for dataset in datasets
                    },
                }
            )

            if previous is not None:
                graph["edges"].append(
                    {"source": previous, "target": node_id, "kind": "sequence"}
                )
            else:
                graph["edges"].extend(
                    {"source": parent, "target": node_id, "kind": "branch"}
                    for parent in parents
                )
            previous = node_id

    if not graph["nodes"]:
        graph["nodes"] = _fallback_cutflow_nodes(stats, datasets)
    return graph


def _empty_cutflow_row(row: dict[str, Any]) -> dict[str, Any]:
    merged = {
        "name": row["name"],
        "n": 0.0,
        "sumw": 0.0,
        "sumw2": 0.0,
        "n_in": 0.0,
        "n_out": 0.0,
        "n_unweighted_in": 0,
        "n_unweighted_out": 0,
        "sumw_in": 0.0,
        "sumw_out": 0.0,
        "sumw2_in": 0.0,
        "sumw2_out": 0.0,
    }
    for field in ("selection", "index", "label", "expr", "kind"):
        if field in row:
            merged[field] = row[field]
    return merged


def _update_manifest(output_dir: Path, key: str, item: dict[str, str]) -> None:
    manifest_path = output_dir / "manifest.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = {key: []}

    items = [
        existing
        for existing in manifest.get(key, [])
        if isinstance(existing, dict) and existing.get("id") != item["id"]
    ]
    items.append(item)
    manifest[key] = sorted(items, key=lambda existing: existing["id"])
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _selection_groups(selection: Any) -> list[tuple[str, list[Any], list[str]]]:
    if not isinstance(selection, dict):
        return []
    groups: list[tuple[str, list[Any], list[str]]] = []
    for name, raw in selection.items():
        if isinstance(raw, list):
            groups.append((str(name), raw, []))
            continue
        if not isinstance(raw, dict):
            continue
        steps = raw.get("steps", raw.get("cuts", []))
        if not isinstance(steps, list):
            continue
        parent_value = raw.get("parents", raw.get("from", raw.get("parent", [])))
        groups.append((str(name), steps, _parent_ids(parent_value)))
    return groups


def _parent_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value else []
    if isinstance(value, list | tuple):
        return [str(item) for item in value if str(item)]
    return [str(value)]


def _cut_label(step: Any) -> str:
    if isinstance(step, str):
        return step
    if isinstance(step, dict):
        if isinstance(step.get("label"), str):
            return step["label"]
        if "expr" in step:
            return str(step["expr"])
        if isinstance(step.get("reduce"), dict):
            reduce_spec = step["reduce"]
            return f"{reduce_spec.get('op', 'reduce')}({reduce_spec.get('over', '')})"
    return str(step)


def _cut_expr(step: Any) -> Any:
    if isinstance(step, str):
        return step
    if isinstance(step, dict):
        if "expr" in step:
            return step["expr"]
        if "reduce" in step:
            return {"reduce": step["reduce"]}
    return step


def _cut_kind(step: Any) -> str:
    if isinstance(step, dict) and "reduce" in step:
        return "reduce"
    return "expression"


def _cutflow_datasets(product: Any) -> list[str]:
    stats = _cutflow_stats_by_dataset(product)
    return sorted(stats) if stats else ["default"]


def _cutflow_stats_by_dataset(product: Any) -> dict[str, dict[str, dict[str, Any]]]:
    if not isinstance(product, dict):
        return {}
    items = product["cutflows"] if isinstance(product.get("cutflows"), list) else [product]
    by_dataset: dict[str, dict[str, dict[str, Any]]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        dataset = str(item.get("dataset") or "default")
        cuts = item.get("cuts", [])
        if not isinstance(cuts, list):
            continue
        dataset_stats = by_dataset.setdefault(dataset, {})
        for row in cuts:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name") or row.get("id") or "")
            if name:
                dataset_stats[name] = row
    return by_dataset


def _selection_stats(row: dict[str, Any]) -> dict[str, Any]:
    if row:
        n_out = row.get("n_out", row.get("n", 0.0))
        n_in = row.get("n_in", n_out)
        n_unweighted_out = row.get("n_unweighted_out", row.get("n", n_out))
        n_unweighted_in = row.get("n_unweighted_in", row.get("n_in", n_unweighted_out))
        sumw_out = row.get("sumw_out", row.get("sumw", n_out))
        sumw_in = row.get("sumw_in", row.get("sumw", n_in))
        sumw2_out = row.get("sumw2_out", row.get("sumw2", n_out))
        sumw2_in = row.get("sumw2_in", row.get("sumw2", n_in))
    else:
        n_in = n_out = 0.0
        n_unweighted_in = n_unweighted_out = 0
        sumw_in = sumw_out = sumw2_in = sumw2_out = 0.0
    return {
        "n_in": float(n_in),
        "n_out": float(n_out),
        "n_unweighted_in": int(n_unweighted_in),
        "n_unweighted_out": int(n_unweighted_out),
        "sumw_in": float(sumw_in),
        "sumw_out": float(sumw_out),
        "sumw2_in": float(sumw2_in),
        "sumw2_out": float(sumw2_out),
    }


def _fallback_cutflow_nodes(
    stats: dict[str, dict[str, dict[str, Any]]],
    datasets: list[str],
) -> list[dict[str, Any]]:
    return [
        {
            "id": node_id,
            "selection": node_id.split("[", 1)[0],
            "index": _node_index(node_id),
            "label": node_id,
            "expr": node_id,
            "kind": "expression",
            "parents": [],
            "stats": {
                dataset: _selection_stats(stats.get(dataset, {}).get(node_id, {}))
                for dataset in datasets
            },
        }
        for node_id in sorted({node_id for rows in stats.values() for node_id in rows})
    ]


def _node_index(node_id: str) -> int:
    match = re.search(r"\[(\d+)\]$", node_id)
    return int(match.group(1)) if match else 0


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    if hasattr(value, "item"):
        return _json_safe(value.item())
    return str(value)
