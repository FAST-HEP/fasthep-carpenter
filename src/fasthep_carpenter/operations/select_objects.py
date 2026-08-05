from __future__ import annotations

import ast
from typing import Any

import awkward as ak
from hepflow.compiler.data_flow import (
    DependencyContext,
    parse_component_data_dependencies,
)
from hepflow.compiler.expr_symbols import data_symbols_in_expr
from hepflow.registry.defaults import default_expr_registry
from hepflow.runtime import ComponentContext
from hepflow.runtime.engine import eval_expr

DEFAULT_SORT = {"by": "pt", "order": "descending"}


SELECT_OBJECTS_SPEC = {
    "name": "hep.select_objects",
    "kind": "transform",
    "version": "1.0",
    "input": {"name": "stream", "kind": "event_stream", "required": True},
    "params": {
        "collection": {"type": "string", "required": True},
        "output": {"type": "string", "required": True},
        "selection": {"type": "list[string]", "required": False, "default": []},
        "keep": {"type": "list[string]", "required": True},
        "derived": {"type": "mapping", "required": False},
        "sort": {"type": "mapping", "required": False, "default": DEFAULT_SORT},
        "legacy": {"type": "mapping", "required": False},
    },
    "normalize_params": {"defaults": True},
    "result": {
        "kind": "event_stream",
        "description": "Event stream with a selected object collection.",
    },
    "requires": {
        "symbols": [
            {
                "from": "params.collection",
                "kind": "field_prefix",
                "suffixes_from": "params.keep",
                "exclude_suffixes_from": "params.derived",
            },
            {
                "from": "params.selection",
                "kind": "relative_expr",
                "prefix_from": "params.collection",
            },
            {
                "from": "params.derived.*",
                "kind": "relative_expr",
                "prefix_from": "params.collection",
            },
            {
                "from": "params.collection",
                "kind": "field_prefix",
                "suffixes_from": "params.sort.by",
                "exclude_suffixes_from": "params.derived",
                "skip_if_false": "params.sort",
                "optional": True,
            },
        ]
    },
    "provides": {
        "symbols": [
            {
                "from": "params.output",
                "kind": "field_prefix",
                "suffixes_from": "params.keep",
            },
            {"from": "params.output", "kind": "count"},
        ]
    },
}


def run_select_objects(
    *,
    stream: Any,
    collection: str,
    output: str,
    selection: list[str] | None = None,
    keep: list[str] | None = None,
    derived: dict[str, str] | None = None,
    sort: dict[str, Any] | bool | None = None,
    ctx: ComponentContext | None = None,
    **params: Any,
) -> Any:
    del params
    collection_name = _required_name(collection, "collection")
    output_name = _required_name(output, "output")
    keep_fields = _field_names(keep, "keep")
    derived_exprs = _derived_expressions(derived)
    selection_exprs = _selection_expressions(selection)
    sort_cfg = _normalise_sort(sort)
    sort_field = _sort_field(sort_cfg)

    runtime_params = {
        "collection": collection_name,
        "output": output_name,
        "selection": selection_exprs,
        "keep": keep_fields,
        "derived": derived_exprs,
        "sort": sort_cfg,
    }
    deps = _dependencies(runtime_params)
    input_fields = sorted(deps.consumes)
    for input_field in input_fields:
        _field(stream, input_field)

    mask_like = _mask_like_field(
        stream,
        collection=collection_name,
        keep=keep_fields,
        derived=derived_exprs,
        input_fields=input_fields,
    )
    mask = ak.ones_like(mask_like == mask_like, dtype=bool)
    resolved_selection = [
        resolve_collection_expression(expr, collection=collection_name)
        for expr in selection_exprs
    ]
    for expression in resolved_selection:
        mask = mask & eval_expr(stream, expression, dict(ctx or {}))

    selected = {}
    for field in keep_fields:
        if field in derived_exprs:
            resolved = resolve_collection_expression(
                derived_exprs[field],
                collection=collection_name,
            )
            selected[field] = eval_expr(stream, resolved, dict(ctx or {}))[mask]
        else:
            selected[field] = _field(stream, f"{collection_name}_{field}")[mask]

    if sort_field is not None:
        if sort_field in selected:
            sort_values = selected[sort_field]
        elif sort_field in derived_exprs:
            resolved = resolve_collection_expression(
                derived_exprs[sort_field],
                collection=collection_name,
            )
            sort_values = eval_expr(stream, resolved, dict(ctx or {}))[mask]
        else:
            sort_values = _field(stream, f"{collection_name}_{sort_field}")[mask]
        order = _sort_order(sort_cfg)
        indices = ak.argsort(
            sort_values,
            axis=1,
            ascending=order == "ascending",
            stable=True,
        )
        selected = {field: value[indices] for field, value in selected.items()}

    out = stream
    out = ak.with_field(out, ak.num(selected[keep_fields[0]], axis=1), f"n{output_name}")
    for field in keep_fields:
        out = ak.with_field(out, selected[field], f"{output_name}_{field}")

    if ctx is not None:
        ctx.provenance.record_operation(
            inputs={"symbols": input_fields},
            outputs={"symbols": sorted(deps.produces)},
        )
    return out


def selected_output_fields(output: str, keep: list[str]) -> list[str]:
    output_name = _required_name(output, "output")
    keep_fields = _field_names(keep, "keep")
    return [f"n{output_name}", *(f"{output_name}_{field}" for field in keep_fields)]


def resolve_collection_expression(expression: str, *, collection: str) -> str:
    collection_name = _required_name(collection, "collection")
    expression_text = str(expression).strip()
    registry = default_expr_registry()
    relative_fields = data_symbols_in_expr(
        expression_text,
        known_functions=set(registry.functions),
        known_constants=set(registry.constants),
        context_symbols=set(),
        produced=set(),
    )
    tree = ast.parse(
        expression_text.replace("&&", " and ").replace("||", " or "),
        mode="eval",
    )
    rewritten = _CollectionExpressionRewriter(
        {field: f"{collection_name}_{field}" for field in relative_fields}
    ).visit(tree)
    ast.fix_missing_locations(rewritten)
    return ast.unparse(rewritten)


class _CollectionExpressionRewriter(ast.NodeTransformer):
    def __init__(self, replacements: dict[str, str]) -> None:
        self._replacements = replacements

    def visit_Name(self, node: ast.Name) -> ast.AST:
        replacement = self._replacements.get(node.id)
        if replacement is None:
            return node
        return ast.copy_location(ast.Name(id=replacement, ctx=node.ctx), node)


def _mask_like_field(
    stream: Any,
    *,
    collection: str,
    keep: list[str],
    derived: dict[str, str],
    input_fields: list[str],
) -> Any:
    for field in keep:
        if field not in derived:
            return _field(stream, f"{collection}_{field}")
    if input_fields:
        return _field(stream, input_fields[0])
    raise ValueError("hep.select_objects requires at least one concrete input field")


def _selection_expressions(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(
        isinstance(item, str) and item.strip() for item in value
    ):
        raise ValueError("hep.select_objects selection must be a list of expressions")
    return [item.strip() for item in value]


def _field_names(value: Any, param: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(
            f"hep.select_objects {param} must be a non-empty list of fields"
        )
    fields = [str(item).strip() for item in value]
    if any(not field or "." in field for field in fields):
        raise ValueError(
            f"hep.select_objects {param} fields must be collection-relative names"
        )
    return fields


def _derived_expressions(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("hep.select_objects derived must be a mapping")
    out = {}
    for field, expression in value.items():
        fields = _field_names([field], "derived")
        if not isinstance(expression, str) or not expression.strip():
            raise ValueError("hep.select_objects derived expressions must be strings")
        out[fields[0]] = expression.strip()
    return out


def _normalise_sort(value: dict[str, Any] | bool | None) -> dict[str, Any] | bool:
    if value is None:
        return dict(DEFAULT_SORT)
    if value is False:
        return False
    if not isinstance(value, dict):
        raise ValueError("hep.select_objects sort must be a mapping, false, or omitted")
    return dict(value)


def _sort_field(value: Any) -> str | None:
    if value in (None, False):
        return None
    if not isinstance(value, dict):
        raise ValueError("hep.select_objects sort must be a mapping")
    by = value.get("by")
    if by is None:
        return None
    return _field_names([by], "sort.by")[0]


def _sort_order(value: dict[str, Any] | bool | None) -> str:
    if value is False:
        return "descending"
    if value is None:
        order = "descending"
    elif isinstance(value, dict):
        order = str(value.get("order", "descending"))
    else:
        raise ValueError("hep.select_objects sort must be a mapping, false, or omitted")
    if order not in {"ascending", "descending"}:
        raise ValueError(
            "hep.select_objects sort.order must be 'ascending' or 'descending', "
            f"got {order!r}"
        )
    return order


def _required_name(value: Any, param: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"hep.select_objects {param} must be a non-empty string")
    return value.strip()


def _field(stream: Any, name: str) -> Any:
    fields = set(getattr(stream, "fields", []) or [])
    if name not in fields:
        raise KeyError(f"Required field {name!r} is missing from event stream")
    return stream[name]


def _dependencies(params: dict[str, Any]):
    registry = default_expr_registry()
    return parse_component_data_dependencies(
        spec=SELECT_OBJECTS_SPEC,
        params=params,
        dep_ctx=DependencyContext(
            known_functions=set(registry.functions),
            known_constants=set(registry.constants),
            context_symbols=set(),
        ),
    )


__all__ = [
    "DEFAULT_SORT",
    "SELECT_OBJECTS_SPEC",
    "resolve_collection_expression",
    "run_select_objects",
    "selected_output_fields",
]
