from __future__ import annotations

from hepflow.model.ops import OpSpec, RequireParse, ValueFromParamProvide

HEP_MAKE_HIST_SPEC = OpSpec(
    requires=(
        RequireParse(("params",), "hist"),
        RequireParse(("params", "axes", "*", "source"), "expr_or_name"),
    ),
    consumes_event_stream=True,
    produces_event_stream=False,
    provides=(ValueFromParamProvide(path=("id",)),),
)
