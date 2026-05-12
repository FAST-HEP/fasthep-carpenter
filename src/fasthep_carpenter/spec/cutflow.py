from hepflow.model.ops import OpSpec, RequireParse, ValueFromParamProvide


HEP_SELECTION_CUTFLOW_SPEC = OpSpec(
    requires=(
        RequireParse(
            path=("params", "selection", "*"), parser="walk_expr", skip_pre_parsing=True
        ),
        RequireParse(
            path=("params", "weight_expr"), parser="expr_or_name", optional=True
        ),
    ),
    provides=(ValueFromParamProvide(path=("id",)),),
    consumes_event_stream=True,
    produces_event_stream=True,
)
