from hepflow.model.ops import OpSpec
from hepflow.model.ops import RequireParse, ValueFromParamProvide


HEP_DEFINE_SPEC = OpSpec(
    requires=(
        RequireParse(path=("params", "variables", "*", "expr"), parser="expr_or_name"),
        RequireParse(
            path=("params", "variables", "*", "reduce", "over"), parser="expr_or_name"
        ),
    ),
    provides=(ValueFromParamProvide(path=("params", "variables", "*", "name")),),
    consumes_event_stream=True,
    produces_event_stream=True,
)
