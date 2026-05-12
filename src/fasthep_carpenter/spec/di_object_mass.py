from hepflow.model.ops import (
    OpSpec,
    RequireParse,
    RequireTemplates,
    TemplateProvide,
    TemplateReq,
)


HEP_DI_OBJECT_MASS_SPEC = OpSpec(
    requires=(
        RequireParse(("params", "mask"), "expr_or_name"),
        RequireTemplates(
            (
                TemplateReq(
                    param="collection",
                    default="Muon",
                    pattern="{collection}_{var}",
                    vars=("Px", "Py", "Pz", "E"),
                ),
            )
        ),
    ),
    provides=(
        # if out_var set, provide exactly that
        TemplateProvide(param="out_var", if_set=True),
        # otherwise provide Di{collection}_Mass
        TemplateProvide(
            param="collection", default="Muon", pattern="Di{collection}_Mass"
        ),
    ),
    consumes_event_stream=True,
    produces_event_stream=True,
)
