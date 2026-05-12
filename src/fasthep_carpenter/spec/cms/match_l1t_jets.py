from hepflow.model.ops import OpSpec, RequireParse, ValueProvide


CMS_MATCH_L1T_JETS_SPEC = OpSpec(
    requires=(
        RequireParse(path=("params", "reco", "*"), parser="expr_or_name"),
        RequireParse(path=("params", "l1", "*"), parser="expr_or_name"),
    ),
    provides=(
        ValueProvide(
            symbols=(
                "matched_jets",
                "n_matched",
                "matched_reco_et",
                "matched_l1_et",
                "unmatched_dr",
                "matched_dr",
            )
        ),
    ),
)
