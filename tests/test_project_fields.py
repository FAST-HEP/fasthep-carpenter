from __future__ import annotations

import awkward as ak

from fasthep_carpenter.operations.project_fields import run_project_fields


def test_project_fields_can_emit_aliases_only() -> None:
    events = ak.Array({"pt": [1.0, 2.0], "eta": [0.1, 0.2]})

    out = run_project_fields(
        events,
        stream_id="variation",
        aliases={"pt_up": "pt"},
        include_existing=False,
    )

    assert out.fields == ["pt_up"]
    assert ak.to_list(out.pt_up) == [1.0, 2.0]
