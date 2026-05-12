from __future__ import annotations


ZIP_JOIN_SPEC = {
    "name": "hep.zip_join",
    "kind": "transform",
    "version": "1.0",
    "params": {
        "inputs": {
            "type": "list[mapping]",
            "required": True,
            "description": "List of input stream definitions.",
        },
        "on_mismatch": {
            "type": "string",
            "required": False,
            "default": "error",
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Zipped event stream.",
    },
}
