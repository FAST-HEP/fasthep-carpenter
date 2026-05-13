from __future__ import annotations

ROOT_TREE_SOURCE_SPEC = {
    "name": "root_tree",
    "kind": "source",
    "version": "1.0",
    "input": None,
    "params": {
        "datasets": {
            "type": "list[mapping]",
            "required": True,
            "description": "Normalized dataset entries.",
        },
        "defaults": {
            "type": "mapping",
            "required": False,
            "default": {},
            "description": "Normalized data.defaults block.",
        },
        "tree": {
            "type": "string",
            "required": True,
            "description": "Name of the ROOT tree to read.",
        },
        "stream_type": {
            "type": "string",
            "required": False,
            "default": "event_stream",
            "description": "Declared output stream kind.",
        },
        "branches": {
            "type": "list[string]",
            "required": False,
            "default": None,
            "description": "Optional list of physical branches to read.",
        },
        "start": {
            "type": "integer",
            "required": False,
            "default": None,
            "description": "Optional inclusive entry start.",
        },
        "stop": {
            "type": "integer",
            "required": False,
            "default": None,
            "description": "Optional exclusive entry stop.",
        },
    },
    "result": {
        "kind": "event_stream",
        "description": "Loaded ROOT tree event stream.",
    },
}
