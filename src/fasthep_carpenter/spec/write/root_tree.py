from __future__ import annotations


ROOT_TREE_WRITE_SPEC = {
    "name": "root_tree",
    "kind": "writer",
    "version": "1.0",
    "input": {
        "name": "target",
        "kind": "event_stream",
        "required": True,
    },
    "params": {
        "path": {
            "type": "string",
            "required": True,
            "description": "Output ROOT file path.",
        },
        "tree": {
            "type": "string",
            "required": False,
            "default": "events",
            "description": "Name of the output TTree.",
        },
        "keep": {
            "type": "list[string]",
            "required": False,
            "default": None,
            "description": (
                "Optional list of branch names to retain from the input stream."
            ),
        },
        "compression": {
            "type": "string",
            "required": False,
            "default": "zlib",
            "allowed": ["zlib", "lz4", "zstd", "none"],
            "description": "Compression algorithm for the ROOT file.",
        },
        "compression_level": {
            "type": "integer",
            "required": False,
            "default": 1,
            "description": "Compression level where supported.",
        },
        "mode": {
            "type": "string",
            "required": False,
            "default": "recreate",
            "allowed": ["recreate"],
            "description": (
                "File writing mode. Only 'recreate' is supported in the initial implementation."
            ),
        },
    },
    "result": {
        "kind": "artifact",
        "description": "A written ROOT file containing a TTree.",
    },
}
