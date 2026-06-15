from __future__ import annotations

from typing import Any

import numpy as np

from fasthep_carpenter.runtime.modifiers.cuda_jit import CUDAJitModifier


def main() -> None:
    try:
        from numba import cuda
    except Exception as exc:
        print(f"Skipping manual cuda.jit check: Numba CUDA is unavailable ({exc}).")
        return

    try:
        if not cuda.is_available():
            print("Skipping manual cuda.jit check: CUDA is not available.")
            return
    except Exception as exc:
        print(f"Skipping manual cuda.jit check: CUDA probe failed ({exc}).")
        return

    def add_one(values, out):
        idx = cuda.grid(1)
        if idx < values.size:
            out[idx] = values[idx] + 1

    node = type("Node", (), {"id": "stage.ManualCUDAJit"})()
    ctx: dict[str, Any] = {"cuda_jit_functions": {"add_one": add_one}}

    CUDAJitModifier(functions=["add_one"]).before_node(
        node=node,
        inputs={},
        ctx=ctx,
    )

    compiled = ctx["cuda_jit_compiled"]["add_one"]
    values = cuda.to_device(np.asarray([1.0, 2.0, 3.0], dtype=np.float32))
    out = cuda.device_array_like(values)
    compiled[1, 32](values, out)

    assert out.copy_to_host().tolist() == [2.0, 3.0, 4.0]
    print("cuda.jit manual check passed.")


if __name__ == "__main__":
    main()
