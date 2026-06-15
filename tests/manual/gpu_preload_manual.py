from __future__ import annotations

from typing import Any

import numpy as np

from fasthep_carpenter.runtime.modifiers.gpu_preload import GPUPreloadModifier


def main() -> None:
    try:
        import cupy
    except ModuleNotFoundError:
        print("Skipping manual gpu.preload check: CuPy is not installed.")
        return

    try:
        device_count = cupy.cuda.runtime.getDeviceCount()
    except Exception as exc:
        print(f"Skipping manual gpu.preload check: no usable CUDA device ({exc}).")
        return

    if device_count < 1:
        print("Skipping manual gpu.preload check: no CUDA devices reported.")
        return

    node = type("Node", (), {"id": "stage.ManualGPUPreload"})()
    inputs: dict[str, Any] = {"stream": {"Muon_Pt": np.asarray([1.0, 2.0, 3.0])}}
    ctx: dict[str, Any] = {}

    GPUPreloadModifier(fields=["Muon_Pt"]).before_node(
        node=node,
        inputs=inputs,
        ctx=ctx,
    )

    moved = inputs["stream"]["Muon_Pt"]
    assert isinstance(moved, cupy.ndarray)
    assert float(cupy.sum(moved).get()) == 6.0
    print("gpu.preload manual check passed.")


if __name__ == "__main__":
    main()
