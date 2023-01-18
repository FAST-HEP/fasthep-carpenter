""" Module for fasthep-carpenter functions and settings """
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CarpenterSettings:
    """Settings for fasthep-carpenter"""

    ncores: int = 1
    outdir: str = "output"
    quiet: bool = False
    mode: str = "multiprocessing"
    nblocks_per_dataset: int = 1
    nblocks_per_sample: int = -1
    blocksize: int = 1_000_000
    profile: bool = False
