import os
from pathlib import Path


def mkdir_p(path: Path | str) -> None:
    os.makedirs(path, exist_ok=True)
