import os
from pathlib import Path


def mkdir_p(path: Path | str) -> None:
    os.makedirs(path, exist_ok=True)

def list_python_packages_with_versions() -> dict[str, str]:
    import pkg_resources

    return {
        dist.project_name: dist.version
        for dist in pkg_resources.working_set
        if dist.project_name != "pip"
    }
