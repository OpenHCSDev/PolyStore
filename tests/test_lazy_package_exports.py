"""Package exports must not initialize optional array runtimes eagerly."""

import json
import subprocess
import sys
from pathlib import Path


def _fresh_python(source: str) -> dict:
    result = subprocess.run(
        [sys.executable, "-c", source],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_constants_import_does_not_load_disk_or_array_frameworks():
    loaded = _fresh_python(
        "\n".join(
            (
                "import json",
                "import sys",
                "from polystore.constants import Backend",
                "assert Backend.DISK.value == 'disk'",
                "names = ('polystore.disk', 'tensorflow', 'torch', 'cupy', 'jax')",
                "print(json.dumps({name: name in sys.modules for name in names}))",
            )
        )
    )

    assert loaded == {
        "polystore.disk": False,
        "tensorflow": False,
        "torch": False,
        "cupy": False,
        "jax": False,
    }


def test_root_disk_exports_load_and_cache_the_declared_classes():
    loaded = _fresh_python(
        "\n".join(
            (
                "import json",
                "import polystore",
                "disk_loaded_before = 'polystore.disk' in __import__('sys').modules",
                "from polystore import DiskBackend, DiskStorageBackend",
                "from polystore.disk import DiskBackend as DeclaredDiskBackend",
                "from polystore.disk import DiskStorageBackend as DeclaredDiskStorageBackend",
                "print(json.dumps({",
                "    'disk_loaded_before': disk_loaded_before,",
                "    'backend_identity': DiskBackend is DeclaredDiskBackend,",
                "    'storage_identity': DiskStorageBackend is DeclaredDiskStorageBackend,",
                "    'backend_cached': polystore.DiskBackend is DiskBackend,",
                "    'storage_cached': polystore.DiskStorageBackend is DiskStorageBackend,",
                "}))",
            )
        )
    )

    assert loaded == {
        "disk_loaded_before": False,
        "backend_identity": True,
        "storage_identity": True,
        "backend_cached": True,
        "storage_cached": True,
    }
