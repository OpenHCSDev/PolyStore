"""Contract tests for the direct OME-Zarr array source."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
import zarr

from polystore.base import BackendBase
from polystore.constants import Backend
from polystore.exceptions import StorageResolutionError
from polystore.ome_zarr_storage import OmeZarrArrayRef, OmeZarrStorageBackend


def test_address_and_backend_declarations_do_not_import_zarr(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import builtins
import sys
from pathlib import Path

original_import = builtins.__import__
def declaration_import(name, *args, **kwargs):
    assert name != 'zarr' and not name.startswith('zarr.'), 'Eager Zarr runtime import'
    return original_import(name, *args, **kwargs)
builtins.__import__ = declaration_import
from polystore.ome_zarr_storage import OmeZarrArrayRef, OmeZarrStorageBackend

ref = OmeZarrArrayRef(Path('missing.zarr'), '0')
assert OmeZarrArrayRef.from_backend_address(ref.to_backend_address()) == ref
assert not OmeZarrStorageBackend().exists(ref.to_backend_address())
assert not {'zarr', 'cupy', 'scipy.linalg', 'scipy.ndimage', 'scipy.special', 'skimage'}.intersection(sys.modules)
""",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def _write_array(
    store_path: Path, array_path: str, data: np.ndarray, *, zarr_format: int = 2
) -> None:
    root = zarr.open_group(str(store_path), mode="w", zarr_format=zarr_format)
    array = root.create_array(array_path, shape=data.shape, dtype=data.dtype)
    array[:] = data


def test_backend_uses_nominal_backend_identity_and_registry() -> None:
    assert OmeZarrStorageBackend._backend_type == Backend.OME_ZARR.value
    assert BackendBase.__registry__[Backend.OME_ZARR.value] is OmeZarrStorageBackend


def test_array_reference_round_trips_as_canonical_opaque_address(tmp_path: Path) -> None:
    ref = OmeZarrArrayRef(tmp_path / "plate.zarr", "/A/01/0/")

    assert ref.array_path == "A/01/0"
    assert json.loads(ref.to_backend_address()) == {
        "array_path": "A/01/0",
        "store_path": str(tmp_path / "plate.zarr"),
    }
    assert OmeZarrArrayRef.from_backend_address(ref.to_backend_address()) == ref


@pytest.mark.parametrize(
    "address, error_type",
    [
        ("not-json", ValueError),
        ("[]", TypeError),
        ('{"array_path":"0"}', ValueError),
        ('{"array_path":0,"store_path":"plate.zarr"}', TypeError),
    ],
)
def test_array_reference_rejects_malformed_addresses(
    address: str,
    error_type: type[Exception],
) -> None:
    with pytest.raises(error_type):
        OmeZarrArrayRef.from_backend_address(address)


@pytest.mark.parametrize("zarr_format", [2, 3])
def test_backend_load_exists_and_projects_physical_source(tmp_path: Path, zarr_format: int) -> None:
    store_path = tmp_path / "plate.zarr"
    pixels = np.arange(12, dtype=np.uint16).reshape(3, 4)
    _write_array(store_path, "A/01/0", pixels, zarr_format=zarr_format)
    ref = OmeZarrArrayRef(store_path, "A/01/0")
    address = ref.to_backend_address()
    backend = OmeZarrStorageBackend()

    assert backend.exists(address)
    assert backend.is_file(address)
    assert not backend.is_dir(address)
    assert backend.source_path(address, base_path=tmp_path / "ignored") == store_path
    np.testing.assert_array_equal(backend.load(address), pixels)
    np.testing.assert_array_equal(backend.load_batch([address]), [pixels])


def test_backend_fails_loudly_for_workspace_operations(tmp_path: Path) -> None:
    backend = OmeZarrStorageBackend()
    with pytest.raises(StorageResolutionError):
        backend.list_files(tmp_path)
    with pytest.raises(StorageResolutionError):
        backend.list_dir(tmp_path)


def test_backend_reports_absent_store_or_array(tmp_path: Path) -> None:
    backend = OmeZarrStorageBackend()
    missing_store = OmeZarrArrayRef(tmp_path / "missing.zarr", "0")
    assert not backend.exists(missing_store.to_backend_address())
    with pytest.raises(FileNotFoundError):
        backend.load(missing_store.to_backend_address())

    store_path = tmp_path / "plate.zarr"
    _write_array(store_path, "present", np.zeros((2, 2), dtype=np.uint8))
    missing_array = OmeZarrArrayRef(store_path, "absent")
    assert not backend.exists(missing_array.to_backend_address())
    with pytest.raises(FileNotFoundError):
        backend.load(missing_array.to_backend_address())
