"""Storage-boundary array normalization tests."""

from types import SimpleNamespace
from dataclasses import dataclass
from typing import Any

import numpy as np
import pytest
import tifffile
from arraybridge import ArrayPayload

from polystore import array_payload
from polystore.disk import DiskStorageBackend


@dataclass
class _ArrayContainer(ArrayPayload):
    data: Any

    def array_payload_data(self):
        return self.data

    def with_data(self, data):
        return type(self)(data)


def test_storage_numpy_array_uses_nominal_payload_contract_without_copy() -> None:
    data = np.arange(120, dtype=np.int32).reshape(2, 4, 15)[:, :, ::3]
    payload = _ArrayContainer(data)

    assert array_payload.storage_numpy_array(payload) is data
    assert payload.data is data


def test_disk_tiff_retains_wrapped_integer_stack_pixels_and_dtype(tmp_path) -> None:
    data = np.zeros((2, 8, 12), dtype=np.int32)
    data[0, 1, 2] = 70001
    data[1, 3:5, 4:7] = 17
    path = tmp_path / "labels.tif"

    DiskStorageBackend().save(_ArrayContainer(data), path)

    retained = tifffile.imread(path)
    assert retained.dtype == data.dtype
    np.testing.assert_array_equal(retained, data)


def test_storage_numpy_array_converts_nominal_cpu_tensor_payload() -> None:
    torch = pytest.importorskip("torch")
    data = torch.tensor([[70001, 0], [9, 17]], dtype=torch.int32)
    payload = _ArrayContainer(data)

    retained = array_payload.storage_numpy_array(payload)

    assert retained.dtype == np.dtype(np.int32)
    np.testing.assert_array_equal(retained, data.numpy())
    assert payload.data is data


def test_storage_numpy_array_preserves_numpy_identity() -> None:
    data = np.arange(4, dtype=np.uint16)

    assert array_payload.storage_numpy_array(data) is data


def test_storage_numpy_array_asks_the_library_that_made_the_array(monkeypatch) -> None:
    data = SimpleNamespace()
    downloaded = np.arange(4, dtype=np.float32)
    calls = []

    class _Library:
        def to_numpy(self, value):
            calls.append(value)
            return downloaded

    library = _Library()
    owners = []
    monkeypatch.setattr(
        array_payload.ArrayLibrary,
        "of",
        classmethod(lambda cls, value: owners.append(value) or library),
    )

    result = array_payload.storage_numpy_array(data)

    assert result is downloaded
    assert owners == [data]
    assert calls == [data]


def test_storage_numpy_array_rejects_an_object_no_library_made() -> None:
    with pytest.raises(ValueError, match="Unknown memory type"):
        array_payload.storage_numpy_array(SimpleNamespace())
