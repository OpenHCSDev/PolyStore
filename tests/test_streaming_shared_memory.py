from __future__ import annotations

import numpy as np
import pytest

from polystore.streaming import (
    StreamingSharedMemoryAuthority,
    _streaming_backend,
)
from polystore.streaming._streaming_backend import (
    StreamingDataTypeAuthority,
    StreamingItemPath,
    StreamingSharedMemoryRequest,
)
from polystore.streaming_constants import StreamingDataType


class _SharedMemoryProbe:
    def __init__(self, source: np.ndarray) -> None:
        self._name = "/sender-owned"
        self.buf = bytearray(source.tobytes())
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _CreatedSharedMemoryProbe:
    def __init__(self, *, size: int, name: str) -> None:
        self.name = name
        self.size = size
        self.buf = bytearray(size)


def test_non_posix_receiver_copies_without_resource_tracker_unregister(
    monkeypatch,
) -> None:
    source = np.arange(12, dtype=np.uint16).reshape(3, 4)
    memory = _SharedMemoryProbe(source)
    unregister_calls = []
    monkeypatch.setattr(_streaming_backend, "_USE_POSIX", False)
    monkeypatch.setattr(
        _streaming_backend.shared_memory,
        "SharedMemory",
        lambda *, name: memory,
    )
    monkeypatch.setattr(
        _streaming_backend.resource_tracker,
        "unregister",
        lambda *args: unregister_calls.append(args),
    )

    copied = StreamingSharedMemoryAuthority.copy_sender_owned_array(
        name="sender-owned",
        shape=source.shape,
        dtype=str(source.dtype),
    )

    np.testing.assert_array_equal(copied, source)
    assert copied.flags.owndata
    assert unregister_calls == []
    assert memory.closed


def test_posix_receiver_releases_tracking_without_unlinking_sender_memory(
    monkeypatch,
) -> None:
    source = np.arange(6, dtype=np.float32).reshape(2, 3)
    memory = _SharedMemoryProbe(source)
    unregister_calls = []
    monkeypatch.setattr(_streaming_backend, "_USE_POSIX", True)
    monkeypatch.setattr(
        _streaming_backend.shared_memory,
        "SharedMemory",
        lambda *, name: memory,
    )
    monkeypatch.setattr(
        _streaming_backend.resource_tracker,
        "unregister",
        lambda *args: unregister_calls.append(args),
    )

    copied = StreamingSharedMemoryAuthority.copy_sender_owned_array(
        name="sender-owned",
        shape=source.shape,
        dtype=source.dtype,
    )

    np.testing.assert_array_equal(copied, source)
    assert unregister_calls == [(memory._name, "shared_memory")]
    assert memory.closed


def test_receiver_closes_attachment_when_array_projection_fails(monkeypatch) -> None:
    source = np.arange(2, dtype=np.uint8)
    memory = _SharedMemoryProbe(source)
    monkeypatch.setattr(_streaming_backend, "_USE_POSIX", False)
    monkeypatch.setattr(
        _streaming_backend.shared_memory,
        "SharedMemory",
        lambda *, name: memory,
    )

    with pytest.raises(TypeError, match="buffer is too small"):
        StreamingSharedMemoryAuthority.copy_sender_owned_array(
            name="sender-owned",
            shape=(3,),
            dtype=source.dtype,
        )

    assert memory.closed


def test_sender_allocates_transport_storage_for_empty_array(monkeypatch) -> None:
    created = []

    def shared_memory_factory(*, create, size, name):
        assert create is True
        memory = _CreatedSharedMemoryProbe(size=size, name=name)
        created.append(memory)
        return memory

    monkeypatch.setattr(
        _streaming_backend.shared_memory,
        "SharedMemory",
        shared_memory_factory,
    )

    block = StreamingSharedMemoryAuthority.create(
        StreamingSharedMemoryRequest(
            data=np.empty((0, 3), dtype=np.float32),
            item_path=StreamingItemPath("empty.tif"),
            shm_prefix="test_",
        )
    )

    assert created == [block.shared_memory]
    assert block.shared_memory.size == 1
    assert block.payload.shape == (0, 3)
    assert block.payload.dtype == "float32"


def test_empty_roi_archive_retains_vector_payload_semantics() -> None:
    assert StreamingDataTypeAuthority.detect(
        [],
        StreamingItemPath("empty.graph.roi.zip"),
    ) is StreamingDataType.SHAPES
