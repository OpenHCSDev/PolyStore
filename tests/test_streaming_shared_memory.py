from __future__ import annotations

import numpy as np
import pytest

from polystore.streaming import StreamingSharedMemoryAuthority, _streaming_backend


class _SharedMemoryProbe:
    def __init__(self, source: np.ndarray) -> None:
        self._name = "/sender-owned"
        self.buf = bytearray(source.tobytes())
        self.closed = False

    def close(self) -> None:
        self.closed = True


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
