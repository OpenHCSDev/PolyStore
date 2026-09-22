from __future__ import annotations

from collections.abc import Iterator

import pytest

from polystore import fiji_stream, napari_stream
from polystore.streaming import StreamingBackend, _streaming_backend


class _SharedMemoryProbe:
    def __init__(self) -> None:
        self.close_calls = 0
        self.unlink_calls = 0

    def close(self) -> None:
        self.close_calls += 1

    def unlink(self) -> None:
        self.unlink_calls += 1


class _PublisherProbe:
    def __init__(self) -> None:
        self.close_calls = 0

    def close(self) -> None:
        self.close_calls += 1


class _ContextProbe:
    def __init__(self) -> None:
        self.term_calls = 0

    def term(self) -> None:
        self.term_calls += 1


@pytest.mark.parametrize(
    "backend_type",
    (fiji_stream.FijiStreamingBackend, napari_stream.NapariStreamingBackend),
)
def test_streaming_destructor_cleanup_is_shutdown_safe_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    backend_type: type[StreamingBackend],
) -> None:
    backend = backend_type()
    shared_memory = _SharedMemoryProbe()
    publisher = _PublisherProbe()
    context = _ContextProbe()
    backend._shared_memory_blocks = {"owned": shared_memory}
    backend._publishers = {"owned": publisher}
    backend._context = context

    # Python clears module globals in an unspecified order during interpreter
    # shutdown. Cleanup must not require the module logger to remain available.
    monkeypatch.setattr(_streaming_backend, "logger", None)

    backend.__del__()
    backend.__del__()

    assert shared_memory.close_calls == 1
    assert shared_memory.unlink_calls == 1
    assert publisher.close_calls == 1
    assert context.term_calls == 1
    assert backend._shared_memory_blocks == {}
    assert backend._publishers == {}
    assert backend._context is None


def test_streaming_subclasses_share_one_destructor_authority() -> None:
    assert fiji_stream.FijiStreamingBackend.__del__ is StreamingBackend.__del__
    assert napari_stream.NapariStreamingBackend.__del__ is StreamingBackend.__del__


def test_streaming_destructor_accepts_partial_construction() -> None:
    backend = napari_stream.NapariStreamingBackend.__new__(napari_stream.NapariStreamingBackend)

    backend.__del__()


class _BrokenResourceMapping(dict):
    def items(self) -> Iterator[tuple[object, object]]:
        raise RuntimeError("broken cleanup ownership")


def test_streaming_cleanup_does_not_hide_unexpected_ownership_errors() -> None:
    backend = napari_stream.NapariStreamingBackend()
    backend._shared_memory_blocks = _BrokenResourceMapping()

    try:
        with pytest.raises(RuntimeError, match="broken cleanup ownership"):
            backend.cleanup()
    finally:
        backend._shared_memory_blocks = {}
