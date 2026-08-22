"""Tests for backend registry lazy instantiation and cleanup behavior."""

import pytest

from polystore import backend_registry as br


def test_get_backend_instance_and_cleanup():
    # instantiate memory backend and verify caching
    mem1 = br.get_backend_instance("memory")
    mem2 = br.get_backend_instance("memory")
    assert mem1 is mem2

    # instantiate disk to ensure it exists
    br.get_backend_instance("disk")

    # cleanup all backends should clear the cache
    br.cleanup_all_backends()

    # after cleanup the instances should be new objects
    mem3 = br.get_backend_instance("memory")
    assert mem3 is not mem1


def test_process_resource_cleanup_scope_is_explicit_and_deduplicated(
    monkeypatch,
) -> None:
    events: list[str] = []

    def cleanup() -> None:
        events.append("cleanup")

    monkeypatch.setattr(br, "_backend_instances", {})
    monkeypatch.setattr(br, "_cleanup_callbacks", [])
    br.register_cleanup_callback(cleanup)
    br.register_cleanup_callback(cleanup)

    br.cleanup_backend_connections()
    assert events == []

    br.cleanup_backend_connections(include_process_resources=True)
    assert events == ["cleanup"]

    br.cleanup_all_backends()
    assert events == ["cleanup", "cleanup"]


def test_process_resource_cleanup_runs_all_callbacks_then_fails(
    monkeypatch,
) -> None:
    events: list[str] = []

    def fail() -> None:
        events.append("fail")
        raise RuntimeError("cleanup failed")

    def succeed() -> None:
        events.append("succeed")

    monkeypatch.setattr(br, "_backend_instances", {})
    monkeypatch.setattr(br, "_cleanup_callbacks", [fail, succeed])

    with pytest.raises(ExceptionGroup, match="Process-resource cleanup failed"):
        br.cleanup_backend_connections(include_process_resources=True)

    assert events == ["fail", "succeed"]
