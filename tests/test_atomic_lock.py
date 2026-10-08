"""Tests for stable-inode cross-platform file locks."""

import pytest

from polystore.atomic import FileLockTimeoutError, LockConfig, file_lock


@pytest.mark.parametrize("filename", ("metadata.json", "metadata", "a.b.c"))
def test_lock_config_owns_atomic_transaction_lock_path(tmp_path, filename):
    path = tmp_path / filename
    config = LockConfig(LOCK_SUFFIX=".guard")
    assert config.lock_path(path) == tmp_path / f"{filename}.guard"


def test_file_lock_preserves_body_exception(tmp_path) -> None:
    lock_path = tmp_path / "resource.lock"

    with pytest.raises(RuntimeError, match="body failed"):
        with file_lock(lock_path):
            raise RuntimeError("body failed")


def test_file_lock_times_out_without_replacing_locked_inode(tmp_path) -> None:
    lock_path = tmp_path / "resource.lock"

    with file_lock(lock_path):
        with pytest.raises(FileLockTimeoutError):
            with file_lock(lock_path, timeout=0.05, poll_interval=0.01):
                pytest.fail("contended lock must not be entered")

    assert lock_path.is_file()


def test_lock_makes_one_acquisition_attempt_at_zero_timeout(monkeypatch, tmp_path) -> None:
    attempts = 0

    def reject_lock(_lock_path):
        nonlocal attempts
        attempts += 1
        return None

    monkeypatch.setattr("polystore.atomic._try_acquire_lock", reject_lock)

    with pytest.raises(FileLockTimeoutError):
        with file_lock(tmp_path / "resource.lock", timeout=0, poll_interval=1):
            pytest.fail("unavailable lock must not be entered")

    assert attempts == 1


@pytest.mark.parametrize("indent", (None, 2))
def test_atomic_json_preserves_parsed_values_and_explicit_presentation(tmp_path, indent):
    import json

    from polystore.atomic import atomic_write_json

    path = tmp_path / "metadata.json"
    document = {"nested": [None, True, 2**60, 1.25, {"label": 'λ\nquoted "value"'}]}
    atomic_write_json(path, document, indent=indent)

    assert json.loads(path.read_text()) == document
    assert path.read_text() == json.dumps(document, indent=indent)
    assert not tuple(tmp_path.glob(".tmp*"))


def test_atomic_update_preserves_transform_and_releases_lock(tmp_path):
    import json

    from polystore.atomic import atomic_update_json, atomic_write_json

    path = tmp_path / "metadata.json"
    atomic_write_json(path, {"revision": 1, "nested": {"unchanged": True}})

    def update(document):
        document["revision"] += 1
        return document

    atomic_update_json(path, update)
    assert json.loads(path.read_text()) == {"revision": 2, "nested": {"unchanged": True}}
    with file_lock(LockConfig().lock_path(path), timeout=0):
        pass


@pytest.mark.parametrize("failure", ("encoding", "fsync", "replace", "update"))
def test_atomic_json_failure_preserves_original_and_removes_temporary(
    tmp_path, monkeypatch, failure
):
    from polystore import atomic

    path = tmp_path / "metadata.json"
    original = '{"revision": 1}\n'
    path.write_text(original)

    def fail(*_args):
        raise OSError("injected failure")

    if failure in {"fsync", "replace"}:
        monkeypatch.setattr(atomic.os, failure, fail)
    if failure == "update":
        with pytest.raises(atomic.FileLockError, match="Update function failed"):
            atomic.atomic_update_json(path, fail)
    else:
        data = {"unsupported": object()} if failure == "encoding" else {"revision": 2}
        with pytest.raises(atomic.FileLockError, match="Atomic JSON write failed"):
            atomic.atomic_write_json(path, data)

    assert path.read_text() == original
    assert not tuple(tmp_path.glob(".tmp*"))
    with file_lock(LockConfig().lock_path(path), timeout=0):
        pass
