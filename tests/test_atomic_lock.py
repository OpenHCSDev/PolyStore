"""Tests for stable-inode cross-platform file locks."""

import pytest

from polystore.atomic import FileLockTimeoutError, file_lock


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
