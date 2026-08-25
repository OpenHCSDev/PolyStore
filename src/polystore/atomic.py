"""
Atomic file operations with locking.

Provides utilities for atomic read-modify-write operations with file locking
to prevent concurrency issues in multiprocessing environments.
"""

import json
import logging
import os
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TypeVar

import portalocker

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass(frozen=True)
class LockConfig:
    """Configuration constants for file locking operations."""

    DEFAULT_TIMEOUT: float = 30.0
    DEFAULT_POLL_INTERVAL: float = 0.1
    LOCK_SUFFIX: str = ".lock"
    TEMP_PREFIX: str = ".tmp"
    JSON_INDENT: int = 2


LOCK_CONFIG = LockConfig()


class FileLockError(Exception):
    """Raised when file locking operations fail."""


class FileLockTimeoutError(FileLockError):
    """Raised when file lock acquisition times out."""


@contextmanager
def file_lock(
    lock_path: str | Path,
    timeout: float = LOCK_CONFIG.DEFAULT_TIMEOUT,
    poll_interval: float = LOCK_CONFIG.DEFAULT_POLL_INTERVAL,
) -> Iterator[None]:
    """Context manager for exclusive file locking."""
    lock_path = Path(lock_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    lock_fd = _acquire_lock_with_timeout(lock_path, timeout, poll_interval)
    try:
        yield
    finally:
        _cleanup_lock(lock_fd, lock_path)


def _acquire_lock_with_timeout(lock_path: Path, timeout: float, poll_interval: float) -> int:
    """Acquire file lock with timeout and return file descriptor."""
    deadline = time.monotonic() + timeout

    while True:
        lock_fd = _try_acquire_lock(lock_path)
        if lock_fd is not None:
            return lock_fd
        remaining_seconds = deadline - time.monotonic()
        if remaining_seconds <= 0:
            break
        time.sleep(min(poll_interval, remaining_seconds))

    raise FileLockTimeoutError(f"Failed to acquire lock {lock_path} within {timeout}s")


def _try_acquire_lock(lock_path: Path) -> int | None:
    """Try to acquire lock once, return fd or None."""
    lock_fd = None
    try:
        lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_WRONLY, 0o600)
        portalocker.lock(lock_fd, portalocker.LOCK_EX | portalocker.LOCK_NB)
        logger.debug("Acquired file lock: %s", lock_path)
        return lock_fd
    except (OSError, portalocker.exceptions.LockException):
        if lock_fd is not None:
            os.close(lock_fd)
        return None
    except BaseException:
        if lock_fd is not None:
            os.close(lock_fd)
        raise


def _cleanup_lock(lock_fd: int, lock_path: Path) -> None:
    """Clean up file lock resources."""
    try:
        portalocker.unlock(lock_fd)
        logger.debug("Released file lock: %s", lock_path)
    except (OSError, portalocker.exceptions.LockException) as exc:
        logger.warning("Error releasing lock %s: %s", lock_path, exc)
    finally:
        try:
            os.close(lock_fd)
        except OSError as exc:
            logger.warning("Error closing lock %s: %s", lock_path, exc)


def atomic_write_json(
    file_path: str | Path,
    data: dict[str, Any],
    indent: int = LOCK_CONFIG.JSON_INDENT,
    ensure_directory: bool = True,
) -> None:
    """Atomically write JSON data to file using temporary file + rename."""
    file_path = Path(file_path)

    if ensure_directory:
        file_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        tmp_path = _write_to_temp_file(file_path, data, indent)
        # Use os.replace() instead of os.rename() for atomic replacement on all platforms
        # os.rename() fails on Windows if destination exists, os.replace() works on both Unix and Windows
        os.replace(tmp_path, str(file_path))
        logger.debug(f"Atomically wrote JSON to {file_path}")
    except Exception as e:
        raise FileLockError(f"Atomic JSON write failed for {file_path}: {e}") from e


def _write_to_temp_file(file_path: Path, data: dict[str, Any], indent: int) -> str:
    """Write data to temporary file and return path."""
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=file_path.parent,
        prefix=f"{LOCK_CONFIG.TEMP_PREFIX}{file_path.name}",
        suffix=".json",
        delete=False,
    ) as tmp_file:
        json.dump(data, tmp_file, indent=indent)
        tmp_file.flush()
        os.fsync(tmp_file.fileno())
        return tmp_file.name


def atomic_update_json(
    file_path: str | Path,
    update_func: Callable[[dict[str, Any] | None], dict[str, Any]],
    lock_timeout: float = LOCK_CONFIG.DEFAULT_TIMEOUT,
    default_data: dict[str, Any] | None = None,
) -> None:
    """Atomically update JSON file using read-modify-write with file locking."""
    file_path = Path(file_path)
    lock_path = file_path.with_suffix(f"{file_path.suffix}{LOCK_CONFIG.LOCK_SUFFIX}")

    with file_lock(lock_path, timeout=lock_timeout):
        current_data = _read_json_or_default(file_path, default_data)

        try:
            updated_data = update_func(current_data)
        except Exception as e:
            raise FileLockError(f"Update function failed for {file_path}: {e}") from e

        atomic_write_json(file_path, updated_data)
        logger.debug(f"Atomically updated JSON file: {file_path}")


def _read_json_or_default(
    file_path: Path, default_data: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Read JSON file or return default data if file doesn't exist or is invalid."""
    if not file_path.exists():
        return default_data

    try:
        with open(file_path) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to read {file_path}, using default: {e}")
        return default_data
