"""Pytest fixtures for polystore tests.

Provides a minimal registry with `disk` and `memory` backends and a FileManager
that routes requests to them. Keeps fixtures small and reusable for parameterized tests.
"""
import pytest
import numpy as np
from pathlib import Path

from polystore.imagej_distribution import FijiArchiveDistribution, ImageJArchiveDownloadPolicy

FijiArchiveDistribution.configure_process_environment(
    default_download_policy=ImageJArchiveDownloadPolicy(allow_download=False)
)

from polystore import FileManager
from polystore.disk import DiskBackend
from polystore.memory import MemoryBackend


@pytest.fixture()
def registry():
    """Return a minimal registry mapping backend name to instance.

    Keep this small to avoid triggering heavy optional imports during test collection.
    """
    disk = DiskBackend()
    memory = MemoryBackend()
    return {"disk": disk, "memory": memory}


@pytest.fixture
def file_manager(registry):
    """FileManager pointed at the mini-registry."""
    return FileManager(registry)


@pytest.fixture
def sample_payloads():
    """Return simple payloads used across tests."""
    arr = np.arange(6).reshape(2, 3)
    text = "hello world"
    data = {"a": 1, "b": "two"}
    return {"array": arr, "text": text, "json": data}
