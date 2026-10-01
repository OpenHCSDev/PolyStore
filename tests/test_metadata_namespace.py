"""Namespace-scoped durable workspace reopen through the existing owners."""

import json
import os
import pickle
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from polystore.disk import DiskBackend
from polystore.filemanager import FileManager
from polystore.metadata_writer import METADATA_CONFIG, MetadataConfig, get_metadata_path
from polystore.virtual_workspace import SourcePixelRef, VirtualWorkspaceBackend


def persist_workspace(root: Path, config: MetadataConfig, index: int = 1) -> np.ndarray:
    source = np.arange(2 * 4 * 5, dtype=np.uint16).reshape(2, 4, 5)
    np.save(root / "source.npy", source)
    ref = SourcePixelRef("disk", "source.npy", (index,))
    config.metadata_path(root).write_text(
        json.dumps(
            {
                config.SUBDIRECTORIES_KEY: {
                    ".": {
                        "workspace_mapping": {"virtual.npy": ref.to_workspace_mapping()},
                    }
                },
            }
        )
    )
    return source[index]


@pytest.mark.parametrize("filename", ["polystore_metadata.json", "custom-metadata.json"])
def test_namespace_reopen_connection_restore_and_native_handoff(tmp_path, filename):
    config = MetadataConfig(METADATA_FILENAME=filename)
    expected = persist_workspace(tmp_path, config)
    backend = VirtualWorkspaceBackend(tmp_path, metadata_config=config)
    assert backend.metadata_config is config
    params = backend.get_connection_params()
    assert params["metadata_config"] is config
    restored = VirtualWorkspaceBackend.from_connection_params(params)
    assert restored.metadata_config is config
    manager = FileManager({"disk": DiskBackend(), "virtual_workspace": restored})
    np.testing.assert_array_equal(
        manager.load(tmp_path / "virtual.npy", backend="virtual_workspace"),
        expected,
    )
    handoff = tmp_path / "native-handoff.pickle"
    with handoff.open("wb") as stream:
        pickle.dump((manager, tmp_path, config, expected), stream)
    result = subprocess.run(
        [
            sys.executable,
            str(Path(__file__).with_name("metadata_namespace_worker.py")),
            str(handoff),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "exact namespace" in result.stdout
    assert sorted(p.name for p in tmp_path.glob("*metadata.json")) == [filename]


def test_new_namespace_declaration_needs_no_workspace_consumer_edits(tmp_path):
    config = MetadataConfig(METADATA_FILENAME="embedded.json", SUBDIRECTORIES_KEY="collections")
    expected = persist_workspace(tmp_path, config)
    backend = VirtualWorkspaceBackend(tmp_path, metadata_config=config)
    manager = FileManager({"disk": DiskBackend(), "virtual_workspace": backend})
    np.testing.assert_array_equal(
        manager.load(tmp_path / "virtual.npy", backend="virtual_workspace"), expected
    )
    assert config.managed_paths(tmp_path)[0] == tmp_path / "embedded.json"


def test_explicit_config_decodes_once_without_mutating_generic_snapshot(tmp_path, monkeypatch):
    snapshot = METADATA_CONFIG
    old_filename = snapshot.METADATA_FILENAME
    monkeypatch.setenv("POLYSTORE_METADATA_FILENAME", "embedding.json")
    config = MetadataConfig()
    persist_workspace(tmp_path, config)
    backend = VirtualWorkspaceBackend(tmp_path, metadata_config=config)
    monkeypatch.setenv("POLYSTORE_METADATA_FILENAME", "later.json")
    assert backend.metadata_config == config
    assert backend._load_mapping()
    assert METADATA_CONFIG is snapshot
    assert snapshot.METADATA_FILENAME == old_filename
    assert get_metadata_path(tmp_path) == snapshot.metadata_path(tmp_path)
    persist_workspace(tmp_path, snapshot)
    assert VirtualWorkspaceBackend(tmp_path).metadata_config is snapshot


def test_namespace_isolation_has_no_alternate_filename_reader(tmp_path):
    persist_workspace(tmp_path, MetadataConfig(METADATA_FILENAME="foreign.json"))
    with pytest.raises(FileNotFoundError, match="missing.json"):
        VirtualWorkspaceBackend(
            tmp_path, metadata_config=MetadataConfig(METADATA_FILENAME="missing.json")
        )


def test_connection_retarget_invalidates_mapping_and_namespace(tmp_path):
    first = MetadataConfig(METADATA_FILENAME="first.json")
    second = MetadataConfig(METADATA_FILENAME="second.json")
    persist_workspace(tmp_path, first, index=0)
    expected = persist_workspace(tmp_path, second)
    backend = VirtualWorkspaceBackend(tmp_path, metadata_config=first)
    backend.set_connection_params(
        VirtualWorkspaceBackend(tmp_path, metadata_config=second).get_connection_params()
    )
    manager = FileManager({"disk": DiskBackend(), "virtual_workspace": backend})
    np.testing.assert_array_equal(
        manager.load(tmp_path / "virtual.npy", backend="virtual_workspace"), expected
    )
    persist_workspace(tmp_path, second, index=0)
    path = second.metadata_path(tmp_path)
    os.utime(path, (path.stat().st_atime, path.stat().st_mtime + 1))
    np.testing.assert_array_equal(
        manager.load(tmp_path / "virtual.npy", backend="virtual_workspace"), expected - 20
    )
