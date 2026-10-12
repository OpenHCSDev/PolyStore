"""The backend that owns a file says what bytes it holds, or why it cannot."""

import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

from polystore import FileContent, FileManager, HashedFileContent, UnhashedFileContent
from polystore.bioformats_storage import BioFormatsPlaneRef, BioFormatsStorageBackend
from polystore.exceptions import StorageResolutionError
from polystore.file_content import HASH_CHUNK_BYTES
from polystore.metadata_writer import METADATA_CONFIG
from polystore.omero_local import OMEROLocalBackend
from polystore.virtual_workspace import SourcePixelRef, VirtualWorkspaceBackend

SHA256_OF_NOTHING = hashlib.sha256(b"").hexdigest()


def _write(path: Path, data: bytes) -> Path:
    path.write_bytes(data)
    return path


def _workspace(root: Path, mapping: dict[str, SourcePixelRef], registry: dict) -> FileManager:
    METADATA_CONFIG.metadata_path(root).write_text(
        json.dumps(
            {
                METADATA_CONFIG.SUBDIRECTORIES_KEY: {
                    ".": {
                        "workspace_mapping": {
                            virtual: ref.to_workspace_mapping() for virtual, ref in mapping.items()
                        }
                    }
                }
            }
        )
    )
    return FileManager({**registry, "virtual_workspace": VirtualWorkspaceBackend(root)})


def test_disk_file_hash_and_size_match_its_bytes(file_manager, tmp_path):
    data = b"plate A01 measurements\n" * 7
    path = _write(tmp_path / "results.csv", data)

    content = file_manager.file_content(path, "disk")

    assert content == HashedFileContent(
        sha256=hashlib.sha256(data).hexdigest(), size_bytes=len(data)
    )


def test_one_changed_byte_changes_the_hash_but_not_the_size(file_manager, tmp_path):
    path = _write(tmp_path / "image.bin", b"\x00" * 64)
    before = file_manager.file_content(path, "disk")

    _write(path, b"\x00" * 63 + b"\x01")
    after = file_manager.file_content(path, "disk")

    assert after.size_bytes == before.size_bytes == 64
    assert after.sha256 != before.sha256


def test_empty_disk_file_is_hashed_not_unhashed(file_manager, tmp_path):
    path = _write(tmp_path / "empty.txt", b"")

    assert file_manager.file_content(path, "disk") == HashedFileContent(SHA256_OF_NOTHING, 0)


def test_hash_streams_a_file_larger_than_one_chunk(file_manager, tmp_path, monkeypatch):
    data = bytes(range(256)) * (HASH_CHUNK_BYTES // 256 * 2) + b"tail"
    path = _write(tmp_path / "large.bin", data)
    hashed_chunk_sizes: list[int] = []
    real_sha256 = hashlib.sha256

    class RecordingSha256:
        def __init__(self):
            self._digest = real_sha256()

        def update(self, chunk):
            hashed_chunk_sizes.append(len(chunk))
            self._digest.update(chunk)

        def hexdigest(self):
            return self._digest.hexdigest()

    def whole_file_read(self):
        raise AssertionError("file_content must not read the whole file into memory")

    monkeypatch.setattr(hashlib, "sha256", RecordingSha256)
    monkeypatch.setattr(Path, "read_bytes", whole_file_read)

    content = file_manager.file_content(path, "disk")

    assert hashed_chunk_sizes == [HASH_CHUNK_BYTES, HASH_CHUNK_BYTES, len(b"tail")]
    assert content == HashedFileContent(real_sha256(data).hexdigest(), len(data))


def test_disk_directory_is_unhashed_with_a_reason(file_manager, tmp_path):
    content = file_manager.file_content(tmp_path, "disk")

    assert isinstance(content, UnhashedFileContent)
    assert "is a directory" in content.reason
    assert str(tmp_path) in content.reason


def test_missing_disk_file_raises(file_manager, tmp_path):
    with pytest.raises(FileNotFoundError):
        file_manager.file_content(tmp_path / "absent.tif", "disk")


def test_relative_disk_path_is_refused_rather_than_resolved_against_cwd(
    file_manager, tmp_path, monkeypatch
):
    _write(tmp_path / "relative.bin", b"x")
    monkeypatch.chdir(tmp_path)

    with pytest.raises(StorageResolutionError, match="absolute"):
        file_manager.file_content("relative.bin", "disk")


def test_memory_file_is_unhashed_and_the_reason_names_the_backend(file_manager, tmp_path):
    # A same-named disk file must not be mistaken for the memory entry.
    path = _write(tmp_path / "array.npy", b"disk bytes")
    file_manager.ensure_directory(tmp_path, "memory")
    file_manager.save(np.zeros((2, 2)), path, "memory")

    content = file_manager.file_content(path, "memory")

    assert isinstance(content, UnhashedFileContent)
    assert "MemoryStorageBackend" in content.reason


def test_memory_directory_is_unhashed_and_missing_memory_path_raises(file_manager):
    file_manager.ensure_directory("/plate/images", "memory")

    directory = file_manager.file_content("/plate/images", "memory")

    assert isinstance(directory, UnhashedFileContent)
    assert "is a directory" in directory.reason
    with pytest.raises(FileNotFoundError):
        file_manager.file_content("/plate/images/absent.npy", "memory")


def test_virtual_file_has_the_content_of_the_whole_source_file_it_names(registry, tmp_path):
    np.save(tmp_path / "source.npy", np.arange(6, dtype=np.uint16))
    data = (tmp_path / "source.npy").read_bytes()
    manager = _workspace(tmp_path, {"virtual.npy": SourcePixelRef("disk", "source.npy")}, registry)

    content = manager.file_content(tmp_path / "virtual.npy", "virtual_workspace")

    assert content == HashedFileContent(hashlib.sha256(data).hexdigest(), len(data))


def test_virtual_file_that_is_part_of_a_source_file_is_unhashed(registry, tmp_path):
    np.save(tmp_path / "source.npy", np.zeros((2, 4, 5), dtype=np.uint16))
    manager = _workspace(
        tmp_path, {"images/plane.npy": SourcePixelRef("disk", "source.npy", (1,))}, registry
    )

    plane = manager.file_content(tmp_path / "images" / "plane.npy", "virtual_workspace")
    directory = manager.file_content(tmp_path / "images", "virtual_workspace")

    assert isinstance(plane, UnhashedFileContent)
    assert "selects part of its source file" in plane.reason
    assert isinstance(directory, UnhashedFileContent)
    assert "is a directory" in directory.reason
    with pytest.raises(FileNotFoundError):
        manager.file_content(tmp_path / "images" / "absent.npy", "virtual_workspace")


def test_bioformats_plane_is_unhashed_because_it_is_part_of_its_container(tmp_path):
    container = _write(tmp_path / "plate.czi", b"two planes in one container")
    manager = FileManager({"bioformats": BioFormatsStorageBackend()})

    content = manager.file_content(
        BioFormatsPlaneRef(container, 0, 1).to_backend_address(), "bioformats"
    )

    assert isinstance(content, UnhashedFileContent)
    assert "BioFormatsStorageBackend" in content.reason
    assert "plane 1" in content.reason
    with pytest.raises(FileNotFoundError):
        manager.file_content(
            BioFormatsPlaneRef(tmp_path / "absent.czi", 0, 0).to_backend_address(), "bioformats"
        )


def test_backend_that_cannot_read_files_is_refused(registry):
    sink = object.__new__(OMEROLocalBackend)
    manager = FileManager({**registry, "omero_local": sink})

    with pytest.raises(StorageResolutionError, match="not a DataSource"):
        manager.file_content("/plate/image.tif", "omero_local")


def test_each_answer_is_registered_under_the_key_that_names_it_in_stored_data():
    assert dict(FileContent.__registry__) == {
        "hashed": HashedFileContent,
        "not_hashed": UnhashedFileContent,
    }


@pytest.mark.parametrize(
    "sha256, size_bytes",
    [
        (SHA256_OF_NOTHING.upper(), 0),
        (SHA256_OF_NOTHING[:-1], 0),
        (SHA256_OF_NOTHING[:-1] + "g", 0),
        (SHA256_OF_NOTHING, -1),
        (SHA256_OF_NOTHING, True),
        (SHA256_OF_NOTHING, 1.0),
    ],
)
def test_hashed_content_refuses_a_malformed_hash_or_size(sha256, size_bytes):
    with pytest.raises(ValueError):
        HashedFileContent(sha256=sha256, size_bytes=size_bytes)


@pytest.mark.parametrize("reason", ["", "   "])
def test_unhashed_content_requires_a_reason(reason):
    with pytest.raises(ValueError):
        UnhashedFileContent(reason=reason)
