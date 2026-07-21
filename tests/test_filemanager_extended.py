"""
Extended FileManager tests to boost coverage.

Tests cover:
- Error handling (invalid backends, None registry, etc.)
- Edge cases (load/save with exceptions)
- Directory operations (exists, is_file, is_dir, mkdir, delete)
- Advanced features (symlinks, find, mirror)
"""

import json
import pickle
import tempfile
import shutil
from pathlib import Path
import numpy as np
import pytest
import polystore

from polystore import FileManager
from polystore.constants import Backend
from polystore.disk import DiskBackend
from polystore.exceptions import StorageResolutionError
from polystore.memory import MemoryBackend
from polystore.metadata_writer import get_metadata_path
from polystore.virtual_workspace import VirtualWorkspaceBackend
from polystore.zarr import ZarrStorageBackend


@pytest.fixture
def registry():
    """Create a backend registry with disk and memory backends."""
    return {
        Backend.DISK.value: DiskBackend(),
        Backend.MEMORY.value: MemoryBackend(),
    }


@pytest.fixture
def file_manager(registry):
    """Create a FileManager instance."""
    return FileManager(registry)


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


class TestFileManagerInit:
    """Test FileManager initialization."""

    def test_init_with_none_registry_raises(self):
        """Test that initializing with None registry raises ValueError."""
        with pytest.raises(ValueError, match="Registry must be provided"):
            FileManager(None)

    def test_init_with_valid_registry(self, registry):
        """FileManager owns an execution-local copy of the submitted registry."""
        fm = FileManager(registry)
        assert fm.registry == registry
        assert fm.registry is not registry

    def test_register_backend_updates_local_registry_and_binds_it(self, registry):
        """Backend registration is the sole mutation and binding boundary."""
        fm = FileManager(registry)
        virtual_backend = object()

        fm.register_backend("virtual_probe", virtual_backend)

        assert fm.registry["virtual_probe"] is virtual_backend

    def test_source_pixel_ref_has_exact_format_neutral_fields(self):
        """Workspace references carry a backend address and ordered axis indices."""
        source_pixel_ref = getattr(polystore, "SourcePixelRef", None)
        assert source_pixel_ref is not None

        ref = source_pixel_ref(
            backend=Backend.DISK.value,
            backend_address="real.npy",
            source_axis_indices=(1, 3),
        )

        assert tuple(ref.__dataclass_fields__) == (
            "backend",
            "backend_address",
            "source_axis_indices",
        )
        assert source_pixel_ref.from_workspace_mapping(
            ref.to_workspace_mapping()
        ) == ref

    def test_virtual_workspace_rejects_string_mapping(self, tmp_path):
        """Legacy string workspace mappings are not a second transport shape."""
        (tmp_path / "real.npy").write_bytes(b"placeholder")
        get_metadata_path(tmp_path).write_text(
            json.dumps(
                {
                    "subdirectories": {
                        ".": {
                            "workspace_mapping": {"virtual.npy": "real.npy"},
                            "available_backends": {
                                Backend.VIRTUAL_WORKSPACE.value: True
                            },
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises((TypeError, ValueError), match="SourcePixelRef|structured"):
            VirtualWorkspaceBackend(tmp_path)

    def test_pickle_preserves_picklable_registry_backends(self, tmp_path):
        """Pickle preserves and rebinds the exact execution-local registry."""
        source_pixel_ref = getattr(polystore, "SourcePixelRef", None)
        assert source_pixel_ref is not None
        source = np.arange(9).reshape(3, 3)
        np.save(tmp_path / "real.npy", source)
        ref = source_pixel_ref(
            backend=Backend.DISK.value,
            backend_address="real.npy",
            source_axis_indices=(),
        )
        get_metadata_path(tmp_path).write_text(
            json.dumps(
                {
                    "subdirectories": {
                        ".": {
                            "workspace_mapping": {
                                "virtual.npy": ref.to_workspace_mapping()
                            },
                            "available_backends": {
                                Backend.VIRTUAL_WORKSPACE.value: True
                            },
                        }
                    }
                }
            ),
            encoding="utf-8",
        )

        filemanager = FileManager(
            {
                Backend.DISK.value: DiskBackend(),
                Backend.ZARR.value: ZarrStorageBackend(),
            }
        )
        filemanager.register_backend(
            Backend.VIRTUAL_WORKSPACE.value,
            VirtualWorkspaceBackend(tmp_path),
        )
        expected_registry_keys = set(filemanager.registry)

        restored = pickle.loads(pickle.dumps(filemanager))

        assert set(restored.registry) == expected_registry_keys
        assert restored.registry is not filemanager.registry
        assert restored.registry[Backend.VIRTUAL_WORKSPACE.value].plate_root == tmp_path
        assert (
            restored.registry[Backend.ZARR.value].config
            == filemanager.registry[Backend.ZARR.value].config
        )
        np.testing.assert_array_equal(
            restored.load(
                tmp_path / "virtual.npy",
                backend=Backend.VIRTUAL_WORKSPACE.value,
            ),
            source,
        )

    def test_virtual_workspace_dispatches_each_ref_to_its_declared_local_backend(
        self,
        tmp_path,
    ):
        """One virtual workspace may contain source refs from mixed backends."""
        source_pixel_ref = getattr(polystore, "SourcePixelRef", None)
        assert source_pixel_ref is not None

        disk_payload = np.arange(6).reshape(2, 3)
        memory_payload = np.arange(6, 12).reshape(2, 3)
        np.save(tmp_path / "disk.npy", disk_payload)

        filemanager = FileManager(
            {
                Backend.DISK.value: DiskBackend(),
                Backend.MEMORY.value: MemoryBackend(),
            }
        )
        filemanager.ensure_directory("/", backend=Backend.MEMORY.value)
        filemanager.save(
            memory_payload,
            "/memory.npy",
            backend=Backend.MEMORY.value,
        )
        refs = {
            "from_disk.npy": source_pixel_ref(
                backend=Backend.DISK.value,
                backend_address="disk.npy",
            ).to_workspace_mapping(),
            "from_memory.npy": source_pixel_ref(
                backend=Backend.MEMORY.value,
                backend_address="/memory.npy",
            ).to_workspace_mapping(),
        }
        get_metadata_path(tmp_path).write_text(
            json.dumps(
                {
                    "subdirectories": {
                        ".": {
                            "workspace_mapping": refs,
                            "available_backends": {
                                Backend.VIRTUAL_WORKSPACE.value: True
                            },
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        filemanager.register_backend(
            Backend.VIRTUAL_WORKSPACE.value,
            VirtualWorkspaceBackend(tmp_path),
        )

        np.testing.assert_array_equal(
            filemanager.load(
                tmp_path / "from_disk.npy",
                backend=Backend.VIRTUAL_WORKSPACE.value,
            ),
            disk_payload,
        )
        np.testing.assert_array_equal(
            filemanager.load(
                tmp_path / "from_memory.npy",
                backend=Backend.VIRTUAL_WORKSPACE.value,
            ),
            memory_payload,
        )


class TestFileManagerBackendResolution:
    """Test backend resolution and error handling."""

    def test_get_backend_unknown_raises(self, file_manager):
        """Test that requesting unknown backend raises StorageResolutionError."""
        with pytest.raises(StorageResolutionError, match="not found in registry"):
            file_manager._get_backend("unknown_backend")

    def test_get_backend_case_insensitive(self, file_manager):
        """Test that backend names are case-insensitive."""
        backend_lower = file_manager._get_backend("disk")
        backend_upper = file_manager._get_backend("DISK")
        backend_mixed = file_manager._get_backend("Disk")

        assert backend_lower is backend_upper
        assert backend_lower is backend_mixed

    def test_get_backend_memory(self, file_manager):
        """Test getting memory backend."""
        backend = file_manager._get_backend("memory")
        assert backend._backend_type == "memory"

    def test_get_backend_disk(self, file_manager):
        """Test getting disk backend."""
        backend = file_manager._get_backend("disk")
        assert backend._backend_type == "disk"


class TestFileManagerLoadErrors:
    """Test error handling in load operations."""

    def test_load_with_invalid_backend(self, file_manager, temp_dir):
        """Test that load with invalid backend raises StorageResolutionError."""
        test_file = Path(temp_dir) / "test.npy"

        with pytest.raises(StorageResolutionError):
            file_manager.load(test_file, backend="invalid_backend")

    def test_load_nonexistent_file(self, file_manager, temp_dir):
        """Test loading nonexistent file raises error."""
        nonexistent = Path(temp_dir) / "nonexistent.npy"

        with pytest.raises(Exception):  # Could be FileNotFoundError or StorageResolutionError
            file_manager.load(nonexistent, backend="disk")

    def test_load_batch_with_invalid_backend(self, file_manager, temp_dir):
        """Test that load_batch with invalid backend raises error."""
        paths = [str(Path(temp_dir) / f"test_{i}.npy") for i in range(3)]

        with pytest.raises(StorageResolutionError):
            file_manager.load_batch(paths, backend="invalid_backend")


class TestFileManagerSaveErrors:
    """Test error handling in save operations."""

    def test_save_with_invalid_backend(self, file_manager, temp_dir):
        """Test that save with invalid backend raises StorageResolutionError."""
        data = np.zeros((10, 10))
        output_path = Path(temp_dir) / "test.npy"

        with pytest.raises(StorageResolutionError):
            file_manager.save(data, output_path, backend="invalid_backend")

    def test_save_batch_with_invalid_backend(self, file_manager, temp_dir):
        """Test that save_batch with invalid backend raises error."""
        data_list = [np.zeros((10, 10)) for _ in range(3)]
        paths = [str(Path(temp_dir) / f"test_{i}.npy") for i in range(3)]

        with pytest.raises(StorageResolutionError):
            file_manager.save_batch(data_list, paths, backend="invalid_backend")

    def test_disk_writes_opaque_bytes_without_extension_registration(
        self,
        file_manager,
        temp_dir,
    ):
        payload = b"SQLite format 3\x00\x01\x02"
        output_path = Path(temp_dir) / "analysis.sqlite"

        file_manager.save(payload, output_path, backend="disk")

        assert output_path.read_bytes() == payload

    def test_disk_batch_preserves_opaque_bytes_save_semantics(
        self,
        file_manager,
        temp_dir,
    ):
        payloads = [b"first opaque payload", b"second opaque payload"]
        output_paths = [
            Path(temp_dir) / "first.bin",
            Path(temp_dir) / "second.bin",
        ]

        file_manager.save_batch(payloads, output_paths, backend="disk")

        assert [path.read_bytes() for path in output_paths] == payloads

    def test_disk_unknown_suffix_still_rejects_non_bytes_payload(
        self,
        file_manager,
        temp_dir,
    ):
        output_path = Path(temp_dir) / "analysis.sqlite"

        with pytest.raises(StorageResolutionError) as exc_info:
            file_manager.save({"not": "opaque bytes"}, output_path, backend="disk")

        assert isinstance(exc_info.value.__cause__, ValueError)
        assert "extension" in str(exc_info.value.__cause__)


class TestFileManagerDirectoryOps:
    """Test directory operations."""

    def test_exists_file(self, file_manager, temp_dir):
        """Test exists() for files."""
        # Create a file
        test_file = Path(temp_dir) / "test.npy"
        data = np.zeros((5, 5))
        file_manager.save(data, test_file, backend="disk")

        # Check existence
        assert file_manager.exists(test_file, backend="disk")

        # Check non-existent
        nonexistent = Path(temp_dir) / "nonexistent.npy"
        assert not file_manager.exists(nonexistent, backend="disk")

    def test_exists_directory(self, file_manager, temp_dir):
        """Test exists() for directories."""
        # Create a directory
        test_dir = Path(temp_dir) / "subdir"
        file_manager.ensure_directory(test_dir, backend="disk")

        assert file_manager.exists(test_dir, backend="disk")

    def test_is_file(self, file_manager, temp_dir):
        """Test is_file() check."""
        # Create a file
        test_file = Path(temp_dir) / "test.npy"
        data = np.zeros((5, 5))
        file_manager.save(data, test_file, backend="disk")

        assert file_manager.is_file(test_file, backend="disk")
        assert not file_manager.is_dir(test_file, backend="disk")

    def test_is_dir(self, file_manager, temp_dir):
        """Test is_dir() check."""
        # Create a directory
        test_dir = Path(temp_dir) / "subdir"
        file_manager.ensure_directory(test_dir, backend="disk")

        assert file_manager.is_dir(test_dir, backend="disk")
        assert not file_manager.is_file(test_dir, backend="disk")

    def test_ensure_directory(self, file_manager, temp_dir):
        """Test ensure_directory() creates nested directories."""
        new_dir = Path(temp_dir) / "level1" / "level2" / "level3"
        file_manager.ensure_directory(new_dir, backend="disk")

        assert new_dir.exists()
        assert new_dir.is_dir()

    def test_list_dir(self, file_manager, temp_dir):
        """Test list_dir() returns directory contents."""
        # Create some files and subdirs
        file_manager.save(np.zeros((5, 5)), Path(temp_dir) / "file1.npy", backend="disk")
        file_manager.save(np.ones((5, 5)), Path(temp_dir) / "file2.npy", backend="disk")
        file_manager.ensure_directory(Path(temp_dir) / "subdir", backend="disk")

        entries = file_manager.list_dir(temp_dir, backend="disk")

        assert "file1.npy" in entries
        assert "file2.npy" in entries
        assert "subdir" in entries

    def test_delete_file(self, file_manager, temp_dir):
        """Test delete() removes files."""
        # Create a file
        test_file = Path(temp_dir) / "test.npy"
        file_manager.save(np.zeros((5, 5)), test_file, backend="disk")
        assert test_file.exists()

        # Delete it
        file_manager.delete(test_file, backend="disk")
        assert not test_file.exists()

    def test_delete_directory(self, file_manager, temp_dir):
        """Test delete() removes empty directories."""
        # Create a directory
        test_dir = Path(temp_dir) / "empty_dir"
        file_manager.ensure_directory(test_dir, backend="disk")
        assert test_dir.exists()

        # Delete it
        file_manager.delete(test_dir, backend="disk")
        assert not test_dir.exists()


class TestFileManagerSymlinks:
    """Test symlink operations."""

    def test_create_symlink(self, file_manager, temp_dir):
        """Test creating a symlink."""
        # Create source file
        source = Path(temp_dir) / "source.npy"
        file_manager.save(np.zeros((5, 5)), source, backend="disk")

        # Create symlink
        link = Path(temp_dir) / "link.npy"
        file_manager.create_symlink(source, link, backend="disk")

        # Verify symlink exists
        assert link.exists()
        assert file_manager.is_symlink(link, backend="disk")


class TestFileManagerFind:
    """Test find operations."""

    def test_find_file_recursive(self, file_manager, temp_dir):
        """Test find_file_recursive() locates specific file."""
        # Create test structure
        file_manager.ensure_directory(Path(temp_dir) / "dir1", backend="disk")
        file_manager.ensure_directory(Path(temp_dir) / "dir2", backend="disk")

        file_manager.save(np.zeros((5, 5)), Path(temp_dir) / "dir1" / "target.npy", backend="disk")
        file_manager.save(np.ones((5, 5)), Path(temp_dir) / "dir2" / "other.npy", backend="disk")

        # Find specific file
        found = file_manager.find_file_recursive(temp_dir, "target.npy", backend="disk")

        assert found is not None
        assert "target.npy" in str(found)


class TestFileManagerMemoryBackend:
    """Test FileManager with memory backend."""

    def test_save_load_memory(self, file_manager):
        """Test save/load with memory backend."""
        data = np.random.rand(10, 10)
        path = "/test_save_load.npy"

        # Ensure parent directory exists in memory
        file_manager.ensure_directory("/", backend="memory")

        file_manager.save(data, path, backend="memory")
        loaded = file_manager.load(path, backend="memory")

        np.testing.assert_array_equal(loaded, data)

    def test_batch_memory(self, file_manager):
        """Test batch operations with memory backend."""
        data_list = [np.random.rand(5, 5) for _ in range(3)]
        paths = [f"/batch_{i}.npy" for i in range(3)]

        # Ensure parent directory exists
        file_manager.ensure_directory("/", backend="memory")

        file_manager.save_batch(data_list, paths, backend="memory")
        loaded_list = file_manager.load_batch(paths, backend="memory")

        assert len(loaded_list) == 3
        for original, loaded in zip(data_list, loaded_list):
            np.testing.assert_array_equal(loaded, original)

    def test_exists_memory(self, file_manager):
        """Test exists() with memory backend."""
        path = "/test_exists.npy"

        # Ensure parent directory exists
        file_manager.ensure_directory("/", backend="memory")

        # Before saving
        assert not file_manager.exists(path, backend="memory")

        # After saving
        file_manager.save(np.zeros((5, 5)), path, backend="memory")
        assert file_manager.exists(path, backend="memory")


class TestFileManagerListImageFiles:
    """Test list_image_files with natural sorting."""

    def test_list_image_files_disk(self, file_manager, temp_dir):
        """Test list_image_files with disk backend."""
        # Create image files with natural sort order
        for i in [1, 2, 10, 20]:
            path = Path(temp_dir) / f"image_{i}.tif"
            file_manager.save(np.zeros((10, 10)), path, backend="disk")

        files = file_manager.list_image_files(temp_dir, backend="disk")

        # Should be naturally sorted: image_1, image_2, image_10, image_20
        assert len(files) == 4
        assert "image_1.tif" in files[0]
        assert "image_2.tif" in files[1]
        assert "image_10.tif" in files[2]
        assert "image_20.tif" in files[3]

    def test_list_image_files_with_pattern(self, file_manager, temp_dir):
        """Test list_image_files with pattern filter."""
        # Create mixed files
        file_manager.save(np.zeros((5, 5)), Path(temp_dir) / "test.tif", backend="disk")
        file_manager.save(np.ones((5, 5)), Path(temp_dir) / "data.npy", backend="disk")

        # List only tif files
        files = file_manager.list_image_files(temp_dir, backend="disk", extensions={'.tif'})

        assert len(files) == 1
        assert "test.tif" in files[0]

    def test_list_image_files_recursive(self, file_manager, temp_dir):
        """Test list_image_files with recursive search."""
        # Create nested structure
        subdir = Path(temp_dir) / "subdir"
        file_manager.ensure_directory(subdir, backend="disk")

        file_manager.save(np.zeros((5, 5)), Path(temp_dir) / "root.tif", backend="disk")
        file_manager.save(np.ones((5, 5)), subdir / "nested.tif", backend="disk")

        # Non-recursive should find 1
        files_nonrec = file_manager.list_image_files(temp_dir, backend="disk", recursive=False)
        assert len(files_nonrec) == 1

        # Recursive should find 2
        files_rec = file_manager.list_image_files(temp_dir, backend="disk", recursive=True)
        assert len(files_rec) == 2
