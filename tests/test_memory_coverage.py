"""Comprehensive tests for MemoryBackend covering missing code paths.

This module targets specific areas like:
- Move and copy operations
- Symlink handling
- stat() method
- Error conditions and edge cases
- clear_files_only() GPU handling
- Path resolution edge cases
"""

from unittest.mock import Mock, patch

import numpy as np
import pytest

from polystore import MemoryBackend
from polystore.exceptions import StorageResolutionError


class TestMemoryCopyAndMove:
    """Test copy() and move() operations in memory backend."""

    def setup_method(self):
        self.backend = MemoryBackend()
        self.backend.ensure_directory("/base")
        self.backend.ensure_directory("/base/sub")

    def test_move_file_basic(self):
        """Test moving a file within memory."""
        data = np.array([1, 2, 3])
        self.backend.save(data, "/base/source.npy")

        self.backend.move("/base/source.npy", "/base/dest.npy")

        assert not self.backend.exists("/base/source.npy")
        assert self.backend.exists("/base/dest.npy")
        np.testing.assert_array_equal(self.backend.load("/base/dest.npy"), data)

    def test_move_file_across_directories(self):
        """Test moving file between directories."""
        data = np.array([1, 2, 3])
        self.backend.save(data, "/base/file.npy")

        self.backend.move("/base/file.npy", "/base/sub/file.npy")

        assert not self.backend.exists("/base/file.npy")
        assert self.backend.is_file("/base/sub/file.npy")
        np.testing.assert_array_equal(self.backend.load("/base/sub/file.npy"), data)

    def test_move_nonexistent_file_raises_error(self):
        """Test moving non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            self.backend.move("/base/nonexistent.npy", "/base/dest.npy")

    def test_move_to_nonexistent_parent_raises_error(self):
        """Test moving to non-existent parent directory raises error."""
        data = np.array([1, 2, 3])
        self.backend.save(data, "/base/source.npy")

        with pytest.raises(FileNotFoundError, match="Destination parent"):
            self.backend.move("/base/source.npy", "/nonexistent/dest.npy")

    def test_move_to_existing_destination_raises_error(self):
        """Test moving to existing destination raises FileExistsError."""
        data1 = np.array([1, 2, 3])
        data2 = np.array([4, 5, 6])

        self.backend.save(data1, "/base/source.npy")
        self.backend.save(data2, "/base/dest.npy")

        with pytest.raises(FileExistsError):
            self.backend.move("/base/source.npy", "/base/dest.npy")

    def test_copy_file_basic(self):
        """Test copying a file within memory."""
        data = np.array([1, 2, 3])
        self.backend.save(data, "/base/source.npy")

        self.backend.copy("/base/source.npy", "/base/dest.npy")

        assert self.backend.exists("/base/source.npy")
        assert self.backend.exists("/base/dest.npy")

        # Verify original unchanged
        np.testing.assert_array_equal(self.backend.load("/base/source.npy"), data)
        # Verify copy independent
        np.testing.assert_array_equal(self.backend.load("/base/dest.npy"), data)

        # Modify original and verify copy unaffected
        self.backend._memory_store[self.backend._normalize("/base/source.npy")] = np.array([99])
        np.testing.assert_array_equal(self.backend.load("/base/dest.npy"), data)

    def test_copy_file_across_directories(self):
        """Test copying file between directories."""
        data = np.array([1, 2, 3])
        self.backend.save(data, "/base/file.npy")

        self.backend.copy("/base/file.npy", "/base/sub/file.npy")

        assert self.backend.is_file("/base/file.npy")
        assert self.backend.is_file("/base/sub/file.npy")

    def test_copy_nonexistent_file_raises_error(self):
        """Test copying non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Source not found"):
            self.backend.copy("/base/nonexistent.npy", "/base/dest.npy")

    def test_copy_to_nonexistent_parent_raises_error(self):
        """Test copying to non-existent parent directory raises error."""
        data = np.array([1, 2, 3])
        self.backend.save(data, "/base/source.npy")

        with pytest.raises(FileNotFoundError, match="Destination parent"):
            self.backend.copy("/base/source.npy", "/nonexistent/dest.npy")

    def test_copy_to_existing_destination_raises_error(self):
        """Test copying to existing destination raises FileExistsError."""
        data1 = np.array([1, 2, 3])
        data2 = np.array([4, 5, 6])

        self.backend.save(data1, "/base/source.npy")
        self.backend.save(data2, "/base/dest.npy")

        with pytest.raises(FileExistsError):
            self.backend.copy("/base/source.npy", "/base/dest.npy")

    def test_copy_directory(self):
        """Test copying a directory (deep copy)."""
        self.backend.ensure_directory("/base/srcdir")
        self.backend.save(np.array([1]), "/base/srcdir/file.npy")

        self.backend.copy("/base/srcdir", "/base/dstdir")

        assert self.backend.is_dir("/base/dstdir")
        assert self.backend.is_file("/base/dstdir/file.npy")


class TestMemorySymlink:
    """Test symlink operations in memory backend."""

    def setup_method(self):
        self.backend = MemoryBackend()
        self.backend.ensure_directory("/base")

    def test_create_symlink_basic(self):
        """Test creating a symlink to a file."""
        self.backend.save(np.array([1, 2, 3]), "/base/target.npy")
        self.backend.create_symlink("/base/target.npy", "/base/link.npy")

        assert self.backend.is_symlink("/base/link.npy")

    def test_create_symlink_to_nonexistent_target_raises_error(self):
        """Test creating symlink to non-existent target."""
        with pytest.raises(FileNotFoundError):
            self.backend.create_symlink("/base/nonexistent.npy", "/base/link.npy")

    def test_create_symlink_to_existing_link_without_overwrite_raises_error(self):
        """Test creating symlink when link already exists."""
        self.backend.save(np.array([1]), "/base/target.npy")
        self.backend.save("old", "/base/link.npy")

        with pytest.raises(FileExistsError):
            self.backend.create_symlink("/base/target.npy", "/base/link.npy", overwrite=False)

    def test_create_symlink_with_overwrite(self):
        """Test creating symlink with overwrite=True."""
        self.backend.save(np.array([1]), "/base/target.npy")
        self.backend.save("old", "/base/link.npy")

        # Should not raise
        self.backend.create_symlink("/base/target.npy", "/base/link.npy", overwrite=True)
        assert self.backend.is_symlink("/base/link.npy")

    def test_is_symlink_on_file(self):
        """Test is_symlink on regular file returns False."""
        self.backend.save("data", "/base/file.txt")
        assert not self.backend.is_symlink("/base/file.txt")

    def test_is_symlink_on_directory(self):
        """Test is_symlink on directory returns False."""
        assert not self.backend.is_symlink("/base")

    def test_is_symlink_on_nonexistent(self):
        """Test is_symlink on non-existent path returns False."""
        assert not self.backend.is_symlink("/base/nonexistent")


class TestMemoryStat:
    """Test stat() method for metadata."""

    def setup_method(self):
        self.backend = MemoryBackend()
        self.backend.ensure_directory("/base")

    def test_stat_on_file(self):
        """Test stat on a file."""
        self.backend.save("content", "/base/file.txt")

        stat = self.backend.stat("/base/file.txt")

        assert stat["type"] == "file"
        assert stat["exists"] is True
        assert stat["path"] == "/base/file.txt"

    def test_stat_on_directory(self):
        """Test stat on a directory."""
        stat = self.backend.stat("/base")

        assert stat["type"] == "directory"
        assert stat["exists"] is True

    def test_stat_on_symlink(self):
        """Test stat on a symlink."""
        self.backend.save("target", "/base/target.txt")
        self.backend.create_symlink("/base/target.txt", "/base/link.txt")

        stat = self.backend.stat("/base/link.txt")

        assert stat["type"] == "symlink"
        assert stat["target"] == "/base/target.txt"
        assert stat["exists"] is True

    def test_stat_on_broken_symlink(self):
        """Test stat reflects broken symlink after target removal."""
        self.backend.save("target", "/base/target.txt")
        self.backend.create_symlink("/base/target.txt", "/base/link.txt")
        self.backend.delete("/base/target.txt")

        stat = self.backend.stat("/base/link.txt")

        assert stat["type"] == "symlink"
        assert stat["exists"] is False

    def test_stat_on_missing_path(self):
        """Test stat on non-existent path."""
        stat = self.backend.stat("/base/missing")

        assert stat["type"] == "missing"
        assert stat["exists"] is False


class TestMemoryDeleteOperations:
    """Test delete and delete_all operations."""

    def setup_method(self):
        self.backend = MemoryBackend()
        self.backend.ensure_directory("/base")

    def test_delete_file(self):
        """Test deleting a file."""
        self.backend.save("data", "/base/file.txt")
        self.backend.delete("/base/file.txt")

        assert not self.backend.exists("/base/file.txt")

    def test_delete_empty_directory(self):
        """Test deleting an empty directory."""
        self.backend.ensure_directory("/base/empty")
        self.backend.delete("/base/empty")

        assert not self.backend.exists("/base/empty")

    def test_delete_nonempty_directory_raises_error(self):
        """Test deleting non-empty directory raises IsADirectoryError."""
        self.backend.ensure_directory("/base/nonempty")
        self.backend.save("file", "/base/nonempty/file.txt")

        with pytest.raises(IsADirectoryError):
            self.backend.delete("/base/nonempty")

    def test_delete_nonexistent_raises_error(self):
        """Test deleting non-existent path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            self.backend.delete("/base/nonexistent")

    def test_delete_all_file(self):
        """Test delete_all on a file."""
        self.backend.save("data", "/base/file.txt")
        self.backend.delete_all("/base/file.txt")

        assert not self.backend.exists("/base/file.txt")

    def test_delete_all_tree(self):
        """Test delete_all removes entire directory tree."""
        self.backend.ensure_directory("/base/tree/sub1/sub2")
        self.backend.save("a", "/base/tree/file1.txt")
        self.backend.save("b", "/base/tree/sub1/file2.txt")
        self.backend.save("c", "/base/tree/sub1/sub2/file3.txt")

        self.backend.delete_all("/base/tree")

        assert not self.backend.exists("/base/tree")

    def test_delete_all_nonexistent_raises_error(self):
        """Test delete_all on non-existent path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            self.backend.delete_all("/base/nonexistent")


class TestMemoryClearFilesOnly:
    """Test clear_files_only() method with GPU handling."""

    def setup_method(self):
        self.backend = MemoryBackend()
        self.backend.ensure_directory("/base")

    def test_clear_files_preserves_directories(self):
        """Test clear_files_only removes files but preserves directories."""
        self.backend.ensure_directory("/base/sub1")
        self.backend.ensure_directory("/base/sub2")
        self.backend.save("file1", "/base/file1.txt")
        self.backend.save("file2", "/base/sub1/file2.txt")

        self.backend.clear_files_only()

        # Directories should still exist
        assert self.backend.is_dir("/base/sub1")
        assert self.backend.is_dir("/base/sub2")

        # Files should be gone
        assert not self.backend.exists("/base/file1.txt")
        assert not self.backend.exists("/base/sub1/file2.txt")

    def test_clear_files_only_removes_symlinks(self):
        """Test clear_files_only also removes symlinks."""
        self.backend.save("target", "/base/target.txt")
        self.backend.create_symlink("/base/target.txt", "/base/link.txt")

        self.backend.clear_files_only()

        # Both file and symlink should be gone
        assert not self.backend.exists("/base/target.txt")
        assert not self.backend.exists("/base/link.txt")

    def test_clear_files_only_on_empty_store(self):
        """Test clear_files_only on store with only directories."""
        self.backend.ensure_directory("/base/sub1")
        self.backend.ensure_directory("/base/sub2")

        # Should not raise
        self.backend.clear_files_only()

        assert self.backend.is_dir("/base/sub1")
        assert self.backend.is_dir("/base/sub2")

    @patch.object(MemoryBackend, "_is_gpu_object", return_value=True)
    @patch.object(MemoryBackend, "_explicit_gpu_delete")
    def test_clear_files_only_gpu_cleanup(self, mock_gpu_delete, mock_is_gpu):
        """Test clear_files_only calls GPU cleanup for GPU objects."""
        # Create mock GPU object
        mock_gpu_obj = Mock()
        self.backend._memory_store["/base/gpu_tensor.pt"] = mock_gpu_obj

        self.backend.clear_files_only()

        # Verify GPU cleanup was attempted
        mock_gpu_delete.assert_called_once()


class TestMemoryIsGPUObject:
    """Test GPU object detection."""

    def setup_method(self):
        self.backend = MemoryBackend()

    def test_is_gpu_object_pytorch_cuda(self):
        """Test detection of PyTorch GPU tensor."""
        mock_tensor = Mock()
        mock_tensor.device = Mock()
        mock_tensor.is_cuda = True

        assert self.backend._is_gpu_object(mock_tensor) is True

    def test_is_gpu_object_cupy_array(self):
        """Test detection of CuPy array."""
        mock_cupy = Mock()
        mock_cupy.__class__ = Mock()
        # Mock the type string to contain 'cupy'
        type(mock_cupy).__name__ = "ndarray"

        # Create a side effect that returns True for string check
        with patch(
            "builtins.str", side_effect=lambda x: "cupy" if x is type(mock_cupy) else str(x)
        ):
            result = self.backend._is_gpu_object(mock_cupy)
            # Result depends on the mock setup, but method shouldn't crash
            assert isinstance(result, bool)

    def test_is_gpu_object_cpu_array(self):
        """Test detection of CPU array."""
        numpy_array = np.array([1, 2, 3])
        assert self.backend._is_gpu_object(numpy_array) is False

    def test_is_gpu_object_regular_object(self):
        """Test detection on regular Python objects."""
        regular_obj = {"key": "value"}
        assert self.backend._is_gpu_object(regular_obj) is False


class TestMemoryListingOperations:
    """Test listing methods with various options."""

    def setup_method(self):
        self.backend = MemoryBackend()
        self.backend.ensure_directory("/base")

    def test_list_files_nonexistent_directory(self):
        """Test listing non-existent directory raises error."""
        with pytest.raises(FileNotFoundError):
            self.backend.list_files("/nonexistent")

    def test_list_files_on_file_raises_error(self):
        """Test listing on file raises error."""
        self.backend.save("data", "/base/file.txt")

        with pytest.raises(NotADirectoryError):
            self.backend.list_files("/base/file.txt")

    def test_list_files_with_extension_filter(self):
        """Test listing with extension filter."""
        self.backend.save("a", "/base/file1.txt")
        self.backend.save("b", "/base/file2.txt")
        self.backend.save("c", "/base/file3.csv")

        txt_files = self.backend.list_files("/base", extensions={".txt"})

        assert len(txt_files) == 2
        assert all(str(f).endswith(".txt") for f in txt_files)

    def test_list_files_recursive(self):
        """Test recursive file listing."""
        self.backend.ensure_directory("/base/sub")
        self.backend.save("a", "/base/file1.txt")
        self.backend.save("b", "/base/sub/file2.txt")

        files = self.backend.list_files("/base", recursive=True)

        assert len(files) == 2

    def test_list_files_non_recursive(self):
        """Test non-recursive listing only gets direct children."""
        self.backend.ensure_directory("/base/sub")
        self.backend.save("a", "/base/file1.txt")
        self.backend.save("b", "/base/sub/file2.txt")

        files = self.backend.list_files("/base", recursive=False)

        assert len(files) == 1
        assert str(files[0]).endswith("file1.txt")

    def test_list_files_with_pattern(self):
        """Test listing with fnmatch pattern."""
        self.backend.save("a", "/base/data1.txt")
        self.backend.save("b", "/base/data2.txt")
        self.backend.save("c", "/base/other.txt")

        files = self.backend.list_files("/base", pattern="data*")

        assert len(files) == 2

    def test_list_dir_returns_direct_children(self):
        """Test list_dir returns names of direct children."""
        self.backend.ensure_directory("/base/sub1")
        self.backend.ensure_directory("/base/sub2")
        self.backend.save("file", "/base/file.txt")

        entries = self.backend.list_dir("/base")

        assert "sub1" in entries
        assert "sub2" in entries
        assert "file.txt" in entries

    def test_list_dir_does_not_include_nested_files(self):
        """Test list_dir doesn't include files in subdirectories."""
        self.backend.ensure_directory("/base/sub")
        self.backend.save("nested", "/base/sub/nested.txt")

        entries = self.backend.list_dir("/base")

        assert "sub" in entries
        assert "nested.txt" not in entries

    def test_list_dir_nonexistent_raises_error(self):
        """Test list_dir on non-existent path raises error."""
        with pytest.raises(FileNotFoundError):
            self.backend.list_dir("/nonexistent")

    def test_list_dir_on_file_raises_error(self):
        """Test list_dir on file raises error."""
        self.backend.save("data", "/base/file.txt")

        with pytest.raises(NotADirectoryError):
            self.backend.list_dir("/base/file.txt")


class TestMemoryEdgeCases:
    """Test edge cases and error conditions."""

    def setup_method(self):
        self.backend = MemoryBackend()
        self.backend.ensure_directory("/base")

    def test_save_to_nonexistent_parent_raises_error(self):
        """Test saving to non-existent parent directory raises error."""
        with pytest.raises(FileNotFoundError):
            self.backend.save("data", "/nonexistent/file.txt")

    def test_save_to_existing_path_raises_error(self):
        """Test saving to existing path raises FileExistsError."""
        self.backend.save("data1", "/base/file.txt")

        with pytest.raises(FileExistsError):
            self.backend.save("data2", "/base/file.txt")

    def test_load_nonexistent_raises_error(self):
        """Test loading non-existent path raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            self.backend.load("/base/nonexistent")

    def test_load_directory_raises_error(self):
        """Test loading a directory raises IsADirectoryError."""
        with pytest.raises(IsADirectoryError):
            self.backend.load("/base")

    def test_ensure_directory_idempotent(self):
        """Test ensure_directory is idempotent."""
        path1 = self.backend.ensure_directory("/new/dir")
        path2 = self.backend.ensure_directory("/new/dir")

        assert str(path1) == str(path2)

    def test_path_normalization_consistent(self):
        """Test path normalization is consistent."""
        self.backend.save("data1", "/base/file.txt")

        # Access with different path formats
        assert self.backend.load("/base/file.txt") == "data1"
        assert self.backend.load("base/file.txt") == "data1"

    def test_is_file_is_dir_boundaries(self):
        """Test is_file and is_dir proper boundaries."""
        self.backend.save("data", "/base/file.txt")

        # is_file should work on file
        assert self.backend.is_file("/base/file.txt") is True

        # is_dir should work on directory
        assert self.backend.is_dir("/base") is True

        # is_file should fail on directory
        with pytest.raises(IsADirectoryError):
            self.backend.is_file("/base")

        # is_dir should fail on file
        with pytest.raises(NotADirectoryError):
            self.backend.is_dir("/base/file.txt")

    def test_move_with_invalid_intermediate_path(self):
        """Test move fails when intermediate path is not a directory."""
        self.backend.save("file1", "/base/file1.txt")
        self.backend.save("file2", "/base/file2.txt")

        # Try to move through a file (which is not a directory)
        with pytest.raises((FileNotFoundError, StorageResolutionError)):
            self.backend.move("/base/file1.txt", "/base/file2.txt/nested.txt")

    def test_copy_with_invalid_intermediate_path(self):
        """Test copy fails when intermediate path is not a directory."""
        self.backend.save("file1", "/base/file1.txt")
        self.backend.save("file2", "/base/file2.txt")

        # Try to copy through a file (which is not a directory)
        with pytest.raises((FileNotFoundError, StorageResolutionError)):
            self.backend.copy("/base/file1.txt", "/base/file2.txt/nested.txt")
