"""Comprehensive tests for MemoryBackend."""

import numpy as np
import pytest

from polystore import MemoryBackend


class TestMemoryBackend:
    """Test suite for MemoryBackend."""

    def setup_method(self):
        """Set up test backend for each test."""
        self.backend = MemoryBackend()
        # Create a directory structure
        self.backend.ensure_directory("/test")
        self.backend.ensure_directory("/test/sub")

    def test_init(self):
        """Test backend initialization."""
        backend = MemoryBackend()
        assert backend._memory_store is not None
        assert isinstance(backend._memory_store, dict)

    def test_init_with_shared_dict(self):
        """Test backend initialization with shared dictionary."""
        shared = {"existing": "data"}
        backend = MemoryBackend(shared_dict=shared)
        assert backend._memory_store is shared
        assert "existing" in backend._memory_store

    def test_save_and_load(self):
        """Test basic save and load operations."""
        data = np.array([1, 2, 3, 4, 5])
        self.backend.save(data, "/test/data.npy")
        loaded = self.backend.load("/test/data.npy")
        np.testing.assert_array_equal(data, loaded)

    def test_save_to_nonexistent_parent(self):
        """Test that saving to non-existent parent directory fails."""
        data = np.array([1, 2, 3])
        with pytest.raises(FileNotFoundError):
            self.backend.save(data, "/nonexistent/data.npy")

    def test_save_duplicate(self):
        """Test that saving to existing path fails."""
        data = np.array([1, 2, 3])
        self.backend.save(data, "/test/data.npy")
        with pytest.raises(FileExistsError):
            self.backend.save(data, "/test/data.npy")

    def test_load_nonexistent(self):
        """Test loading non-existent file raises error."""
        with pytest.raises(FileNotFoundError):
            self.backend.load("/test/missing.npy")

    def test_load_directory(self):
        """Test loading a directory raises error."""
        with pytest.raises(IsADirectoryError):
            self.backend.load("/test")

    def test_batch_save_and_load(self):
        """Test batch save and load operations."""
        data_list = [np.array([1, 2, 3]), np.array([4, 5, 6]), np.array([7, 8, 9])]
        paths = ["/test/data1.npy", "/test/data2.npy", "/test/data3.npy"]

        self.backend.save_batch(data_list, paths)
        loaded_list = self.backend.load_batch(paths)

        assert len(loaded_list) == len(data_list)
        for original, loaded in zip(data_list, loaded_list, strict=False):
            np.testing.assert_array_equal(original, loaded)

    def test_batch_save_length_mismatch(self):
        """Test batch save with mismatched lengths raises error."""
        data_list = [np.array([1, 2, 3]), np.array([4, 5, 6])]
        paths = ["/test/data1.npy"]

        with pytest.raises(ValueError):
            self.backend.save_batch(data_list, paths)

    def test_ensure_directory(self):
        """Test directory creation."""
        path = self.backend.ensure_directory("/new/nested/dir")
        # Path should be normalized to forward slashes and preserve leading slash
        assert str(path) == "/new/nested/dir"

        # Verify directory exists
        assert self.backend.exists("/new/nested/dir")
        assert self.backend.is_dir("/new/nested/dir")

    def test_list_files(self):
        """Test listing files in directory."""
        # Create some files
        for i in range(3):
            self.backend.save(np.array([i]), f"/test/file{i}.npy")

        files = self.backend.list_files("/test")
        assert len(files) == 3
        assert all(str(f).endswith(".npy") for f in files)

    def test_list_files_with_extension_filter(self):
        """Test listing files with extension filter."""
        # Create files with different extensions
        self.backend.save(np.array([1]), "/test/data1.npy")
        self.backend.save("text", "/test/data2.txt")
        self.backend.save(np.array([2]), "/test/data3.npy")

        npy_files = self.backend.list_files("/test", extensions={".npy"})
        assert len(npy_files) == 2

    def test_list_files_recursive(self):
        """Test recursive file listing."""
        # Create files in multiple levels
        self.backend.save(np.array([1]), "/test/file1.npy")
        self.backend.save(np.array([2]), "/test/sub/file2.npy")

        files = self.backend.list_files("/test", recursive=True)
        assert len(files) >= 2

    def test_list_dir(self):
        """Test listing directory entries."""
        # Create some entries
        self.backend.save(np.array([1]), "/test/file.npy")
        self.backend.ensure_directory("/test/subdir")

        entries = self.backend.list_dir("/test")
        assert "file.npy" in entries or "sub" in entries

    def test_exists(self):
        """Test path existence checking."""
        assert self.backend.exists("/test")
        assert not self.backend.exists("/nonexistent")

        self.backend.save(np.array([1]), "/test/file.npy")
        assert self.backend.exists("/test/file.npy")

    def test_is_file(self):
        """Test file checking."""
        self.backend.save(np.array([1]), "/test/file.npy")
        assert self.backend.is_file("/test/file.npy")
        with pytest.raises(IsADirectoryError):
            self.backend.is_file("/test")

    def test_is_dir(self):
        """Test directory checking."""
        assert self.backend.is_dir("/test")
        self.backend.save(np.array([1]), "/test/file.npy")
        with pytest.raises(NotADirectoryError):
            self.backend.is_dir("/test/file.npy")

    def test_delete(self):
        """Test file deletion."""
        self.backend.save(np.array([1]), "/test/file.npy")
        assert self.backend.exists("/test/file.npy")

        self.backend.delete("/test/file.npy")
        assert not self.backend.exists("/test/file.npy")

    def test_delete_nonexistent(self):
        """Test deleting non-existent path raises error."""
        with pytest.raises(FileNotFoundError):
            self.backend.delete("/test/missing.npy")

    def test_delete_nonempty_directory(self):
        """Test deleting non-empty directory raises error."""
        self.backend.save(np.array([1]), "/test/file.npy")
        with pytest.raises(IsADirectoryError):
            self.backend.delete("/test")

    def test_delete_all(self):
        """Test recursive deletion."""
        # Create a directory tree
        self.backend.save(np.array([1]), "/test/file1.npy")
        self.backend.save(np.array([2]), "/test/sub/file2.npy")

        self.backend.delete_all("/test")
        assert not self.backend.exists("/test")

    def test_path_normalization(self):
        """Test that paths are normalized correctly."""
        # Test with different path formats
        data = np.array([1, 2, 3])
        self.backend.save(data, "/test/data.npy")

        # Should be able to load with different path format
        loaded = self.backend.load("/test/data.npy")
        np.testing.assert_array_equal(data, loaded)

    def test_different_data_types(self):
        """Test saving and loading different data types."""
        # NumPy array
        arr = np.array([1, 2, 3])
        self.backend.save(arr, "/test/array.npy")
        loaded_arr = self.backend.load("/test/array.npy")
        np.testing.assert_array_equal(arr, loaded_arr)

        # String
        text = "Hello, World!"
        self.backend.save(text, "/test/text.txt")
        loaded_text = self.backend.load("/test/text.txt")
        assert text == loaded_text

        # Dictionary
        data_dict = {"key": "value", "number": 42}
        self.backend.save(data_dict, "/test/data.json")
        loaded_dict = self.backend.load("/test/data.json")
        assert data_dict == loaded_dict

    def test_is_file_nonexistent(self):
        """Test is_file on non-existent path returns False."""
        assert not self.backend.is_file("/nonexistent/file.txt")

    def test_is_dir_nonexistent(self):
        """Test is_dir on non-existent path returns False."""
        assert not self.backend.is_dir("/nonexistent/dir")

    def test_create_symlink_parent_not_found(self):
        """Test creating symlink with non-existent parent fails."""
        self.backend.save("data", "/test/source.txt")
        with pytest.raises(FileNotFoundError):
            self.backend.create_symlink("/test/source.txt", "/nonexistent/link.txt")

    def test_move_directory_with_contents(self):
        """Test moving a directory with nested files."""
        # Create directory structure
        self.backend.ensure_directory("/test/dir")
        self.backend.ensure_directory("/test/dir/sub")
        self.backend.save("file1", "/test/dir/file1.txt")
        self.backend.save("file2", "/test/dir/sub/file2.txt")
        self.backend.save("file3", "/test/dir/sub/file3.txt")

        # Move the directory
        self.backend.move("/test/dir", "/test/newdir")

        # Verify old location is gone
        assert not self.backend.exists("/test/dir")
        assert not self.backend.exists("/test/dir/file1.txt")
        assert not self.backend.exists("/test/dir/sub/file2.txt")

        # Verify new location has all files
        assert self.backend.exists("/test/newdir")
        assert self.backend.load("/test/newdir/file1.txt") == "file1"
        assert self.backend.load("/test/newdir/sub/file2.txt") == "file2"
        assert self.backend.load("/test/newdir/sub/file3.txt") == "file3"

    def test_copy_directory_with_contents(self):
        """Test copying a directory with nested files."""
        # Create directory structure
        self.backend.ensure_directory("/test/dir")
        self.backend.ensure_directory("/test/dir/sub")
        self.backend.save("file1", "/test/dir/file1.txt")
        self.backend.save("file2", "/test/dir/sub/file2.txt")

        # Copy the directory
        self.backend.copy("/test/dir", "/test/copydir")

        # Verify original still exists
        assert self.backend.exists("/test/dir")
        assert self.backend.load("/test/dir/file1.txt") == "file1"

        # Verify copy exists with all files
        assert self.backend.exists("/test/copydir")
        assert self.backend.load("/test/copydir/file1.txt") == "file1"
        assert self.backend.load("/test/copydir/sub/file2.txt") == "file2"
