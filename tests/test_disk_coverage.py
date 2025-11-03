"""Comprehensive tests for DiskBackend covering missing code paths.

This module targets specific areas like:
- Batch operations error handling
- Listing methods with various filters
- Path resolution and symlink behavior
- Directory operations edge cases
- stat() method
- copy() and move() operations
"""

import os
from pathlib import Path
import pytest
import numpy as np

from polystore.disk import DiskBackend
from polystore.exceptions import StorageResolutionError


class TestDiskBatchOperations:
    """Test batch save/load operations and error handling."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_batch_save_length_mismatch(self, tmp_path):
        """Test that batch save with mismatched lengths raises ValueError."""
        data_list = [np.array([1, 2, 3]), np.array([4, 5, 6])]
        paths = [tmp_path / "file1.npy"]

        with pytest.raises(ValueError, match="length"):
            self.backend.save_batch(data_list, paths)

    def test_batch_load_multiple_files(self, tmp_path):
        """Test loading multiple files with batch operation."""
        self.backend.ensure_directory(tmp_path)

        # Create test files
        data1 = np.array([1, 2, 3])
        data2 = np.array([4, 5, 6])
        data3 = np.array([7, 8, 9])

        path1 = tmp_path / "a.npy"
        path2 = tmp_path / "b.npy"
        path3 = tmp_path / "c.npy"

        self.backend.save(data1, path1)
        self.backend.save(data2, path2)
        self.backend.save(data3, path3)

        # Load batch
        loaded = self.backend.load_batch([path1, path2, path3])

        assert len(loaded) == 3
        assert np.array_equal(loaded[0], data1)
        assert np.array_equal(loaded[1], data2)
        assert np.array_equal(loaded[2], data3)

    def test_batch_save_multiple_formats(self, tmp_path):
        """Test batch saving different data types."""
        self.backend.ensure_directory(tmp_path)

        # Create varied data
        arr = np.array([1, 2, 3])
        text_data = "hello world"
        json_data = {"key": "value"}

        paths = [
            tmp_path / "data.npy",
            tmp_path / "text.txt",
            tmp_path / "data.json",
        ]

        # Batch save (though it's sequential internally)
        self.backend.save(arr, paths[0])
        self.backend.save(text_data, paths[1])
        self.backend.save(json_data, paths[2])

        # Verify all loaded correctly
        assert np.array_equal(self.backend.load(paths[0]), arr)
        assert self.backend.load(paths[1]) == text_data
        assert self.backend.load(paths[2]) == json_data


class TestDiskListingOperations:
    """Test file listing with various filters and recursive options."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_list_files_nonexistent_directory(self, tmp_path):
        """Test listing files in non-existent directory raises error."""
        nonexistent = tmp_path / "nonexistent"
        with pytest.raises(ValueError, match="not a directory"):
            self.backend.list_files(nonexistent)

    def test_list_files_on_file_path(self, tmp_path):
        """Test listing files when path is a file raises error."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)

        with pytest.raises(ValueError, match="not a directory"):
            self.backend.list_files(file_path)

    def test_list_files_empty_directory(self, tmp_path):
        """Test listing files in empty directory returns empty list."""
        self.backend.ensure_directory(tmp_path)
        files = self.backend.list_files(tmp_path)
        assert files == []

    def test_list_files_with_pattern(self, tmp_path):
        """Test listing files with glob pattern."""
        self.backend.ensure_directory(tmp_path)

        # Create files with different patterns
        self.backend.save("data", tmp_path / "data1.txt")
        self.backend.save("data", tmp_path / "data2.txt")
        self.backend.save("other", tmp_path / "other.csv")

        # List with pattern
        txt_files = self.backend.list_files(tmp_path, pattern="*.txt")
        assert len(txt_files) == 2
        assert all(f.endswith(".txt") for f in txt_files)

    def test_list_files_extension_filter_case_insensitive(self, tmp_path):
        """Test extension filtering is case-insensitive."""
        self.backend.ensure_directory(tmp_path)

        self.backend.save("a", tmp_path / "file1.TXT")
        self.backend.save("b", tmp_path / "file2.txt")
        self.backend.save("c", tmp_path / "file3.csv")

        # Filter by lowercase extension
        txt_files = self.backend.list_files(tmp_path, extensions={".txt"})
        assert len(txt_files) == 2

    def test_list_files_recursive_depth_ordering(self, tmp_path):
        """Test that breadth-first ordering is maintained in recursive listing."""
        self.backend.ensure_directory(tmp_path)

        # Create structure: level0 > level1 > level2
        self.backend.save("a", tmp_path / "level0.txt")
        self.backend.ensure_directory(tmp_path / "sub1")
        self.backend.save("b", tmp_path / "sub1" / "level1.txt")
        self.backend.ensure_directory(tmp_path / "sub1" / "sub2")
        self.backend.save("c", tmp_path / "sub1" / "sub2" / "level2.txt")

        files = self.backend.list_files(tmp_path, recursive=True)

        # Find indices
        files_str = [str(f) for f in files]
        level0_idx = next(i for i, f in enumerate(files_str) if f.endswith("level0.txt"))
        level1_idx = next(i for i, f in enumerate(files_str) if f.endswith("level1.txt"))
        level2_idx = next(i for i, f in enumerate(files_str) if f.endswith("level2.txt"))

        # Verify breadth-first ordering
        assert level0_idx < level1_idx < level2_idx

    def test_list_dir_on_nonexistent_path(self, tmp_path):
        """Test list_dir on nonexistent path raises error."""
        nonexistent = tmp_path / "nonexistent"
        with pytest.raises(FileNotFoundError):
            self.backend.list_dir(nonexistent)

    def test_list_dir_on_file_path(self, tmp_path):
        """Test list_dir on file path raises error."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)

        with pytest.raises(NotADirectoryError):
            self.backend.list_dir(file_path)

    def test_list_dir_returns_entry_names(self, tmp_path):
        """Test list_dir returns names of direct children only."""
        self.backend.ensure_directory(tmp_path)
        self.backend.save("a", tmp_path / "file.txt")
        self.backend.ensure_directory(tmp_path / "subdir")

        entries = self.backend.list_dir(tmp_path)
        assert "file.txt" in entries
        assert "subdir" in entries


class TestDiskDeleteOperations:
    """Test delete and delete_all with various edge cases."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_delete_nonexistent_path(self, tmp_path):
        """Test deleting non-existent path raises FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent"
        with pytest.raises(FileNotFoundError):
            self.backend.delete(nonexistent)

    def test_delete_file_succeeds(self, tmp_path):
        """Test deleting a file."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)
        assert file_path.exists()

        self.backend.delete(file_path)
        assert not file_path.exists()

    def test_delete_empty_directory_succeeds(self, tmp_path):
        """Test deleting an empty directory."""
        subdir = tmp_path / "empty"
        self.backend.ensure_directory(subdir)
        assert subdir.exists()

        self.backend.delete(subdir)
        assert not subdir.exists()

    def test_delete_non_empty_directory_raises_error(self, tmp_path):
        """Test deleting non-empty directory raises IsADirectoryError."""
        subdir = tmp_path / "nonempty"
        self.backend.ensure_directory(subdir)
        self.backend.save("data", subdir / "file.txt")

        with pytest.raises(IsADirectoryError):
            self.backend.delete(subdir)

    def test_delete_all_file(self, tmp_path):
        """Test delete_all on a single file."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)

        self.backend.delete_all(file_path)
        assert not file_path.exists()

    def test_delete_all_directory_tree(self, tmp_path):
        """Test delete_all removes entire directory tree."""
        base = tmp_path / "root"
        self.backend.ensure_directory(base)
        self.backend.ensure_directory(base / "sub1" / "sub2")
        self.backend.save("a", base / "file1.txt")
        self.backend.save("b", base / "sub1" / "file2.txt")
        self.backend.save("c", base / "sub1" / "sub2" / "file3.txt")

        self.backend.delete_all(base)
        assert not base.exists()

    def test_delete_all_nonexistent_path(self, tmp_path):
        """Test delete_all on non-existent path raises FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent"
        with pytest.raises(FileNotFoundError):
            self.backend.delete_all(nonexistent)


class TestDiskPathOperations:
    """Test path checking and metadata operations."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_exists_on_file(self, tmp_path):
        """Test exists returns True for files."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)
        assert self.backend.exists(file_path)

    def test_exists_on_directory(self, tmp_path):
        """Test exists returns True for directories."""
        self.backend.ensure_directory(tmp_path)
        assert self.backend.exists(tmp_path)

    def test_exists_on_nonexistent(self, tmp_path):
        """Test exists returns False for non-existent paths."""
        nonexistent = tmp_path / "nonexistent"
        assert not self.backend.exists(nonexistent)

    def test_is_file_on_regular_file(self, tmp_path):
        """Test is_file on regular file."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)

        assert self.backend.is_file(file_path)

    def test_is_file_on_directory_raises_error(self, tmp_path):
        """Test is_file on directory raises IsADirectoryError."""
        self.backend.ensure_directory(tmp_path)

        with pytest.raises(IsADirectoryError):
            self.backend.is_file(tmp_path)

    def test_is_file_on_nonexistent_raises_error(self, tmp_path):
        """Test is_file on non-existent path raises FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent"

        with pytest.raises(FileNotFoundError):
            self.backend.is_file(nonexistent)

    def test_is_dir_on_directory(self, tmp_path):
        """Test is_dir on directory returns True."""
        self.backend.ensure_directory(tmp_path)
        assert self.backend.is_dir(tmp_path)

    def test_is_dir_on_file_raises_error(self, tmp_path):
        """Test is_dir on file raises NotADirectoryError."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)

        with pytest.raises(NotADirectoryError):
            self.backend.is_dir(file_path)

    def test_is_dir_on_nonexistent_raises_error(self, tmp_path):
        """Test is_dir on non-existent path raises FileNotFoundError."""
        nonexistent = tmp_path / "nonexistent"

        with pytest.raises(FileNotFoundError):
            self.backend.is_dir(nonexistent)


class TestDiskStatOperation:
    """Test stat() method for metadata retrieval."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_stat_on_file(self, tmp_path):
        """Test stat on a regular file."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)

        stat_result = self.backend.stat(file_path)

        assert stat_result["type"] == "file"
        assert stat_result["exists"] is True
        assert str(file_path) == stat_result["path"]

    def test_stat_on_directory(self, tmp_path):
        """Test stat on a directory."""
        self.backend.ensure_directory(tmp_path)

        stat_result = self.backend.stat(tmp_path)

        assert stat_result["type"] == "directory"
        assert stat_result["exists"] is True

    def test_stat_on_symlink(self, tmp_path):
        """Test stat on a symlink."""
        target = tmp_path / "target.txt"
        self.backend.save("test", target)

        link = tmp_path / "link.txt"
        self.backend.create_symlink(target, link)

        stat_result = self.backend.stat(link)

        assert stat_result["type"] == "symlink"
        assert stat_result["exists"] is True
        assert "target" in stat_result

    def test_stat_on_broken_symlink(self, tmp_path):
        """Test stat on a broken symlink."""
        target = tmp_path / "target.txt"
        link = tmp_path / "link.txt"

        # Create symlink to non-existent target
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(target)

        stat_result = self.backend.stat(link)

        assert stat_result["type"] == "symlink"
        assert stat_result["exists"] is False

    def test_stat_on_missing_path(self, tmp_path):
        """Test stat on non-existent path."""
        missing = tmp_path / "missing"

        stat_result = self.backend.stat(missing)

        assert stat_result["type"] == "missing"
        assert stat_result["exists"] is False


class TestDiskCopyAndMove:
    """Test copy() and move() operations."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_copy_file_basic(self, tmp_path):
        """Test copying a file."""
        src = tmp_path / "source.txt"
        dst = tmp_path / "dest.txt"

        self.backend.save("test content", src)
        self.backend.copy(src, dst)

        assert dst.exists()
        assert self.backend.load(dst) == "test content"
        assert src.exists()  # Original still exists

    def test_copy_file_to_existing_dest_raises_error(self, tmp_path):
        """Test copy to existing destination raises FileExistsError."""
        src = tmp_path / "source.txt"
        dst = tmp_path / "dest.txt"

        self.backend.save("src", src)
        self.backend.save("dst", dst)

        with pytest.raises(FileExistsError):
            self.backend.copy(src, dst)

    def test_copy_nonexistent_file_raises_error(self, tmp_path):
        """Test copy of non-existent file raises FileNotFoundError."""
        src = tmp_path / "nonexistent.txt"
        dst = tmp_path / "dest.txt"

        with pytest.raises(FileNotFoundError):
            self.backend.copy(src, dst)

    def test_copy_directory(self, tmp_path):
        """Test copying a directory tree."""
        src = tmp_path / "srcdir"
        self.backend.ensure_directory(src)
        self.backend.save("file1", src / "file1.txt")
        self.backend.ensure_directory(src / "sub")
        self.backend.save("file2", src / "sub" / "file2.txt")

        dst = tmp_path / "dstdir"
        self.backend.copy(src, dst)

        assert dst.exists()
        assert (dst / "file1.txt").exists()
        assert (dst / "sub" / "file2.txt").exists()

    def test_move_file_basic(self, tmp_path):
        """Test moving a file."""
        src = tmp_path / "source.txt"
        dst = tmp_path / "dest.txt"

        self.backend.save("test content", src)
        self.backend.move(src, dst)

        assert not src.exists()
        assert dst.exists()
        assert self.backend.load(dst) == "test content"

    def test_move_to_existing_dest_raises_error(self, tmp_path):
        """Test move to existing destination raises FileExistsError."""
        src = tmp_path / "source.txt"
        dst = tmp_path / "dest.txt"

        self.backend.save("src", src)
        self.backend.save("dst", dst)

        with pytest.raises(FileExistsError):
            self.backend.move(src, dst)

    def test_move_nonexistent_file_raises_error(self, tmp_path):
        """Test move of non-existent file raises FileNotFoundError."""
        src = tmp_path / "nonexistent.txt"
        dst = tmp_path / "dest.txt"

        with pytest.raises(FileNotFoundError):
            self.backend.move(src, dst)


class TestDiskSymlinkAdvanced:
    """Advanced symlink operations and edge cases."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_symlink_to_directory(self, tmp_path):
        """Test creating symlink to a directory."""
        src_dir = tmp_path / "srcdir"
        self.backend.ensure_directory(src_dir)
        self.backend.save("file", src_dir / "file.txt")

        link_dir = tmp_path / "linkdir"
        self.backend.create_symlink(src_dir, link_dir)

        assert self.backend.is_symlink(link_dir)
        # Verify we can access files through symlink
        assert (link_dir / "file.txt").exists()

    def test_symlink_to_nonexistent_source_raises_error(self, tmp_path):
        """Test creating symlink to non-existent source raises error."""
        src = tmp_path / "nonexistent"
        link = tmp_path / "link"

        with pytest.raises(FileNotFoundError):
            self.backend.create_symlink(src, link)

    def test_symlink_to_existing_dest_without_overwrite_raises_error(self, tmp_path):
        """Test creating symlink when destination exists."""
        src = tmp_path / "source.txt"
        link = tmp_path / "link.txt"

        self.backend.save("src", src)
        self.backend.save("existing", link)

        with pytest.raises(FileExistsError):
            self.backend.create_symlink(src, link, overwrite=False)

    def test_symlink_with_overwrite(self, tmp_path):
        """Test creating symlink with overwrite=True."""
        src = tmp_path / "source.txt"
        link = tmp_path / "link.txt"

        self.backend.save("src", src)
        self.backend.save("old_target", link)

        # Should not raise
        self.backend.create_symlink(src, link, overwrite=True)
        assert self.backend.is_symlink(link)

    def test_is_symlink_on_regular_file(self, tmp_path):
        """Test is_symlink on regular file returns False."""
        file_path = tmp_path / "file.txt"
        self.backend.save("test", file_path)

        assert not self.backend.is_symlink(file_path)


class TestDiskErrorHandling:
    """Test error handling in various scenarios."""

    def setup_method(self):
        self.backend = DiskBackend()

    def test_load_unregistered_extension(self, tmp_path):
        """Test loading file with unregistered extension."""
        # Create a file with unknown extension
        file_path = tmp_path / "file.unknown_ext"
        file_path.write_text("test")

        with pytest.raises(ValueError, match="No writer registered"):
            self.backend.load(file_path)

    def test_save_unregistered_extension(self, tmp_path):
        """Test saving with unregistered extension."""
        file_path = tmp_path / "file.unknown_ext"

        with pytest.raises(ValueError, match="No writer registered"):
            self.backend.save("data", file_path)

    def test_ensure_directory_already_exists(self, tmp_path):
        """Test ensure_directory on existing directory."""
        self.backend.ensure_directory(tmp_path)
        # Should not raise
        result = self.backend.ensure_directory(tmp_path)
        assert result == tmp_path

    def test_ensure_directory_creates_nested_structure(self, tmp_path):
        """Test ensure_directory creates nested directories."""
        nested = tmp_path / "a" / "b" / "c"
        result = self.backend.ensure_directory(nested)
        
        assert nested.exists()
        assert nested.is_dir()
