"""Additional DiskBackend tests targeting high-coverage branches.

These tests are designed to be small but exercise many code paths in
`src/polystore/disk.py` such as the format registry, numpy handling,
symlink overwrite logic, deletion branches, and recursive listing order.
"""
import os
from pathlib import Path

import numpy as np
import pytest

from polystore.disk import DiskBackend, FileFormatRegistry


def test_numpy_save_load(tmp_path: Path):
    disk = DiskBackend()
    disk.ensure_directory(tmp_path)

    arr = np.arange(12).reshape(3, 4)
    out = tmp_path / "a.npy"
    disk.save(arr, out)
    loaded = disk.load(out)
    assert np.array_equal(arr, loaded)


def test_file_format_registry_api(tmp_path: Path):
    registry = FileFormatRegistry()

    def writer(path, data):
        path.write_text(str(data))

    def reader(path):
        return path.read_text()

    registry.register('.foo', writer, reader)
    assert registry.is_registered('.foo')
    assert registry.get_reader('.foo') is reader
    assert registry.get_writer('.foo') is writer


def test_symlink_overwrite_behavior(tmp_path: Path):
    disk = DiskBackend()
    src_dir = tmp_path / "s"
    disk.ensure_directory(src_dir)
    src_file = src_dir / "file.txt"
    disk.save("hello", src_file)

    link = tmp_path / "link" / "file.txt"
    # first creation should succeed
    disk.create_symlink(src_file, link)
    assert link.exists()

    # creating again without overwrite should raise
    with pytest.raises(FileExistsError):
        disk.create_symlink(src_file, link, overwrite=False)

    # with overwrite=True should succeed
    disk.create_symlink(src_file, link, overwrite=True)
    assert link.exists()


def test_is_file_is_dir_and_delete(tmp_path: Path):
    disk = DiskBackend()
    base = tmp_path / "root"
    disk.ensure_directory(base)
    disk.save("x", base / "f.txt")
    disk.ensure_directory(base / "sub")
    disk.save("y", base / "sub" / "g.txt")

    assert disk.is_dir(base)
    assert disk.is_file(base / "f.txt")

    # delete file
    disk.delete(base / "f.txt")
    assert not (base / "f.txt").exists()

    # deleting non-empty directory should raise
    with pytest.raises(IsADirectoryError):
        disk.delete(base)

    # delete_all should remove the tree
    disk.delete_all(base)
    assert not base.exists()


def test_list_files_breadth_first_order(tmp_path: Path):
    disk = DiskBackend()
    base = tmp_path / "root2"
    disk.ensure_directory(base)
    # root files
    disk.save("r", base / "rootfile.txt")
    # nested deeper
    disk.ensure_directory(base / "a")
    disk.ensure_directory(base / "a" / "b")
    disk.save("d", base / "a" / "b" / "deep.txt")

    files = disk.list_files(base, recursive=True)
    # ensure rootfile appears before the deeper file (breadth-first)
    str_files = [str(f) for f in files]
    assert any(s.endswith("rootfile.txt") for s in str_files)
    assert any(s.endswith("deep.txt") for s in str_files)
    assert str_files.index(next(s for s in str_files if s.endswith("rootfile.txt"))) < \
        str_files.index(next(s for s in str_files if s.endswith("deep.txt")))
