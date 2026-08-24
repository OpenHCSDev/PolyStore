"""Targeted tests for DiskBackend functionality.

Keep tests small and focused — these hit CSV/JSON/TEXT handlers, listing,
ensure_directory idempotence, and symlink creation.
"""

from pathlib import Path

import numpy as np
import tifffile

from polystore import disk as disk_module
from polystore.disk import DiskBackend


def test_text_json_csv_save_load(tmp_path: Path):
    disk = DiskBackend()

    # ensure directory
    disk.ensure_directory(tmp_path)

    # text
    t = tmp_path / "a.txt"
    disk.save("hello", t)
    assert disk.load(t) == "hello"

    # json
    j = tmp_path / "data.json"
    payload = {"x": 1, "y": "two"}
    disk.save(payload, j)
    assert disk.load(j) == payload

    # csv (list of dicts)
    c = tmp_path / "rows.csv"
    rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
    disk.save(rows, c)
    loaded = disk.load(c)
    assert isinstance(loaded, list)
    assert loaded[0]["a"] == "1"


def test_preformatted_text_is_written_as_exact_utf8_bytes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    content = "well,count\r\nA01,2\r\n"
    output_path = tmp_path / "summary.csv"

    def reject_platform_text_translation(*args, **kwargs):
        del args, kwargs
        raise AssertionError("text persistence must bypass platform newline translation")

    monkeypatch.setattr(Path, "write_text", reject_platform_text_translation)
    DiskBackend().save(content, output_path)

    assert output_path.read_bytes() == content.encode(disk_module.DISK_TEXT_ENCODING)


def test_tiff_save_normalizes_external_array_payload(tmp_path, monkeypatch) -> None:
    class ExternalArray:
        pass

    source = ExternalArray()
    expected = np.arange(16, dtype=np.uint16).reshape(4, 4)
    monkeypatch.setattr(
        disk_module,
        "storage_numpy_array",
        lambda value: expected if value is source else value,
    )
    path = tmp_path / "external.tif"

    DiskBackend().save(source, path)

    np.testing.assert_array_equal(tifffile.imread(path), expected)


def test_list_files_recursive_and_extension_filter(tmp_path: Path):
    disk = DiskBackend()
    base = tmp_path / "root"
    disk.ensure_directory(base)
    disk.save("a", base / "a.txt")
    disk.ensure_directory(base / "sub")
    disk.save("b", base / "sub" / "b.txt")

    files_nonrec = disk.list_files(base, recursive=False)
    assert any(str(f).endswith("a.txt") for f in files_nonrec)

    files_rec = disk.list_files(base, recursive=True)
    assert any(str(f).endswith("b.txt") for f in files_rec)

    # extension filter
    txt_only = disk.list_files(base, extensions={".txt"}, recursive=True)
    assert all(str(f).endswith(".txt") for f in txt_only)


def test_symlink_creation_and_detection(tmp_path: Path):
    disk = DiskBackend()
    src_dir = tmp_path / "src"
    disk.ensure_directory(src_dir)
    src_file = src_dir / "file.txt"
    disk.save("x", src_file)

    link = tmp_path / "link" / "file.txt"
    # create symlink
    disk.create_symlink(src_file, link)
    assert disk.is_symlink(link)
