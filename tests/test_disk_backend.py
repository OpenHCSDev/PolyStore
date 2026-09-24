"""Targeted tests for DiskBackend functionality.

Keep tests small and focused — these hit CSV/JSON/TEXT handlers, listing,
ensure_directory idempotence, and symlink creation.
"""

import hashlib
from pathlib import Path

import numpy as np
import pytest
import tifffile

from polystore import FileManager
from polystore import disk as disk_module
from polystore.config import TiffCompression, TiffConfig, tiff_write_batches
from polystore.disk import DiskBackend
from polystore.exceptions import StorageResolutionError


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


def test_verified_source_resource_reads_exact_bounded_bytes(tmp_path):
    path = tmp_path / "receipt.json"
    data = b'{"value": 1}\r\n'
    path.write_bytes(data)
    manager = FileManager({"disk": DiskBackend()})
    options = dict(base_path=tmp_path, expected_sha256=hashlib.sha256(data).hexdigest())
    assert manager.read_verified_source_bytes(path, "disk", max_bytes=len(data), **options) == data
    with pytest.raises(ValueError, match="byte bound"):
        manager.read_verified_source_bytes(path, "disk", max_bytes=len(data) - 1, **options)
    with pytest.raises(ValueError, match="SHA256 does not match"):
        manager.read_verified_source_bytes(
            path, "disk", base_path=tmp_path, expected_sha256="0" * 64
        )
    for bound in (True, 0, -1, 1.5):
        with pytest.raises(ValueError, match="positive integer"):
            manager.read_verified_source_bytes(path, "disk", max_bytes=bound, **options)


def test_verified_source_resource_opaque_source_refuses(tmp_path):
    class OpaqueDisk(DiskBackend):
        _backend_type = None

        def physical_source_path(self, backend_address, *, base_path):
            return None

    with pytest.raises(StorageResolutionError, match="Opaque source"):
        OpaqueDisk().read_verified_source_bytes(
            "opaque", base_path=tmp_path, expected_sha256="0" * 64
        )


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


def test_tiff_deflate_is_lossless_and_opt_in(tmp_path: Path) -> None:
    pixels = np.zeros((2, 256, 256), dtype=np.int32)
    pixels[0, 20:40, 30:50] = 70001
    raw_path = tmp_path / "raw.tif"
    compressed_path = tmp_path / "compressed.tif"
    backend = DiskBackend()

    backend.save(pixels, raw_path)
    backend.save(
        pixels,
        compressed_path,
        tiff_config=TiffConfig(
            compression=TiffCompression.DEFLATE,
            compression_level=3,
        ),
    )

    with (
        tifffile.TiffFile(raw_path) as raw,
        tifffile.TiffFile(compressed_path) as compressed,
    ):
        assert raw.pages[0].compression.value == 1
        assert compressed.pages[0].compression.value != 1
    assert compressed_path.stat().st_size < raw_path.stat().st_size
    np.testing.assert_array_equal(tifffile.imread(compressed_path), pixels)


def test_tiff_write_batches_preserve_order_and_exclude_other_formats() -> None:
    config = TiffConfig(compression=TiffCompression.DEFLATE)
    assert tiff_write_batches(("a.csv", "b.tif", "c.tiff", "d.npy"), config) == (
        ((0,), None),
        ((1, 2), config),
        ((3,), None),
    )
    assert tiff_write_batches(("a.csv", "b.tif"), TiffConfig()) == (
        ((0, 1), None),
    )
    with pytest.raises(ValueError, match="compression_level"):
        TiffConfig(compression_level=10)


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
