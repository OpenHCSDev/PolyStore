"""Focused PNG format registration and round-trip coverage."""

import ast
import inspect
import textwrap
from pathlib import Path

import numpy as np
import pytest

import polystore.disk as disk_module
from polystore.disk import DiskBackend, DiskStorageBackend
from polystore.formats import DEFAULT_IMAGE_EXTENSIONS, FileFormat, get_format_from_extension


@pytest.mark.parametrize(
    "source",
    [
        np.arange(63, dtype=np.uint8).reshape(7, 9),
        np.arange(189, dtype=np.uint8).reshape(7, 9, 3),
        np.arange(252, dtype=np.uint8).reshape(7, 9, 4),
    ],
    ids=["grayscale", "rgb", "rgba"],
)
def test_png_round_trip_preserves_pixels_shape_and_dtype(
    tmp_path: Path,
    source: np.ndarray,
):
    backend = DiskBackend()
    path = tmp_path / "pixels.png"

    backend.save(source, path)
    loaded = backend.load(path)

    assert loaded.shape == source.shape
    assert loaded.dtype == source.dtype
    np.testing.assert_array_equal(loaded, source)


def test_png_is_an_exact_registered_file_format():
    backend = DiskBackend()
    png_writer = backend.format_registry.get_writer(".png")
    raster_writer = backend.format_registry.get_writer(".bmp")

    assert get_format_from_extension("png") is FileFormat.PNG
    assert get_format_from_extension(".PNG") is FileFormat.PNG
    assert FileFormat.PNG.extensions == (".png",)
    assert ".png" not in FileFormat.RASTER_IMAGE.extensions
    assert ".png" in DEFAULT_IMAGE_EXTENSIONS
    assert png_writer.__func__ is DiskStorageBackend._png_writer
    assert raster_writer.__func__ is DiskStorageBackend._image_writer


def test_png_writer_uses_lossless_compression_level_one(monkeypatch, tmp_path: Path):
    calls = []

    def record_imwrite(path, data, **kwargs):
        calls.append((path, np.asarray(data), kwargs))

    monkeypatch.setattr(disk_module.imageio, "imwrite", record_imwrite)
    source = np.arange(12, dtype=np.uint8).reshape(3, 4)
    path = tmp_path / "pixels.png"

    DiskBackend().save(source, path)

    assert len(calls) == 1
    assert calls[0][0] == path
    np.testing.assert_array_equal(calls[0][1], source)
    assert calls[0][2] == {"compress_level": 1}


def test_generic_raster_writer_has_no_format_dispatch():
    tree = ast.parse(
        textwrap.dedent(inspect.getsource(DiskStorageBackend._image_writer))
    )

    assert not any(isinstance(node, ast.If) for node in ast.walk(tree))
