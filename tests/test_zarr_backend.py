"""
Tests for ZarrStorageBackend - array storage operations.

Tests cover:
- Basic save/load operations for numpy arrays
- ZarrConfig integration (compression, chunking strategies)
- Error handling
- Basic file existence checks

Note: This tests the core array storage functionality.
HCS-specific features (plates/wells) can be tested separately or moved to a plugin.
Directory operations are limited - zarr stores data in hierarchical groups, not flat files.
"""

import shutil
import tempfile
from dataclasses import fields
from inspect import getdoc
from pathlib import Path
from typing import get_type_hints

import numpy as np
import pytest
import zarr

from polystore import FileManager
from polystore import zarr as zarr_module
from polystore.config import (
    ZarrChunkStrategy,
    ZarrCompressor,
    ZarrCompressorFactory,
    ZarrConfig,
)
from polystore.disk import DiskBackend
from polystore.exceptions import StorageResolutionError
from polystore.zarr import ZarrStorageBackend
from polystore.zarr_batch import (
    ATTR_FILENAME_MAP,
    ZarrBatchAxis,
    ZarrBatchAxisRole,
    ZarrBatchLayout,
)


@pytest.fixture
def zarr_backend():
    """Create a ZarrStorageBackend instance with default config."""
    return ZarrStorageBackend()


@pytest.fixture
def temp_zarr_dir():
    """Create a temporary directory for zarr stores."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


def save_hcs_image_axis_batch(
    backend,
    store_path,
    *,
    axis_name,
    values,
    row,
    col,
    suffixes=None,
):
    suffixes = suffixes or (".tif",) * len(values)
    output_paths = [
        store_path / f"{row}{col}_{axis_name}_{value}{suffix}"
        for value, suffix in zip(values, suffixes, strict=True)
    ]
    layout = ZarrBatchLayout(
        axes=(
            ZarrBatchAxis(
                axis_name,
                "field",
                values,
                ZarrBatchAxisRole.HCS_IMAGE,
            ),
        ),
        item_coordinates=tuple((index,) for index in range(len(values))),
    )
    backend.save_batch(
        [np.full((2, 3), index, dtype=np.uint16) for index in range(len(values))],
        output_paths,
        chunk_name=f"{row}{col}",
        batch_layout=layout,
        row=row,
        col=col,
    )
    return output_paths


class TestZarrBackendBasics:
    """Test basic zarr backend functionality."""

    def test_backend_type(self, zarr_backend):
        """Test backend type is correctly set."""
        assert zarr_backend._backend_type == "zarr"

    def test_init_with_config(self):
        """Test initialization with custom ZarrConfig."""
        config = ZarrConfig(compression_level=5, chunk_strategy=ZarrChunkStrategy.FILE)
        backend = ZarrStorageBackend(zarr_config=config)
        assert backend.config.compression_level == 5
        assert backend.config.chunk_strategy == ZarrChunkStrategy.FILE

    def test_init_without_config(self, zarr_backend):
        """Test initialization without config uses defaults."""
        assert zarr_backend.config is not None
        assert isinstance(zarr_backend.config, ZarrConfig)
        assert zarr_backend.config.chunk_strategy == ZarrChunkStrategy.WELL


class TestZarrArrayOperations:
    """Test save/load operations for zarr arrays."""

    def test_save_and_load_numpy_array(self, zarr_backend, temp_zarr_dir):
        """Test basic save and load of numpy array."""
        data = np.random.rand(100, 100).astype(np.float32)
        path = Path(temp_zarr_dir) / "test_array.zarr"

        # Save
        zarr_backend.save(data, path)
        assert path.exists()

        # Load
        loaded = zarr_backend.load(path)
        assert isinstance(loaded, np.ndarray)
        np.testing.assert_array_equal(loaded, data)

    def test_save_normalizes_external_array_payload(
        self,
        zarr_backend,
        temp_zarr_dir,
        monkeypatch,
    ):
        class ExternalArray:
            pass

        source = ExternalArray()
        expected = np.arange(16, dtype=np.float32).reshape(4, 4)
        monkeypatch.setattr(
            zarr_module,
            "storage_numpy_array",
            lambda value: expected if value is source else value,
        )
        path = Path(temp_zarr_dir) / "external.zarr"

        zarr_backend.save(source, path)

        np.testing.assert_array_equal(zarr_backend.load(path), expected)

    def test_save_and_load_different_dtypes(self, zarr_backend, temp_zarr_dir):
        """Test save/load with different numpy dtypes."""
        dtypes = [np.uint8, np.uint16, np.int32, np.float32, np.float64]

        for dtype in dtypes:
            data = np.arange(100, dtype=dtype).reshape(10, 10)
            path = Path(temp_zarr_dir) / f"test_{dtype.__name__}.zarr"

            zarr_backend.save(data, path)
            loaded = zarr_backend.load(path)

            assert loaded.dtype == dtype
            np.testing.assert_array_equal(loaded, data)

    def test_save_multidimensional_arrays(self, zarr_backend, temp_zarr_dir):
        """Test save/load of multidimensional arrays."""
        # 3D array
        data_3d = np.random.rand(10, 20, 30).astype(np.float32)
        path_3d = Path(temp_zarr_dir) / "test_3d.zarr"
        zarr_backend.save(data_3d, path_3d)
        loaded_3d = zarr_backend.load(path_3d)
        np.testing.assert_array_equal(loaded_3d, data_3d)

        # 4D array
        data_4d = np.random.rand(5, 10, 15, 20).astype(np.float16)
        path_4d = Path(temp_zarr_dir) / "test_4d.zarr"
        zarr_backend.save(data_4d, path_4d)
        loaded_4d = zarr_backend.load(path_4d)
        np.testing.assert_array_equal(loaded_4d, data_4d)

    def test_save_rejects_overwrite_without_modifying_existing_array(
        self, zarr_backend, temp_zarr_dir
    ):
        """A rejected write preserves the original shape and pixels."""
        path = Path(temp_zarr_dir) / "overwrite.zarr"

        # Save initial data
        data1 = np.ones((10, 10), dtype=np.float32)
        zarr_backend.save(data1, path)

        # Overwrite with new data
        data2 = np.zeros((20, 20), dtype=np.float32)
        with pytest.raises(FileExistsError):
            zarr_backend.save(data2, path)

        # Load and verify
        loaded = zarr_backend.load(path)
        assert loaded.shape == (10, 10)
        np.testing.assert_array_equal(loaded, data1)


class TestZarrBatchOperations:
    """Test batch save/load operations."""

    @pytest.mark.parametrize(
        ("strategy", "expected_chunks"),
        [
            (ZarrChunkStrategy.WELL, (2, 2, 1, 3, 4)),
            (ZarrChunkStrategy.FILE, (1, 1, 1, 3, 4)),
        ],
    )
    def test_batch_save_and_load(self, temp_zarr_dir, strategy, expected_chunks):
        """Declared axes preserve timepoints and non-flat item ordering."""
        zarr_backend = ZarrStorageBackend(ZarrConfig(chunk_strategy=strategy))
        output_paths = [
            Path(temp_zarr_dir) / "images" / name
            for name in (
                "A01_s001_w2_z001_t002.tif",
                "A01_s001_w1_z001_t001.tif",
                "A01_s001_w2_z001_t001.tif",
                "A01_s001_w1_z001_t002.tif",
            )
        ]
        coordinates = ((1, 0, 1, 0), (0, 0, 0, 0), (0, 0, 1, 0), (1, 0, 0, 0))
        layout = ZarrBatchLayout(
            axes=(
                ZarrBatchAxis("t", "time", ("1", "2")),
                ZarrBatchAxis(
                    "field",
                    "field",
                    ("1",),
                    ZarrBatchAxisRole.HCS_IMAGE,
                ),
                ZarrBatchAxis("c", "channel", ("1", "2")),
                ZarrBatchAxis("z", "space", ("1",)),
            ),
            item_coordinates=coordinates,
        )
        data = [np.full((3, 4), value, dtype=np.uint16) for value in range(4)]

        zarr_backend.save_batch(
            data,
            output_paths,
            chunk_name="A01",
            batch_layout=layout,
            row="A",
            col="01",
        )

        root = zarr.open_group(str(output_paths[0].parent), mode="r")
        image_group = root["A/01/0"]
        array = image_group["0"]
        assert root.metadata.zarr_format == 2
        assert array.metadata.zarr_format == 2
        assert root.attrs["plate"]["version"] == "0.4"
        assert root["A/01"].attrs["well"]["version"] == "0.4"
        assert image_group.attrs["multiscales"][0]["version"] == "0.4"
        assert "ome" not in root.attrs
        assert "ome" not in root["A/01"].attrs
        assert array.shape == (2, 2, 1, 3, 4)
        assert array.chunks == expected_chunks
        assert [axis["name"] for axis in image_group.attrs["multiscales"][0]["axes"]] == [
            "t",
            "c",
            "z",
            "y",
            "x",
        ]
        requested_order = (2, 0, 3, 1)
        loaded = zarr_backend.load_batch([output_paths[index] for index in requested_order])
        for loaded_item, expected_index in zip(loaded, requested_order, strict=True):
            np.testing.assert_array_equal(loaded_item, data[expected_index])

    def test_batch_operations_length_mismatch(self, zarr_backend, temp_zarr_dir):
        """Test that batch operations raise error on length mismatch."""
        layout = ZarrBatchLayout(
            axes=(ZarrBatchAxis("c", "channel", ("1",)),),
            item_coordinates=((0,),),
        )
        with pytest.raises(ValueError, match="equal lengths"):
            zarr_backend.save_batch(
                [np.ones((2, 2))],
                [
                    Path(temp_zarr_dir) / "images" / "first.tif",
                    Path(temp_zarr_dir) / "images" / "second.tif",
                ],
                chunk_name="A01",
                batch_layout=layout,
                row="A",
                col="01",
            )

    @pytest.mark.parametrize("bad_shape", [(2, 2, 2), (3, 4)])
    def test_invalid_batch_preserves_existing_well(self, zarr_backend, tmp_path, bad_shape):
        paths = save_hcs_image_axis_batch(
            zarr_backend,
            tmp_path / "images",
            axis_name="field",
            values=("1", "2"),
            row="A",
            col="01",
        )
        before = zarr_backend.load_batch(paths)
        layout = ZarrBatchLayout(
            axes=(ZarrBatchAxis("c", "channel", ("1", "2")),),
            item_coordinates=((0,), (1,)),
        )
        with pytest.raises(ValueError, match="image-plane shape|two-dimensional"):
            zarr_backend.save_batch(
                [np.ones((2, 3)), np.ones(bad_shape)],
                paths,
                chunk_name="A01",
                batch_layout=layout,
                row="A",
                col="01",
            )
        for expected, actual in zip(before, zarr_backend.load_batch(paths), strict=True):
            np.testing.assert_array_equal(actual, expected)

    def test_image_axis_creates_one_hcs_image_per_value(
        self,
        zarr_backend,
        temp_zarr_dir,
    ):
        """Image-role coordinates become HCS image groups, not array axes."""
        output_paths = [
            Path(temp_zarr_dir) / "images" / "A01_s003_w1_z001_t001.tif",
            Path(temp_zarr_dir) / "images" / "A01_s007_w1_z001_t001.tif",
        ]
        layout = ZarrBatchLayout(
            axes=(
                ZarrBatchAxis("t", "time", ("1",)),
                ZarrBatchAxis(
                    "field",
                    "field",
                    ("3", "7"),
                    ZarrBatchAxisRole.HCS_IMAGE,
                ),
                ZarrBatchAxis("c", "channel", ("1",)),
                ZarrBatchAxis("z", "space", ("1",)),
            ),
            item_coordinates=((0, 0, 0, 0), (0, 1, 0, 0)),
        )
        data = [np.full((2, 3), value, dtype=np.uint16) for value in (3, 7)]

        zarr_backend.save_batch(
            data,
            output_paths,
            chunk_name="A01",
            batch_layout=layout,
            row="A",
            col="01",
        )

        root = zarr.open_group(str(output_paths[0].parent), mode="r")
        assert set(root["A/01"].group_keys()) == {"0", "1"}
        assert root["A/01/0/0"].attrs["polystore_image_coordinate"] == {"field": "3"}
        assert root["A/01/1/0"].attrs["polystore_image_coordinate"] == {"field": "7"}
        loaded = zarr_backend.load_batch(output_paths)
        for loaded_item, expected in zip(loaded, data, strict=True):
            np.testing.assert_array_equal(loaded_item, expected)

    @pytest.mark.parametrize("recursive", [False, True])
    def test_list_files_uses_every_declared_hcs_image_group(
        self,
        zarr_backend,
        temp_zarr_dir,
        recursive,
    ):
        store_path = Path(temp_zarr_dir) / "images"
        field_paths = save_hcs_image_axis_batch(
            zarr_backend,
            store_path,
            axis_name="field",
            values=("3", "7"),
            row="A",
            col="01",
        )
        scene_paths = save_hcs_image_axis_batch(
            zarr_backend,
            store_path,
            axis_name="scene",
            values=("left", "mid", "right"),
            row="B",
            col="02",
            suffixes=(".tif", ".tif", ".png"),
        )

        root = zarr.open_group(str(store_path), mode="a")
        auxiliary = root["B/02"].create_group("auxiliary")
        auxiliary_array = auxiliary.create_array(
            "0",
            shape=(2, 3),
            dtype=np.uint16,
        )
        auxiliary_array.attrs["polystore_output_paths"] = ["undeclared.tif"]
        auxiliary_array.attrs[ATTR_FILENAME_MAP] = {"undeclared.tif": []}

        assert set(zarr_backend.list_files(store_path, recursive=recursive)) == set(
            field_paths + scene_paths
        )
        assert zarr_backend.list_files(store_path, pattern="*_7.tif") == [field_paths[1]]
        assert zarr_backend.list_files(store_path, extensions={".png"}) == [scene_paths[2]]
        with pytest.raises(KeyError, match="undeclared.tif"):
            zarr_backend.load_batch([store_path / "undeclared.tif"])

    def test_list_files_supports_nested_and_legacy_well_declarations(
        self,
        zarr_backend,
        temp_zarr_dir,
    ):
        store_path = Path(temp_zarr_dir) / "images"
        output_paths = save_hcs_image_axis_batch(
            zarr_backend,
            store_path,
            axis_name="field",
            values=("3", "7"),
            row="A",
            col="01",
        )

        root = zarr.open_group(str(store_path), mode="a")
        well_group = root["A/01"]
        plate_metadata = root.attrs["plate"]
        del root.attrs["plate"]
        root.attrs["ome"] = {"version": "0.5", "plate": plate_metadata}
        well_group.attrs["ome"] = {"version": "0.5", "well": well_group.attrs["well"]}
        del well_group.attrs["well"]
        assert set(zarr_backend.list_files(store_path)) == set(output_paths)

        well_group.attrs["ome"] = {"version": "0.5"}
        assert zarr_backend.list_files(store_path) == [output_paths[0]]

    def test_historical_conflicting_nested_metadata_preserves_top_level_authority(
        self, zarr_backend, temp_zarr_dir
    ):
        """Old PolyStore stores emitted valid 0.4 plus stale nested 0.5 metadata."""
        store_path = Path(temp_zarr_dir) / "images"
        paths = save_hcs_image_axis_batch(
            zarr_backend, store_path, axis_name="field", values=("3", "7"), row="A", col="01"
        )
        root = zarr.open_group(str(store_path), mode="a")
        root.attrs["ome"] = {"version": "0.4"}
        root["A/01"].attrs["ome"] = {
            "version": "0.5",
            "well": {"version": "0.5", "images": [{"path": "missing", "acquisition": 0}]},
        }

        assert set(zarr_backend.list_files(store_path)) == set(paths)
        for index, loaded in enumerate(zarr_backend.load_batch(paths)):
            np.testing.assert_array_equal(loaded, np.full((2, 3), index, dtype=np.uint16))

    def test_list_files_rejects_missing_declared_image_group(
        self,
        zarr_backend,
        temp_zarr_dir,
    ):
        store_path = Path(temp_zarr_dir) / "images"
        save_hcs_image_axis_batch(
            zarr_backend,
            store_path,
            axis_name="field",
            values=("3",),
            row="A",
            col="01",
        )
        root = zarr.open_group(str(store_path), mode="a")
        root["A/01"].attrs["well"] = {"images": [{"path": "missing"}]}

        with pytest.raises(StorageResolutionError, match="missing image groups"):
            zarr_backend.list_files(store_path)


class TestZarrPassthrough:
    """Non-array outputs remain ordinary files beside Zarr data."""

    @pytest.mark.parametrize(
        ("suffix", "payload"),
        [
            (".json", {"count": 3, "sample": "A01"}),
            (".csv", [{"count": "3", "sample": "A01"}]),
            (".txt", "Three objects in sample A01.\n"),
        ],
    )
    def test_file_manager_writes_text_outputs_as_regular_files(
        self, zarr_backend, tmp_path, suffix, payload
    ):
        disk = DiskBackend()
        manager = FileManager({"disk": disk, "zarr": zarr_backend})
        path = tmp_path / "outputs" / f"results{suffix}"
        manager.save(payload, path, backend="zarr")
        assert path.is_file()
        assert disk.load(path) == payload
        assert zarr_backend.exists(path)


class TestZarrDirectoryOperations:
    """Test directory-related operations.

    Note: Zarr backend stores data in hierarchical groups within .zarr directories,
    not as flat files. Directory operations have different semantics than disk backend.
    Many operations are HCS-specific (require plate/well structure).
    """

    def test_exists_for_zarr_file(self, zarr_backend, temp_zarr_dir):
        """Test exists() for zarr files."""
        path = Path(temp_zarr_dir) / "exists_test.zarr"

        # Before creation
        assert not zarr_backend.exists(path)

        # After creation
        data = np.zeros((10, 10))
        zarr_backend.save(data, path)
        assert zarr_backend.exists(path)

    def test_ensure_directory(self, zarr_backend, temp_zarr_dir):
        """Store creation belongs to writes; ensuring a path is a documented no-op."""
        path = Path(temp_zarr_dir) / "images"
        assert zarr_backend.ensure_directory(path) == path

    def test_exists_for_directory(self, zarr_backend, temp_zarr_dir):
        path = Path(temp_zarr_dir) / "images"
        assert not zarr_backend.exists(path)
        assert not path.exists()
        zarr_backend.save(np.zeros((2, 3)), path / "image.tif")
        assert zarr_backend.exists(path)

    def test_is_file_for_zarr(self, zarr_backend, temp_zarr_dir):
        path = Path(temp_zarr_dir) / "image.tif"
        zarr_backend.save(np.zeros((2, 3)), path)
        assert zarr_backend.is_file(path)

    def test_is_dir(self, zarr_backend, temp_zarr_dir):
        path = Path(temp_zarr_dir) / "images"
        zarr_backend.save(np.zeros((2, 3)), path / "image.tif")
        assert zarr_backend.is_dir(path)

    def test_list_files(self, zarr_backend, temp_zarr_dir):
        path = Path(temp_zarr_dir) / "images"
        paths = save_hcs_image_axis_batch(
            zarr_backend, path, axis_name="field", values=("3", "7"), row="A", col="01"
        )
        assert set(zarr_backend.list_files(path)) == set(paths)

    def test_list_files_with_extension_filter(self, zarr_backend, temp_zarr_dir):
        path = Path(temp_zarr_dir) / "images"
        paths = save_hcs_image_axis_batch(
            zarr_backend,
            path,
            axis_name="field",
            values=("3", "7"),
            row="A",
            col="01",
            suffixes=(".tif", ".png"),
        )
        assert zarr_backend.list_files(path, extensions={".png"}) == [paths[1]]

    def test_list_dir(self, zarr_backend, temp_zarr_dir):
        path = Path(temp_zarr_dir) / "images"
        zarr_backend.save(np.zeros((2, 3)), path / "image.tif")
        root = zarr.open_group(str(path), mode="a")
        root.create_group("nested")
        assert set(zarr_backend.list_dir(path)) == {"image.tif", "nested"}

    @pytest.mark.parametrize("operation", [ZarrStorageBackend.copy, ZarrStorageBackend.move])
    @pytest.mark.parametrize("same_store", [False, True])
    def test_transfer_preserves_pixels_metadata_and_chunks(
        self, zarr_backend, tmp_path, operation, same_store
    ):
        source = tmp_path / "source" / "image.tif"
        destination = (source.parent if same_store else tmp_path / "destination") / "copy.tif"
        pixels = np.arange(60, dtype=np.uint16).reshape(3, 4, 5)
        zarr_backend.save(pixels, source, chunks=(1, 2, 5))
        source_array = zarr.open_array(str(source), mode="a")
        source_array.attrs["experiment"] = {"name": "transfer", "axes": ["z", "y", "x"]}
        original_attrs = source_array.attrs.asdict()

        operation(zarr_backend, source, destination)

        np.testing.assert_array_equal(zarr_backend.load(destination), pixels)
        copied = zarr.open_array(str(destination), mode="r")
        assert copied.metadata.zarr_format == 2
        assert copied.chunks == (1, 2, 5)
        assert copied.attrs.asdict() == original_attrs
        assert zarr_backend.exists(source) is (operation is ZarrStorageBackend.copy)

    @pytest.mark.parametrize("operation", [ZarrStorageBackend.copy, ZarrStorageBackend.move])
    def test_transfer_never_overwrites_destination(self, zarr_backend, tmp_path, operation):
        source, destination = tmp_path / "source.tif", tmp_path / "destination.tif"
        zarr_backend.save(np.ones((2, 3)), source)
        zarr_backend.save(np.zeros((4, 5)), destination)
        with pytest.raises(FileExistsError):
            operation(zarr_backend, source, destination)
        np.testing.assert_array_equal(zarr_backend.load(source), np.ones((2, 3)))
        np.testing.assert_array_equal(zarr_backend.load(destination), np.zeros((4, 5)))

    def test_delete_array_and_empty_group_preserves_siblings(self, zarr_backend, tmp_path):
        first, second = tmp_path / "first.tif", tmp_path / "second.tif"
        zarr_backend.save(np.ones((2, 3)), first)
        zarr_backend.save(np.zeros((2, 3)), second)
        zarr_backend.delete(first)
        assert not zarr_backend.exists(first)
        assert zarr_backend.exists(second)

        root = zarr.open_group(str(tmp_path), mode="a")
        root.create_group("empty")
        zarr_backend.delete(tmp_path / "empty")
        assert "empty" not in root
        with pytest.raises(IsADirectoryError):
            zarr_backend.delete(tmp_path)
        assert zarr_backend.exists(second)

    def test_logical_symlink_load_and_overwrite_contract(self, zarr_backend, tmp_path):
        source, link = tmp_path / "source.tif", tmp_path / "link.tif"
        pixels = np.arange(6, dtype=np.uint16).reshape(2, 3)
        zarr_backend.save(pixels, source)
        zarr_backend.create_symlink(source, link)
        assert zarr_backend.is_symlink(link)
        assert zarr_backend.is_file(link)
        np.testing.assert_array_equal(zarr_backend.load(link), pixels)
        with pytest.raises(FileExistsError):
            zarr_backend.create_symlink(source, link)
        zarr_backend.create_symlink(source, link, overwrite=True)
        np.testing.assert_array_equal(zarr_backend.load(link), pixels)

    def test_logical_symlink_cycle_raises_resolution_error(self, zarr_backend, tmp_path):
        source, link = tmp_path / "source.tif", tmp_path / "link.tif"
        zarr_backend.save(np.ones((2, 3)), source)
        zarr_backend.create_symlink(source, link)
        zarr_backend.create_symlink(link, source, overwrite=True)
        with pytest.raises(StorageResolutionError, match="cycle"):
            zarr_backend.load(link)

    def test_missing_read_does_not_create_store(self, zarr_backend, tmp_path):
        path = tmp_path / "missing" / "image.tif"
        assert not zarr_backend.exists(path)
        with pytest.raises(FileNotFoundError):
            zarr_backend.load(path)
        assert not path.parent.exists()


class TestZarrErrorHandling:
    """Test error handling in zarr backend."""

    def test_load_nonexistent_file(self, zarr_backend, temp_zarr_dir):
        """Test loading nonexistent file raises error."""
        path = Path(temp_zarr_dir) / "nonexistent.zarr"

        with pytest.raises(FileNotFoundError):
            zarr_backend.load(path)

    def test_save_to_nonexistent_parent_creates_parent(self, zarr_backend, temp_zarr_dir):
        """Test saving to nonexistent parent directory creates it."""
        nested_path = Path(temp_zarr_dir) / "new" / "nested" / "test.zarr"
        data = np.zeros((10, 10))

        # Should create parent directories automatically
        zarr_backend.save(data, nested_path)
        assert nested_path.exists()


class TestZarrConfigIntegration:
    """Test ZarrConfig integration with backend."""

    def test_backend_config_annotation_resolves_to_nominal_owner(self):
        hints = get_type_hints(ZarrStorageBackend._configure)

        assert hints["zarr_config"] is ZarrConfig

    def test_public_fields_have_declaration_help(self):
        docstring = getdoc(ZarrConfig)

        assert docstring is not None
        assert all(f"{field.name}:" in docstring for field in fields(ZarrConfig))

    def test_compressor_registry_covers_exactly_the_owning_enum(self):
        assert set(ZarrCompressorFactory.__registry__) == set(ZarrCompressor)

    def test_compression_level_config(self, temp_zarr_dir):
        """Test that compression level config is applied."""
        config = ZarrConfig(compression_level=9)
        backend = ZarrStorageBackend(zarr_config=config)

        assert backend.config.compression_level == 9

    def test_chunk_strategy_config(self, temp_zarr_dir):
        """Test different chunk strategies."""
        for strategy in [ZarrChunkStrategy.WELL, ZarrChunkStrategy.FILE]:
            config = ZarrConfig(chunk_strategy=strategy)
            backend = ZarrStorageBackend(zarr_config=config)

            assert backend.config.chunk_strategy == strategy

    def test_compressor_config(self, temp_zarr_dir):
        """Test compressor config is accessible."""
        config = ZarrConfig(
            compressor=ZarrCompressor.NONE,
        )
        backend = ZarrStorageBackend(zarr_config=config)

        assert backend.config.compressor is ZarrCompressor.NONE
        assert backend.compressor is None

    @pytest.mark.parametrize("compressor", tuple(ZarrCompressor))
    def test_every_compressor_has_one_registered_factory(self, compressor):
        """The owning enum resolves directly through the owning registry."""
        factory_type = ZarrCompressorFactory.__registry__[compressor]
        factory = factory_type()

        assert factory.compressor is compressor
        if compressor is ZarrCompressor.NONE:
            assert factory.create(3) is None
        else:
            assert factory.create(3) is not None

    @pytest.mark.parametrize(
        ("strategy", "expected"),
        (
            (ZarrChunkStrategy.WELL, (2, 3, 4, 10, 20)),
            (ZarrChunkStrategy.FILE, (1, 1, 1, 10, 20)),
        ),
    )
    def test_chunk_strategy_controls_backend_chunks(self, strategy, expected):
        backend = ZarrStorageBackend(ZarrConfig(chunk_strategy=strategy))

        assert backend._calculate_chunks((2, 3, 4, 10, 20)) == expected

    @pytest.mark.parametrize("compressor", tuple(ZarrCompressor))
    @pytest.mark.parametrize("dtype", [np.uint8, np.uint16, np.int32, np.float16, np.float64])
    def test_every_codec_round_trips_with_explicit_chunks(self, tmp_path, compressor, dtype):
        backend = ZarrStorageBackend(ZarrConfig(compressor=compressor))
        pixels = np.arange(120, dtype=dtype).reshape(2, 3, 4, 5)
        path = tmp_path / "image.tif"
        backend.save(pixels, path, chunks=(1, 1, 2, 5))
        array = zarr.open_array(str(path), mode="r")

        assert array.metadata.zarr_format == 2
        assert array.chunks == (1, 1, 2, 5)
        assert array.dtype == dtype
        assert array.metadata.compressor == backend.compressor
        np.testing.assert_array_equal(backend.load(path), pixels)
