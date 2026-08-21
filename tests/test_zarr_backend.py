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

from polystore import zarr as zarr_module
from polystore.config import (
    ZarrChunkStrategy,
    ZarrCompressor,
    ZarrCompressorFactory,
    ZarrConfig,
)
from polystore.zarr import ZarrStorageBackend
from polystore.zarr_batch import (
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

    @pytest.mark.skip(reason="Overwrite behavior needs investigation - may require delete first")
    def test_overwrite_existing_array(self, zarr_backend, temp_zarr_dir):
        """Test overwriting an existing zarr array."""
        path = Path(temp_zarr_dir) / "overwrite.zarr"

        # Save initial data
        data1 = np.ones((10, 10), dtype=np.float32)
        zarr_backend.save(data1, path)

        # Overwrite with new data
        data2 = np.zeros((20, 20), dtype=np.float32)
        zarr_backend.save(data2, path)

        # Load and verify
        loaded = zarr_backend.load(path)
        assert loaded.shape == (20, 20)
        np.testing.assert_array_equal(loaded, data2)


class TestZarrBatchOperations:
    """Test batch save/load operations."""

    def test_batch_save_and_load(self, zarr_backend, temp_zarr_dir):
        """Declared axes preserve timepoints and non-flat item ordering."""
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
        assert array.shape == (2, 2, 1, 3, 4)
        assert [
            axis["name"] for axis in image_group.attrs["multiscales"][0]["axes"]
        ] == [
            "t",
            "c",
            "z",
            "y",
            "x",
        ]
        requested_order = (2, 0, 3, 1)
        loaded = zarr_backend.load_batch(
            [output_paths[index] for index in requested_order]
        )
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
        assert root["A/01/0/0"].attrs["polystore_image_coordinate"] == {
            "field": "3"
        }
        assert root["A/01/1/0"].attrs["polystore_image_coordinate"] == {
            "field": "7"
        }
        loaded = zarr_backend.load_batch(output_paths)
        for loaded_item, expected in zip(loaded, data, strict=True):
            np.testing.assert_array_equal(loaded_item, expected)


class TestZarrPassthrough:
    """Test passthrough of non-array files to disk backend.

    Note: Passthrough is designed to work via FileManager routing, not direct backend calls.
    The decorator checks file extensions and delegates to disk backend when appropriate.
    Direct backend testing of passthrough is complex due to zarr's group structure.
    """

    @pytest.mark.skip(reason="Passthrough works via FileManager, not direct backend calls")
    def test_json_passthrough(self, zarr_backend, temp_zarr_dir):
        """JSON passthrough should be tested at FileManager level."""
        pass

    @pytest.mark.skip(reason="Passthrough works via FileManager, not direct backend calls")
    def test_csv_passthrough(self, zarr_backend, temp_zarr_dir):
        """CSV passthrough should be tested at FileManager level."""
        pass

    @pytest.mark.skip(reason="Passthrough works via FileManager, not direct backend calls")
    def test_txt_passthrough(self, zarr_backend, temp_zarr_dir):
        """TXT passthrough should be tested at FileManager level."""
        pass


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

    @pytest.mark.skip(reason="Directory operations are HCS/plate-specific in zarr backend")
    def test_ensure_directory(self, zarr_backend, temp_zarr_dir):
        """Test ensure_directory - works differently in zarr (creates groups)."""
        pass

    @pytest.mark.skip(reason="Directory operations are HCS/plate-specific in zarr backend")
    def test_exists_for_directory(self, zarr_backend, temp_zarr_dir):
        """Test exists() for directories."""
        pass

    @pytest.mark.skip(reason="is_file/is_dir semantics differ in zarr group structure")
    def test_is_file_for_zarr(self, zarr_backend, temp_zarr_dir):
        """Test is_file() for zarr arrays."""
        pass

    @pytest.mark.skip(reason="is_file/is_dir semantics differ in zarr group structure")
    def test_is_dir(self, zarr_backend, temp_zarr_dir):
        """Test is_dir() for directories."""
        pass

    @pytest.mark.skip(reason="list_files is HCS-specific - needs plate context")
    def test_list_files(self, zarr_backend, temp_zarr_dir):
        """Test list_files() - HCS-specific in zarr backend."""
        pass

    @pytest.mark.skip(reason="list_files is HCS-specific - needs plate context")
    def test_list_files_with_extension_filter(self, zarr_backend, temp_zarr_dir):
        """Test list_files with extension filter."""
        pass

    @pytest.mark.skip(reason="list_dir is HCS-specific - needs plate context")
    def test_list_dir(self, zarr_backend, temp_zarr_dir):
        """Test list_dir() - HCS-specific in zarr backend."""
        pass


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
