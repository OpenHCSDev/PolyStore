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

import tempfile
import shutil
from pathlib import Path
import numpy as np
import pytest

from polystore.zarr import ZarrStorageBackend
from polystore.config import ZarrConfig, ZarrChunkStrategy, CompressorConfig


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
        config = ZarrConfig(
            compression_level=5,
            chunk_strategy=ZarrChunkStrategy.FILE
        )
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

    @pytest.mark.skip(reason="Batch operations are HCS-specific - need well/plate context")
    def test_batch_save_and_load(self, zarr_backend, temp_zarr_dir):
        """Test batch save and load of multiple arrays."""
        # Note: Batch operations in zarr backend expect HCS structure (wells/plates)
        # For simple array batching, use FileManager with zarr backend instead
        pass

    @pytest.mark.skip(reason="Batch operations are HCS-specific")
    def test_batch_operations_length_mismatch(self, zarr_backend, temp_zarr_dir):
        """Test that batch operations raise error on length mismatch."""
        pass


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
            compressor=CompressorConfig(name='none')
        )
        backend = ZarrStorageBackend(zarr_config=config)

        assert backend.config.compressor.name == 'none'
