"""Basic tests for polystore."""

import pytest
import numpy as np


def test_import():
    """Test that polystore can be imported."""
    import polystore
    assert polystore.__version__ == "0.1.0"


def test_memory_backend():
    """Test memory backend basic functionality."""
    from polystore import MemoryBackend
    
    backend = MemoryBackend()
    data = np.array([1, 2, 3])
    
    # Save and load
    backend.save(data, "test.npy")
    loaded = backend.load("test.npy")
    
    assert np.array_equal(data, loaded)


def test_file_manager():
    """Test FileManager basic functionality."""
    from polystore import FileManager, BackendRegistry
    
    registry = BackendRegistry()
    fm = FileManager(registry)
    
    data = np.array([[1, 2], [3, 4]])
    
    # Save to memory backend
    fm.save(data, "test.npy", backend="memory")
    
    # Load from memory backend
    loaded = fm.load("test.npy", backend="memory")
    
    assert np.array_equal(data, loaded)

