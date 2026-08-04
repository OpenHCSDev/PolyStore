"""Basic tests for polystore."""

from importlib.metadata import version

import numpy as np


def test_import():
    """Test that polystore can be imported."""
    import polystore
    assert polystore.__version__ == version("polystore")


def test_memory_backend():
    """Test memory backend basic functionality."""
    from polystore import MemoryBackend
    
    backend = MemoryBackend()
    # Create root directory for memory backend
    backend.ensure_directory("/")
    
    data = np.array([1, 2, 3])
    
    # Save and load
    backend.save(data, "/test.npy")
    loaded = backend.load("/test.npy")
    
    assert np.array_equal(data, loaded)


def test_file_manager():
    """Test FileManager basic functionality."""
    from polystore import FileManager, MemoryBackend
    
    registry = {"memory": MemoryBackend()}
    fm = FileManager(registry)
    
    # Ensure directory exists in memory backend
    fm.ensure_directory("/", backend="memory")
    
    data = np.array([[1, 2], [3, 4]])
    
    # Save to memory backend
    fm.save(data, "/test.npy", backend="memory")
    
    # Load from memory backend
    loaded = fm.load("/test.npy", backend="memory")
    
    assert np.array_equal(data, loaded)
