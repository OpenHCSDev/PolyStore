# polystore

**Framework-agnostic multi-backend storage abstraction for ML and scientific computing**

[![PyPI version](https://badge.fury.io/py/polystore.svg)](https://badge.fury.io/py/polystore)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- **Pluggable Backends**: Disk, memory, Zarr, and streaming backends with auto-registration
- **Multi-Framework I/O**: Seamless support for NumPy, PyTorch, JAX, TensorFlow, CuPy
- **Atomic Operations**: Cross-platform atomic file writes with automatic locking
- **Batch Operations**: Efficient batch loading and saving
- **Format Detection**: Automatic format detection and routing
- **Type-Safe**: Full type hints and mypy support
- **Zero Dependencies**: Core requires only NumPy (framework support is optional)

## Quick Start

```python
from polystore import FileManager, BackendRegistry

# Create registry and file manager
registry = BackendRegistry()
fm = FileManager(registry)

# Save data to disk
import numpy as np
data = np.array([[1, 2], [3, 4]])
fm.save(data, "output.npy", backend="disk")

# Load data back
loaded = fm.load("output.npy", backend="disk")

# Use memory backend for testing
fm.save(data, "test.npy", backend="memory")
cached = fm.load("test.npy", backend="memory")
```

## Installation

```bash
# Base installation (NumPy only)
pip install polystore

# With specific frameworks
pip install polystore[zarr]
pip install polystore[torch]
pip install polystore[jax]
pip install polystore[tensorflow]
pip install polystore[cupy]

# With streaming support
pip install polystore[streaming]

# With all optional dependencies
pip install polystore[all]
```

## Supported Backends

| Backend | Description | Storage | Dependencies |
|---------|-------------|---------|--------------|
| **disk** | Local filesystem | Persistent | None |
| **memory** | In-memory cache | Volatile | None |
| **zarr** | Zarr/OME-Zarr arrays | Persistent | zarr, ome-zarr |
| **streaming** | ZeroMQ streaming | None | pyzmq |

## Supported Formats

| Format | Extensions | Frameworks |
|--------|-----------|------------|
| **NumPy** | `.npy`, `.npz` | NumPy, PyTorch, JAX, TensorFlow, CuPy |
| **TIFF** | `.tif`, `.tiff` | NumPy, PyTorch, JAX, TensorFlow, CuPy |
| **Zarr** | `.zarr` | NumPy, PyTorch, JAX, TensorFlow, CuPy |
| **PyTorch** | `.pt`, `.pth` | PyTorch |
| **CSV** | `.csv` | NumPy, pandas |
| **JSON** | `.json` | Python dicts |

## Architecture

```
polystore/
├── base.py              # Abstract interfaces (DataSink, DataSource, StorageBackend)
├── backend_registry.py  # Auto-registration system
├── disk.py              # Disk storage backend
├── memory.py            # In-memory backend
├── zarr.py              # Zarr backend
├── streaming.py         # ZeroMQ streaming backend
├── filemanager.py       # High-level API
├── atomic.py            # Atomic file operations
└── exceptions.py        # Custom exceptions
```

## Advanced Usage

### Custom Backends

```python
from polystore import StorageBackend

class MyBackend(StorageBackend):
    _backend_type = 'my_backend'  # Auto-registers
    
    def save(self, data, file_path, **kwargs):
        # Your save logic
        pass
    
    def load(self, file_path, **kwargs):
        # Your load logic
        pass
```

### Batch Operations

```python
# Save multiple files
data_list = [np.random.rand(100, 100) for _ in range(10)]
paths = [f"image_{i}.npy" for i in range(10)]
fm.save_batch(data_list, paths, backend="disk")

# Load multiple files
loaded_list = fm.load_batch(paths, backend="disk")
```

### Atomic Writes

```python
from polystore import atomic_write, atomic_write_json

# Atomic file write with automatic locking
with atomic_write("output.txt") as f:
    f.write("data")

# Atomic JSON write
atomic_write_json({"key": "value"}, "config.json")
```

## Why polystore?

**Before** (Manual backend management):
```python
if backend == 'disk':
    np.save(path, data)
elif backend == 'memory':
    cache[path] = data
elif backend == 'zarr':
    zarr.save(path, data)
# ... 50 more lines of if/elif ...
```

**After** (polystore):
```python
fm.save(data, path, backend=backend)
```

## Documentation

Full documentation available at [polystore.readthedocs.io](https://polystore.readthedocs.io)

## Addons

Extend polystore with additional backends:

- **polystore-napari**: Napari viewer streaming backend
- **polystore-fiji**: Fiji/ImageJ streaming backend
- **polystore-omero**: OMERO server backend

## Performance

- **Zero-copy** conversions between frameworks via DLPack (when possible)
- **Lazy loading** for optional dependencies
- **Batch operations** for efficient I/O
- **Atomic writes** with minimal overhead

## License

MIT License - see LICENSE file for details

## Contributing

Contributions welcome! Please see CONTRIBUTING.md for guidelines.

## Credits

Developed by Tristan Simas. Extracted from the [OpenHCS](https://github.com/trissim/openhcs) project.

