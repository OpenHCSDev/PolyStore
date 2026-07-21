# PolyStore

Framework-agnostic storage primitives for scientific applications.

[![PyPI version](https://badge.fury.io/py/polystore.svg)](https://badge.fury.io/py/polystore)
[![Documentation Status](https://readthedocs.org/projects/polystore/badge/?version=latest)](https://polystore.readthedocs.io/en/latest/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

PolyStore owns backend interfaces, execution-local backend registries,
``FileManager`` routing, storage formats, ROI values, virtual-workspace source
references, and viewer-streaming payload mechanics. Applications retain
ownership of domain artifact names and materialization policy.

## Quick start

Pass an explicit mapping of backend names to instances. ``FileManager`` has no
global fallback and does not infer a backend from a path.

```python
from pathlib import Path
import numpy as np

from polystore import DiskBackend, FileManager, MemoryBackend

registry = {
    "disk": DiskBackend(),
    "memory": MemoryBackend(),
}
files = FileManager(registry)

data = np.arange(6).reshape(2, 3)
files.save(data, Path("output.npy"), backend="disk")
loaded = files.load(Path("output.npy"), backend="disk")
```

For application startup where every discoverable context-free backend is
wanted, use the package-owned lazy registry:

```python
from polystore import FileManager, ensure_storage_registry, storage_registry

ensure_storage_registry()
files = FileManager(dict(storage_registry))
```

Backends requiring context-specific construction, including virtual workspaces
and OMERO, must be instantiated by the application and added to its registry.

## Nominal backend extension

Concrete backends inherit ``DataSource``, ``DataSink``, ``StorageBackend``, or
``ReadOnlyBackend`` and declare their backend key. The authoritative class
catalog is ``BackendBase.__registry__`` (also exported as
``STORAGE_BACKENDS``). Do not build a second backend class table.

## Installation

```bash
python -m pip install polystore
```

The core install includes NumPy, ArrayBridge, metaclass-registry, file locking,
image I/O, and Zarr/OME-Zarr support. Framework and streaming extras add their
corresponding optional runtimes.

Full documentation: [polystore.readthedocs.io](https://polystore.readthedocs.io)
