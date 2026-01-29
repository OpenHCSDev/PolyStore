Creating Custom Backends
========================

This guide shows you how to create custom storage backends for polystore.

Overview
--------

Custom backends allow you to:

* Integrate with custom storage systems
* Add support for new file formats
* Implement specialized I/O patterns
* Connect to remote services (S3, databases, etc.)

Backend inheritance provides automatic registration and a consistent API.

Basic Backend
-------------

The simplest backend implements the ``StorageBackend`` interface:

.. code-block:: python

   from polystore import StorageBackend
   import json
   from pathlib import Path
   
   class JSONStorageBackend(StorageBackend):
       """Backend that stores everything as JSON."""
       
       _backend_type = 'json_storage'  # Auto-registration key
       
       def __init__(self, base_dir="./json_storage"):
           self.base_dir = Path(base_dir)
           self.base_dir.mkdir(parents=True, exist_ok=True)
       
       def save(self, data, file_path, **kwargs):
           """Save data as JSON."""
           full_path = self.base_dir / file_path
           full_path.parent.mkdir(parents=True, exist_ok=True)
           
           with open(full_path, 'w') as f:
               json.dump(data, f, indent=2)
       
       def load(self, file_path, **kwargs):
           """Load data from JSON."""
           full_path = self.base_dir / file_path
           
           if not full_path.exists():
               raise FileNotFoundError(f"File not found: {file_path}")
           
           with open(full_path, 'r') as f:
               return json.load(f)
       
       def save_batch(self, data_list, output_paths, **kwargs):
           """Save multiple files."""
           for data, path in zip(data_list, output_paths):
               self.save(data, path, **kwargs)
       
       def load_batch(self, file_paths, **kwargs):
           """Load multiple files."""
           return [self.load(path, **kwargs) for path in file_paths]
       
       def list_files(self, directory, pattern=None, 
                     extensions=None, recursive=False, **kwargs):
           """List files in directory."""
           dir_path = self.base_dir / directory
           
           if not dir_path.exists():
               return []
           
           if recursive:
               files = dir_path.rglob("*.json")
           else:
               files = dir_path.glob("*.json")
           
           return [str(f.relative_to(self.base_dir)) for f in files]
       
       def exists(self, path):
           """Check if path exists."""
           return (self.base_dir / path).exists()
       
       def is_file(self, path):
           """Check if path is a file."""
           return (self.base_dir / path).is_file()
       
       def is_dir(self, path):
           """Check if path is a directory."""
           return (self.base_dir / path).is_dir()
       
       def list_dir(self, path):
           """List directory entries."""
           dir_path = self.base_dir / path
           if not dir_path.is_dir():
               raise NotADirectoryError(f"Not a directory: {path}")
           return [f.name for f in dir_path.iterdir()]

Usage:

.. code-block:: python

   from polystore import BackendRegistry, FileManager
   
   # Backend is auto-registered
   registry = BackendRegistry()
   fm = FileManager(registry)
   
   # Use the custom backend
   data = {"key": "value", "count": 42}
   fm.save(data, "config.json", backend="json_storage")
   
   loaded = fm.load("config.json", backend="json_storage")
   print(loaded)  # {'key': 'value', 'count': 42}

Read-Only Backend
-----------------

For read-only data sources, inherit from ``ReadOnlyBackend``:

.. code-block:: python

   from polystore import ReadOnlyBackend
   import requests
   
   class HTTPBackend(ReadOnlyBackend):
       """Read-only backend for HTTP resources."""
       
       _backend_type = 'http'
       
       def __init__(self, base_url):
           self.base_url = base_url.rstrip('/')
       
       def load(self, file_path, **kwargs):
           """Load data from HTTP."""
           url = f"{self.base_url}/{file_path}"
           response = requests.get(url)
           response.raise_for_status()
           return response.content
       
       def load_batch(self, file_paths, **kwargs):
           """Load multiple files."""
           return [self.load(path, **kwargs) for path in file_paths]
       
       def list_files(self, directory, **kwargs):
           """List files (not implemented for HTTP)."""
           raise NotImplementedError("HTTP backend doesn't support listing")
       
       def exists(self, path):
           """Check if resource exists."""
           url = f"{self.base_url}/{path}"
           response = requests.head(url)
           return response.status_code == 200
       
       def is_file(self, path):
           """Always true for HTTP resources."""
           return self.exists(path)
       
       def is_dir(self, path):
           """Always false for HTTP."""
           return False
       
       def list_dir(self, path):
           """Not supported."""
           raise NotImplementedError()

Advanced Features
-----------------

Context Manager Support
~~~~~~~~~~~~~~~~~~~~~~~

Implement cleanup with context managers:

.. code-block:: python

   from polystore import StorageBackend
   import tempfile
   import shutil
   
   class TemporaryBackend(StorageBackend):
       """Backend with automatic cleanup."""
       
       _backend_type = 'temporary'
       
       def __init__(self):
           self.temp_dir = None
       
       def __enter__(self):
           self.temp_dir = tempfile.mkdtemp()
           return self
       
       def __exit__(self, exc_type, exc_val, exc_tb):
           if self.temp_dir:
               shutil.rmtree(self.temp_dir)
       
       # Implement required methods...

Usage:

.. code-block:: python

   with TemporaryBackend() as backend:
       registry = {'temp': backend}
       fm = FileManager(registry)
       
       fm.save(data, "temp_file.npy", backend="temp")
       # Files automatically cleaned up on exit

Compression Support
~~~~~~~~~~~~~~~~~~~

Add compression/decompression:

.. code-block:: python

   from polystore import StorageBackend
   import gzip
   import pickle
   
   class CompressedBackend(StorageBackend):
       """Backend with automatic compression."""
       
       _backend_type = 'compressed'
       
       def save(self, data, file_path, **kwargs):
           """Save with compression."""
           with gzip.open(file_path, 'wb') as f:
               pickle.dump(data, f)
       
       def load(self, file_path, **kwargs):
           """Load with decompression."""
           with gzip.open(file_path, 'rb') as f:
               return pickle.load(f)
       
       # Implement other required methods...

Caching Layer
~~~~~~~~~~~~~

Add caching for expensive operations:

.. code-block:: python

   from polystore import StorageBackend
   from functools import lru_cache
   
   class CachedBackend(StorageBackend):
       """Backend with LRU cache."""
       
       _backend_type = 'cached'
       
       def __init__(self, wrapped_backend, cache_size=128):
           self.backend = wrapped_backend
           self._cached_load = lru_cache(maxsize=cache_size)(
               self._do_load
           )
       
       def _do_load(self, file_path):
           """Actual load operation."""
           return self.backend.load(file_path)
       
       def load(self, file_path, **kwargs):
           """Load with caching."""
           return self._cached_load(file_path)
       
       def save(self, data, file_path, **kwargs):
           """Save (invalidates cache)."""
           self.backend.save(data, file_path, **kwargs)
           # Clear cache entry if it exists
           try:
               self._cached_load.cache_clear()
           except AttributeError:
               pass
       
       # Delegate other methods to wrapped backend...

Database Backend Example
------------------------

Here's a complete example connecting to a database:

.. code-block:: python

   from polystore import StorageBackend
   import sqlite3
   import pickle
   from pathlib import Path
   
   class SQLiteBackend(StorageBackend):
       """Backend using SQLite for storage."""
       
       _backend_type = 'sqlite'
       
       def __init__(self, db_path="polystore.db"):
           self.db_path = db_path
           self._init_db()
       
       def _init_db(self):
           """Initialize database schema."""
           conn = sqlite3.connect(self.db_path)
           conn.execute('''
               CREATE TABLE IF NOT EXISTS files (
                   path TEXT PRIMARY KEY,
                   data BLOB NOT NULL,
                   created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
               )
           ''')
           conn.commit()
           conn.close()
       
       def save(self, data, file_path, **kwargs):
           """Save data to database."""
           conn = sqlite3.connect(self.db_path)
           serialized = pickle.dumps(data)
           
           conn.execute(
               'INSERT OR REPLACE INTO files (path, data) VALUES (?, ?)',
               (str(file_path), serialized)
           )
           conn.commit()
           conn.close()
       
       def load(self, file_path, **kwargs):
           """Load data from database."""
           conn = sqlite3.connect(self.db_path)
           cursor = conn.execute(
               'SELECT data FROM files WHERE path = ?',
               (str(file_path),)
           )
           row = cursor.fetchone()
           conn.close()
           
           if not row:
               raise FileNotFoundError(f"File not found: {file_path}")
           
           return pickle.loads(row[0])
       
       def save_batch(self, data_list, output_paths, **kwargs):
           """Batch save."""
           conn = sqlite3.connect(self.db_path)
           for data, path in zip(data_list, output_paths):
               serialized = pickle.dumps(data)
               conn.execute(
                   'INSERT OR REPLACE INTO files (path, data) VALUES (?, ?)',
                   (str(path), serialized)
               )
           conn.commit()
           conn.close()
       
       def load_batch(self, file_paths, **kwargs):
           """Batch load."""
           conn = sqlite3.connect(self.db_path)
           results = []
           for path in file_paths:
               cursor = conn.execute(
                   'SELECT data FROM files WHERE path = ?',
                   (str(path),)
               )
               row = cursor.fetchone()
               if not row:
                   raise FileNotFoundError(f"File not found: {path}")
               results.append(pickle.loads(row[0]))
           conn.close()
           return results
       
       def list_files(self, directory, **kwargs):
           """List files in directory."""
           conn = sqlite3.connect(self.db_path)
           pattern = f"{directory}/%"
           cursor = conn.execute(
               'SELECT path FROM files WHERE path LIKE ?',
               (pattern,)
           )
           files = [row[0] for row in cursor.fetchall()]
           conn.close()
           return files
       
       def exists(self, path):
           """Check if path exists."""
           conn = sqlite3.connect(self.db_path)
           cursor = conn.execute(
               'SELECT 1 FROM files WHERE path = ?',
               (str(path),)
           )
           exists = cursor.fetchone() is not None
           conn.close()
           return exists
       
       def is_file(self, path):
           """Check if path is a file."""
           return self.exists(path)
       
       def is_dir(self, path):
           """Directories don't exist in flat database."""
           return False
       
       def list_dir(self, path):
           """List directory entries."""
           return self.list_files(path)

Testing Your Backend
--------------------

Write comprehensive tests for your backend:

.. code-block:: python

   import pytest
   import numpy as np
   from my_package import MyCustomBackend
   
   class TestMyCustomBackend:
       def setup_method(self):
           self.backend = MyCustomBackend()
       
       def test_save_and_load(self):
           data = np.array([1, 2, 3])
           self.backend.save(data, "test.npy")
           loaded = self.backend.load("test.npy")
           np.testing.assert_array_equal(data, loaded)
       
       def test_batch_operations(self):
           data_list = [np.array([i]) for i in range(3)]
           paths = [f"test{i}.npy" for i in range(3)]
           
           self.backend.save_batch(data_list, paths)
           loaded_list = self.backend.load_batch(paths)
           
           for original, loaded in zip(data_list, loaded_list):
               np.testing.assert_array_equal(original, loaded)
       
       def test_file_not_found(self):
           with pytest.raises(FileNotFoundError):
               self.backend.load("nonexistent.npy")

File Extension Filtering (Streaming Backends)
---------------------------------------------

Streaming backends (Napari, Fiji) should declare which file types they support
to avoid attempting to stream incompatible files (CSV, JSON, etc.).

**Declare Supported Extensions:**

.. code-block:: python

   from polystore import StreamingBackend

   class MyStreamingBackend(StreamingBackend):
       """Custom streaming backend with extension filtering."""

       _backend_type = 'my_stream'
       VIEWER_TYPE = 'myviewer'
       SHM_PREFIX = 'myviewer_'

       # Declare which extensions this backend can stream
       SUPPORTED_EXTENSIONS = {
           '.tif', '.tiff', '.png', '.jpg', '.jpeg',
           '.roi.zip',  # Compound extensions work too
       }

       def save_batch(self, data_list, file_paths, **kwargs):
           """Stream data, automatically filtering unsupported files."""
           # Filter to only supported file types
           data_list, file_paths, skipped = self._filter_streamable_files(
               data_list, file_paths
           )

           if not data_list:
               return

           # Now stream only the supported files...
           for data, path in zip(data_list, file_paths):
               self._stream_data(data, path, **kwargs)

**How It Works:**

1. **SUPPORTED_EXTENSIONS**: Class attribute defining valid file extensions
2. **_filter_streamable_files()**: Base class method that filters data
3. **Automatic Logging**: Skipped files are logged at INFO level

**Adding/Removing Support:**

To add support for new formats, simply add to the set:

.. code-block:: python

   SUPPORTED_EXTENSIONS = {
       '.tif', '.tiff',
       '.png', '.jpg', '.jpeg',
       '.roi.zip',
       '.myformat',  # Add your format
   }

To remove support, remove from the set or override in subclass:

.. code-block:: python

   class RestrictedNapariBackend(NapariStreamingBackend):
       """Napari backend that only supports TIFF files."""
       SUPPORTED_EXTENSIONS = {'.tif', '.tiff'}

**Why This Pattern:**

- **Prevents Crashes**: CSV/JSON files won't crash the viewer
- **Clear Contracts**: Developers know what each backend supports
- **Extensible**: Easy to add new formats
- **Consistent**: All streaming backends use the same pattern

Best Practices
--------------

1. **Auto-Registration**: Always define ``_backend_type`` for automatic registration
2. **Error Handling**: Raise appropriate exceptions (``FileNotFoundError``, etc.)
3. **Documentation**: Document your backend's specific features and limitations
4. **Testing**: Write comprehensive tests covering all methods
5. **Thread Safety**: Consider thread-safety if backend maintains state
6. **Resource Cleanup**: Implement ``__del__`` or context managers for cleanup
7. **Type Hints**: Add type hints for better IDE support
8. **Logging**: Use logging for debugging and monitoring
9. **Extension Filtering**: For streaming backends, always use ``SUPPORTED_EXTENSIONS``

Publishing Your Backend
-----------------------

To share your backend:

1. **Package It**: Create a Python package with your backend
2. **Dependencies**: List polystore as a dependency
3. **Documentation**: Document installation and usage
4. **Examples**: Provide example code
5. **Tests**: Include comprehensive tests

Example ``pyproject.toml``:

.. code-block:: toml

   [project]
   name = "polystore-mybackend"
   version = "0.1.0"
   dependencies = [
       "polystore>=0.1.0",
       # Your backend's dependencies
   ]

Example usage in your package:

.. code-block:: python

   # my_package/__init__.py
   from polystore import StorageBackend
   
   class MyBackend(StorageBackend):
       _backend_type = 'mybackend'
       # Implementation...
   
   __all__ = ['MyBackend']

Users can then install and use:

.. code-block:: bash

   pip install polystore-mybackend

.. code-block:: python

   import my_package  # Imports and registers backend
   from polystore import BackendRegistry, FileManager
   
   registry = BackendRegistry()
   assert 'mybackend' in registry  # Auto-registered!

See Also
--------

* :doc:`../api/backends` - Backend API reference
* :doc:`../api/registry` - Registration system
* :doc:`../api/filemanager` - Using backends via FileManager
