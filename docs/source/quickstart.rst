Quick start
===========

Explicit registry
-----------------

``FileManager`` requires an execution-local registry. Construct only the
backends that the context supports:

.. code-block:: python

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

   files.ensure_directory("/scratch", backend="memory")
   files.save(data, "/scratch/data.npy", backend="memory")
   cached = files.load("/scratch/data.npy", backend="memory")

Every operation names its backend. PolyStore does not infer storage semantics
from an extension or search a fallback chain.

Discovered registry
-------------------

Applications that want every context-free installed backend can initialize the
package registry and copy its instances into a FileManager:

.. code-block:: python

   from polystore import FileManager, ensure_storage_registry, storage_registry

   ensure_storage_registry()
   files = FileManager(dict(storage_registry))

The discovery helper deliberately skips backends that require application
context, such as a plate root or server connection. Add those instances to the
mapping explicitly.

Batch operations
----------------

.. code-block:: python

   values = [np.arange(3), np.arange(3, 6)]
   paths = [Path("first.npy"), Path("second.npy")]
   files.save_batch(values, paths, backend="disk")
   loaded = files.load_batch(paths, backend="disk")

Backend extension
-----------------

Extend the narrowest nominal interface that matches the capability:
``DataSource`` for reads, ``DataSink`` for writes, ``StorageBackend`` for both,
or ``ReadOnlyBackend`` for read-only storage. Concrete declarations register on
``BackendBase.__registry__`` through metaclass-registry.
