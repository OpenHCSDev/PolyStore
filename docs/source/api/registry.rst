Backend registration
====================

Class catalog
-------------

``BackendBase`` uses metaclass-registry. Concrete subclasses declare
``_backend_type`` and appear in ``BackendBase.__registry__``. The same registry
is exported as ``STORAGE_BACKENDS``.

.. code-block:: python

   from polystore import BackendBase, STORAGE_BACKENDS

   assert STORAGE_BACKENDS is BackendBase.__registry__

Execution-local instances
-------------------------

Applications construct the exact backend instances valid for one execution
context and give that mapping to ``FileManager``:

.. code-block:: python

   from polystore import DiskBackend, FileManager, MemoryBackend

   registry = {
       "disk": DiskBackend(),
       "memory": MemoryBackend(),
   }
   files = FileManager(registry)

Two FileManagers share state only when the application gives them the same
instances. This is especially important for the memory backend.

Discovered instances
--------------------

.. code-block:: python

   from polystore import ensure_storage_registry, storage_registry

   ensure_storage_registry()
   installed = dict(storage_registry)

Discovery instantiates context-free registered backends. It skips backends
that require a plate root, server connection, or other application state.

Extension rule
--------------

Declare a concrete backend under the narrowest nominal interface and rely on
the root registry. Do not maintain another class dictionary. Host applications
may add a context-specific instance directly to their FileManager mapping.
