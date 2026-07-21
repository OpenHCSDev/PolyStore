API orientation
===============

.. toctree::
   :maxdepth: 1

   filemanager
   backends
   registry
   atomic
   exceptions

Primary imports
---------------

.. code-block:: python

   from polystore import (
       BackendBase,
       DataSink,
       DataSource,
       DiskBackend,
       FileManager,
       MemoryBackend,
       ReadOnlyBackend,
       STORAGE_BACKENDS,
       StorageBackend,
       ensure_storage_registry,
       storage_registry,
   )

``FileManager`` routes operations through an explicit mapping of backend names
to instances. ``BackendBase.__registry__`` (exported as
``STORAGE_BACKENDS``) is the nominal catalog of backend classes. The lazy
``storage_registry`` is a convenience collection of context-free instances,
not a semantic fallback for FileManager.
