Atomic metadata operations
==========================

PolyStore exports cross-platform lock and JSON update primitives:

``file_lock(path, timeout=..., poll_interval=...)``
   Exclusive advisory lock context manager.

``atomic_write_json(path, data, ...)``
   Write JSON through a temporary file and atomic replacement.

``atomic_update_json(path, update_func, ...)``
   Lock, read, transform, and replace a JSON document.

``FileLockError`` and ``FileLockTimeoutError``
   Lock/write failure types.

.. code-block:: python

   from polystore import atomic_update_json

   def increment(document):
       document["revision"] = document.get("revision", 0) + 1
       return document

   atomic_update_json("metadata.json", increment)

There is no exported generic ``atomic_write`` context manager. Use
``atomic_write_json`` for JSON metadata or a storage backend for other values.
