Custom backends
===============

Choose the narrowest interface
------------------------------

- inherit ``DataSource`` for a readable source;
- inherit ``DataSink`` for a write-only destination or stream;
- inherit ``StorageBackend`` for read/write file-like storage;
- inherit ``ReadOnlyBackend`` for a read-only virtual or remote source.

Every concrete backend declares ``_backend_type``. The metaclass registers it
on ``BackendBase.__registry__``.

Minimal sink example
--------------------

.. code-block:: python

   from pathlib import Path
   from polystore import DataSink, FileManager

   class AuditSink(DataSink):
       _backend_type = "audit"

       @property
       def requires_filesystem_validation(self):
           return False

       def save(self, data, identifier, **kwargs):
           print(identifier, type(data).__name__)

       def save_batch(self, data_list, identifiers, **kwargs):
           if len(data_list) != len(identifiers):
               raise ValueError("data_list and identifiers must have equal length")
           for data, identifier in zip(data_list, identifiers):
               self.save(data, identifier, **kwargs)

   files = FileManager({"audit": AuditSink()})
   files.save({"status": "ok"}, Path("run-1"), backend="audit")

Readable and storage backends must implement the abstract operations declared
by ``DataSource`` and ``StorageBackend``. Do not provide placeholder methods or
fallback behavior merely to satisfy the interface.

Context-specific construction
-----------------------------

If a backend needs credentials, a workspace root, or an application service,
construct it at that owning boundary and add the instance to the local
FileManager mapping. Generic discovery cannot supply context-specific state.

Testing
-------

Test direct backend operations, FileManager routing, missing/unsupported
operations, serialization if the backend is picklable, and cleanup/lifecycle
behavior. Assert registration through ``BackendBase.__registry__`` rather than
a second test registry.
