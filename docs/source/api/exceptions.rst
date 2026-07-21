Exceptions
==========

Storage boundary failures live in ``polystore.exceptions``:

.. code-block:: python

   from polystore.exceptions import (
       ImageLoadError,
       ImageSaveError,
       MetadataNotFoundError,
       PathMismatchError,
       StorageResolutionError,
       StorageWriteError,
       VFSTypeError,
   )

The exception types are specific structural or operation failures; there is no
``StorageError`` superclass and no ``BackendNotFoundError``. FileManager uses
``StorageResolutionError`` when a backend key or address cannot be resolved.

Locking failures are exported separately as ``FileLockError`` and
``FileLockTimeoutError``.
