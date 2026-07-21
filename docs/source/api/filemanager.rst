FileManager
===========

``FileManager(registry)`` is an execution-local router over an explicit mapping
of backend names to ``BackendBase`` instances. Passing ``None`` is an error.

Core operations
---------------

All operations name a backend explicitly:

.. code-block:: python

   files.save(value, "result.npy", backend="disk")
   value = files.load("result.npy", backend="disk")

   files.save_batch(values, paths, backend="disk")
   values = files.load_batch(paths, backend="disk")

The manager also delegates directory and path operations including
``list_files``, ``list_dir``, ``ensure_directory``, ``exists``, ``is_file``,
``is_dir``, ``copy``, ``move``, ``delete``, and symlink operations. Whether an
operation is supported is determined by the nominal backend capability.

Sampling one source
-------------------

``sample(file_path, backend, request)`` requires the selected backend to be a
``DataSource`` and delegates the complete ``ImageSamplingRequest`` unchanged:

.. code-block:: python

   from polystore import ImageSamplingRequest

   sample = files.sample(
       "image.tif",
       backend="disk",
       request=ImageSamplingRequest(origin_yx=(16, 32), shape_yx=(64, 64)),
   )

The returned ``ImageSamplingResult`` is the backend's authoritative bounded data
and native-resolution provenance. ``FileManager`` does not choose levels, infer
statistics coverage, or reconstruct provenance from array shapes.

Address resolution
------------------

``resolve_address(backend_address, backend, base_path=...)`` delegates address
meaning to a registered ``DataSource``. FileManager does not infer a backend or
reinterpret a virtual address.

Adding one instance
-------------------

``register_backend(name, instance)`` adds and binds a backend to that manager's
local mapping. Use it for a backend constructed with context that generic
package discovery cannot supply.

Failure behavior
----------------

An absent backend, unsupported source/sink capability, invalid path, or backend
failure raises at the operation boundary. No fallback backend is searched.
