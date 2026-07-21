File-format declarations
========================

``FileFormat`` is PolyStore's nominal declaration of storage-format semantics.
Each enum member owns four facts:

``value``
  Stable format identity such as ``"numpy"``, ``"tiff"``, or ``"json"``.

``extensions``
  All filename suffixes recognized for that format.

``is_pixel_payload``
  Whether the format represents pixel or array data rather than metadata,
  tables, text, or ROIs.

``is_raster_source``
  Whether the format is a raster image source suitable for default image-file
  discovery.

``DEFAULT_IMAGE_EXTENSIONS`` is derived from the members whose
``is_raster_source`` declaration is true. Consumers must iterate ``FileFormat``
or use that derived set rather than maintain a second extension table.

.. code-block:: python

   from polystore import DEFAULT_IMAGE_EXTENSIONS, FileFormat

   assert ".tif" in FileFormat.TIFF.extensions
   assert FileFormat.TIFF.is_pixel_payload
   assert FileFormat.TIFF.is_raster_source
   assert ".tif" in DEFAULT_IMAGE_EXTENSIONS

Resolution and runtime availability
-----------------------------------

``polystore.formats.get_format_from_extension`` performs case-insensitive
extension-to-format resolution and raises ``ValueError`` for an undeclared
extension. It identifies semantics; it does not promise that an installed
backend can read or write that format.

Concrete backends own runtime codec availability. For example,
``DiskStorageBackend`` builds its local reader/writer registration from the
dependencies available in that environment. Callers request the operation from
the selected backend and let an unsupported format fail at that boundary. They
must not infer another backend or silently substitute a different format.

Extension rule
--------------

Adding a format requires extending ``FileFormat`` at the declaration boundary,
then implementing the relevant backend codec registration and tests. A new
extension list in host code is not an alternative declaration mechanism.
