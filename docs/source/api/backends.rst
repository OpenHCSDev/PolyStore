Backend interfaces
==================

``BackendBase``
   Nominal root and registry owner. Every backend declares whether filesystem
   validation applies and may bind an execution-local registry.

``DataSource``
   Read, bounded sample, batch-read, listing, existence, type, and
   address-resolution surface.

``DataSink``
   Write surface, including contextual save keywords.

``StorageBackend``
   Read/write storage with file-like operations.

``ReadOnlyBackend``
   Read-only source for virtual, remote, or mounted data.

``StreamingBackend``
   Data sink for typed viewer-stream payloads.

Built-in families
-----------------

The core package exports ``DiskBackend`` / ``DiskStorageBackend`` and
``MemoryBackend`` / ``MemoryStorageBackend``. ``ZarrStorageBackend`` and
integration-specific backends are registered when their modules and optional
requirements are available.

Capability is nominal. Callers should request the operation they require and
let an incompatible interface fail; they should not branch on copied backend
name lists.

Zarr configuration ownership
----------------------------

PolyStore owns the complete generic Zarr configuration boundary:
``ZarrConfig``, ``ZarrCompressor``, ``ZarrCompressorFactory``, and
``ZarrChunkStrategy``. The compressor factory registry is keyed directly by
the owning enum, and ``ZarrStorageBackend`` consumes those same nominal
identities. Applications may subclass ``ZarrConfig`` to attach presentation or
registration metadata, but must not redeclare its storage fields or translate
its enum values through strings or lookup tables.

.. code-block:: python

   from polystore.config import (
       ZarrChunkStrategy,
       ZarrCompressor,
       ZarrConfig,
   )
   from polystore.zarr import ZarrStorageBackend

   backend = ZarrStorageBackend(
       ZarrConfig(
           compressor=ZarrCompressor.ZLIB,
           compression_level=3,
           chunk_strategy=ZarrChunkStrategy.WELL,
       )
   )

Zarr batch layout
-----------------

``ZarrStorageBackend.save_batch`` requires a ``ZarrBatchLayout`` from
``polystore.zarr_batch``. The layout declares every non-pixel axis, its opaque
semantic values, and the exact coordinate of each two-dimensional image plane.
PolyStore validates that the coordinates form one complete dense product, so
item order is never inferred from filenames or list position.

``ZarrBatchAxisRole.ARRAY`` places an axis inside each OME-NGFF image array.
``ZarrBatchAxisRole.HCS_IMAGE`` projects one axis into the well's HCS image
groups. A layout may declare at most one HCS image axis. This keeps each image
array within the OME-NGFF ``t, c, z, y, x`` model while preserving fields or
sites as distinct HCS images.

.. code-block:: python

   from polystore.zarr_batch import (
       ZarrBatchAxis,
       ZarrBatchAxisRole,
       ZarrBatchLayout,
   )

   layout = ZarrBatchLayout(
       axes=(
           ZarrBatchAxis("t", "time", ("1", "2")),
           ZarrBatchAxis(
               "field",
               "field",
               ("3", "7"),
               ZarrBatchAxisRole.HCS_IMAGE,
           ),
           ZarrBatchAxis("c", "channel", ("DNA", "RNA")),
           ZarrBatchAxis("z", "space", ("1",)),
       ),
       item_coordinates=(
           (0, 0, 0, 0),
           (0, 0, 1, 0),
           (0, 1, 0, 0),
           (0, 1, 1, 0),
           (1, 0, 0, 0),
           (1, 0, 1, 0),
           (1, 1, 0, 0),
           (1, 1, 1, 0),
       ),
   )

The backend persists both filename-to-coordinate mappings and semantic axis
values. ``load_batch`` uses the stored coordinates directly, and readers can
use ``ZarrStoredBatchSemantics`` to recover application-owned labels without
teaching PolyStore what those labels mean.

Bounded native sampling
-----------------------

``ImageSamplingRequest`` asks a ``DataSource`` for a bounded spatial region. A
caller may select an exact native resolution or leave resolution selection to
the source using the request's maximum automatic resolution size. The source,
not the caller, owns pyramid discovery and selection.

Every source returns ``ImageSamplingResult``. Its displayed ``data`` is bounded,
while ``source_shape``, ``resolution_shape``, the selected level, level count,
downsample factors, and sample origin preserve the native-resolution context.
``statistics_data`` has an explicit ``ImageSamplingStatisticsScope``: ordinary
single-resolution sources may report statistics over the loaded source, while a
decoder that performs a native region read can report statistics over only the
bounded sample. Consumers must inspect that scope instead of assuming whole-
source statistics.

``DataSource.sample()`` is the generic template method. It loads an ordinary
source and bounds its trailing Y/X axes. Decoder leaves override it when they can
read a native region or pyramid level without loading the full image. A request
for a non-existent explicit level fails; the generic implementation does not
simulate a resolution pyramid.
