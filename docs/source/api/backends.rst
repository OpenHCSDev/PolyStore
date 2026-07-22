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
