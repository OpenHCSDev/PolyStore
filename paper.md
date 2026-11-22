---
title: "PolyStore: Unified Storage Abstraction with Streaming Backends for Scientific Python"
tags:
  - Python
  - storage
  - scientific computing
  - microscopy
  - streaming
authors:
  - name: Tristan Simas
    orcid: 0000-0002-6526-3149
    affiliation: 1
affiliations:
  - name: McGill University
    index: 1
date: 15 January 2026
bibliography: paper.bib
---

# Summary

PolyStore gives Python programs one call interface for storage backends (files on disk, an in-memory store, Zarr [@zarr]) and for live viewers (napari [@napari], Fiji [@fiji]). A viewer is registered as a backend like the others: saving to it sends the data to the viewer instead of storing it.

```python
import numpy as np
from polystore import DiskBackend, FileManager, MemoryBackend

fm = FileManager({"disk": DiskBackend(), "memory": MemoryBackend()})
image = np.zeros((512, 512), dtype=np.uint16)

# The backend is named on every call
fm.save(image, "result.npy", backend="disk")
fm.save(image, "result.npy", backend="memory")
loaded = fm.load("result.npy", backend="disk")
```

`FileManager` holds a mapping from backend names to instances and passes each call to the named backend. It has no default backend and does not infer one from the path. Backend classes register by name when defined, format libraries are imported on first use, and JSON metadata can be updated atomically under a file lock.

# Statement of Need

Scientific pipelines move data between arrays, files, chunked formats, and visualization tools. Each destination has different I/O conventions:

```python
# Without PolyStore: per-backend code everywhere
np.save("result.npy", data)                    # Disk
memory_store["result.npy"] = data              # Memory
zarr.save("result.zarr", data)                 # Zarr
socket.send(msgpack.packb({"data": data}))    # Streaming
```

With PolyStore the destination is a value, the `backend=` argument. `FileManager` never substitutes another backend: if the named backend lacks the operation (for example `load` on a viewer), the call raises.

# State of the Field

| | PolyStore | fsspec | zarr | xarray |
|---------|---------|------|----|------|
| Unit of abstraction | named backend that saves and loads Python objects by path | filesystem of files and bytes | chunked array format | labelled N-D arrays |
| Destination chosen by | backend name on each call | protocol or URL | store | format engine |
| Write-only viewer destinations | yes | no | no | no |

**fsspec** [@fsspec] gives uniform access to filesystems; its interface is files and byte streams. PolyStore's write interface is `DataSink.save(data, identifier)`, which a destination such as a napari viewer can implement without being able to read anything back.

**zarr** [@zarr] is a chunked array format, which PolyStore uses as one backend. **xarray** [@xarray] provides labelled multi-dimensional arrays with NetCDF and Zarr I/O.

# Software Design

**Backend Hierarchy**: Reading and writing are separate abstract base classes under one registered root, and a backend inherits only what it can do:

```python
class BackendBase(metaclass=AutoRegisterMeta): ...  # registry keyed by _backend_type
class DataSink(BackendBase): ...                    # save, save_batch
class DataSource(BackendBase): ...                  # load, load_batch, list_files, exists
class StorageBackend(DataSource, DataSink): ...     # disk, memory, zarr
class ReadOnlyBackend(DataSource): ...              # virtual_workspace, ome_zarr, bioformats
class StreamingBackend(DataSink): ...               # napari_stream, fiji_stream
```

A subclass that sets `_backend_type` is registered under that name through `metaclass-registry` [@metaclassregistry]; no other list of backends exists. Release 0.6.3 registers nine: the eight above and `omero_local` for OMERO [@omero]. A viewer backend has no `load` and a read-only source has no `save`; calling a missing operation through `FileManager`, or naming an unknown backend, raises `StorageResolutionError`.

**Streaming**: `StreamingBackend.save_batch` converts each array to NumPy, copies it into a shared-memory block, sends one JSON message naming the blocks over a ZeroMQ [@zeromq] request socket, waits for the viewer's reply, and then releases the blocks. ROI lists become napari shapes or ImageJ ROI bytes. The caller passes one keyword, `stream_request`, a `ViewerStreamRequest` carrying the viewer endpoint, display configuration, and source metadata. Text, and items whose path extension is neither a pixel format nor `.roi.zip`, are skipped without an error.

**Atomic Metadata Updates**: `atomic_update_json()` takes an exclusive lock on a sidecar `.lock` file (through `portalocker` on every platform), reads the JSON file, applies the caller's function, writes the result to a temporary file, and replaces the original with `os.replace`. `AtomicMetadataWriter` builds plate-metadata updates on it, and the Zarr backend takes the same lock while it writes OME-Zarr plate metadata. Array and image files are written directly, without this protection.

**Lazy Loading**: `import polystore` imports no backend module; each exported name loads its module on first access. The disk backend imports `tifffile`, `imageio`, PyTorch, and the other format libraries when a file of that format is first read or written. The process-wide `storage_registry` is built on first read.

**Batch Operations**: `save_batch()` and `load_batch()` take lists of data and paths. The disk and memory backends loop over the items. The Zarr backend writes a batch of 2-D planes as one OME-Zarr [@ome_zarr] well, stacking them into one array per field and recording a filename-to-index map so that `load_batch()` returns planes by their original paths; it requires the keywords `chunk_name`, `batch_layout`, `row`, and `col`. A streaming backend sends one message per batch.

# Research Application

PolyStore was developed for OpenHCS (Open High-Content Screening), where microscopy pipelines process the images of a multi-well plate. A typical workflow:

1. **Load**: Read TIFF images from disk, or through `virtual_workspace`, a read-only backend that maps virtual paths recorded in plate metadata to pixels held by another backend
2. **Process**: Apply filters, segmentation, feature extraction in memory
3. **Save**: Write results to OME-Zarr (chunked, zlib-compressed by default)
4. **Stream**: Send intermediate results to napari or Fiji for live preview

```python
# layout: polystore.zarr_batch.ZarrBatchLayout
# request: polystore.streaming.viewer_transport.ViewerStreamRequest
images = fm.load_batch(paths, backend="disk")
processed = pipeline(images)
fm.save_batch(processed, out_paths, backend="zarr",
              chunk_name="A01", batch_layout=layout, row="A", col="01")
fm.save_batch(processed, out_paths, backend="napari_stream",
              stream_request=request)
```

Arguments that only one backend needs travel as keywords; OpenHCS asks each destination for them through `contextual_save_kwargs()`. OpenHCS writes plate metadata through `AtomicMetadataWriter`. A `FileManager` can be sent to worker processes: it pickles as its backend names plus the connection parameters of backends that declare them, and the backends are rebuilt in the worker.

# AI Usage Disclosure

Generative AI (Claude) assisted with code generation and documentation. All content was reviewed and tested.

# Acknowledgements

This work was supported in part by the Fournier lab at the Montreal Neurological Institute, McGill University.

# References
