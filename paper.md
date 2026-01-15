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

PolyStore provides a unified API for heterogeneous storage backends—disk, memory, Zarr, and live streaming to Napari or Fiji—through a single interface. The key insight: **streaming viewers are just backends**:

```python
from polystore import FileManager, BackendRegistry

fm = FileManager(BackendRegistry())

# Same API for persistent storage, cache, and live visualization
fm.save(image, "result.npy", backend="disk")
fm.save(image, "result.npy", backend="memory")
fm.save(image, "result.npy", backend="napari_stream")  # Appears in Napari
```

The `FileManager` routes operations to explicitly selected backends with no implicit fallback. Backends auto-register via metaclass, support lazy imports for optional dependencies, and provide atomic file operations for concurrent metadata updates.

# Statement of Need

Scientific pipelines move data between arrays, files, chunked formats, and visualization tools. Each destination has different I/O conventions:

```python
# Without PolyStore: per-backend code everywhere
np.save("result.npy", data)                    # Disk
memory_store["result.npy"] = data              # Memory
zarr.save("result.zarr", data)                 # Zarr
socket.send(msgpack.packb({"data": data}))    # Streaming
```

With PolyStore, one call handles all backends. The explicit `backend=` parameter ensures deterministic behavior—no silent fallbacks, no hidden resolution logic.

# State of the Field

| Feature | PolyStore | fsspec | zarr | xarray |
|---------|:---------:|:------:|:----:|:------:|
| Unified storage API | ✓ | ✓ | — | — |
| Streaming backends | ✓ | — | — | — |
| Multi-framework I/O | ✓ | — | — | ✓ |
| Atomic concurrent writes | ✓ | — | — | — |
| Explicit backend selection | ✓ | — | — | — |
| Zero implicit fallback | ✓ | — | — | — |

**fsspec** [@fsspec] provides unified filesystem access but lacks streaming and array framework handling. **zarr** [@zarr] handles chunked arrays but is a single format, not a storage abstraction. **xarray** [@xarray] provides multi-dimensional arrays with NetCDF/Zarr backends but no streaming or explicit backend routing.

# Software Design

**Backend Hierarchy**: `DataSource` (read-only), `DataSink` (write-only), `StorageBackend` (read/write). Backends auto-register via `metaclass-registry` [@metaclassregistry] and are lazily instantiated.

**FileManager**: Thin router enforcing explicit backend selection. No magic resolution—if you don't specify a backend, you get an error.

**Streaming Backends**: ZeroMQ transport with shared memory for zero-copy image transfer. ROI data model provides backend-neutral shapes/points with converters for Napari and Fiji.

**Atomic Operations**: Cross-platform file locking (`fcntl` on Unix, `portalocker` on Windows) with `atomic_update_json()` for concurrent metadata writes from multiple pipeline workers.

```python
# Multiple workers safely update shared metadata
from polystore import AtomicMetadataWriter

writer = AtomicMetadataWriter()
writer.merge_subdirectory_metadata(metadata_path, {
    "TimePoint_1": {"available_backends": {"zarr": True}}
})
```

# Research Application

PolyStore was developed for OpenHCS (Open High-Content Screening) where microscopy pipelines:

- Load images from disk or virtual workspace
- Process in memory (avoiding I/O between steps)
- Write results to Zarr (chunked, compressed)
- Stream intermediate results to Napari for live preview

All through one interface:

```python
# Load → process → save → stream: same API
images = fm.load_batch(paths, backend="disk")
processed = pipeline(images)
fm.save_batch(processed, paths, backend="zarr")
fm.save_batch(processed, paths, backend="napari_stream")
```

The explicit backend model eliminated an entire class of bugs where code assumed disk storage but ran against memory or streaming backends.

# AI Usage Disclosure

Generative AI (Claude) assisted with code generation and documentation. All content was reviewed and tested.

# Acknowledgements

This work was supported in part by the Fournier lab at the Montreal Neurological Institute, McGill University.

# References
