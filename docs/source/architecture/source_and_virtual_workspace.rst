Source references and virtual workspaces
========================================

Ownership boundary
------------------

PolyStore owns the format-neutral reference that connects a virtual path to a
registered source backend. The source backend owns the meaning of its address
and the operation that loads it. Host applications own plate discovery and the
policy that writes workspace metadata; they do not reinterpret backend
addresses during loading.

``SourcePixelRef``
------------------

``SourcePixelRef`` is the sole structured value stored in a virtual workspace
mapping. It contains exactly:

``backend``
  The execution-local ``FileManager`` key for a ``DataSource``.

``backend_address``
  An opaque string interpreted only by that source backend.

``source_axis_indices``
  Zero or more nonnegative leading-axis indices applied after the source is
  loaded. Each index selects axis zero of the value produced by the preceding
  selection.

The JSON mapping is exact: missing or additional fields are rejected rather
than accepted through a compatibility fallback.

.. code-block:: python

   from polystore import SourcePixelRef

   source = SourcePixelRef(
       backend="bioformats",
       backend_address='{"plane_index":0,"series_index":2,"source_path":"plate.czi"}',
       source_axis_indices=(1,),
   )
   workspace_value = source.to_workspace_mapping()

Backend-owned addresses
-----------------------

``backend_address`` is not a filesystem-path convention. ``FileManager`` and
``VirtualWorkspaceBackend`` pass it to the selected ``DataSource.resolve_address``
implementation. For example, ``BioFormatsPlaneRef`` in
``polystore.bioformats_storage`` serializes a source path, series index, and
plane index as one canonical JSON address. Other backends may use paths, object
identifiers, or another exact string encoding without changing
``SourcePixelRef``.

``VirtualWorkspaceBackend``
---------------------------

``VirtualWorkspaceBackend`` is a read-only projection over a plate metadata
file. It loads each ``subdirectories.*.workspace_mapping`` entry and maps a
virtual relative path to one ``SourcePixelRef``.

The backend is constructed with a ``plate_root`` and then bound to the same
execution-local registry used by its ``FileManager``. Loading follows one
authority-preserving path:

1. Resolve the virtual path to its exact ``SourcePixelRef``.
2. Select ``ref.backend`` from the bound registry and require a ``DataSource``.
3. Ask that backend to resolve ``ref.backend_address`` relative to the plate
   root.
4. Load once through that backend.
5. Apply ``ref.source_axis_indices`` in order.

A reference cannot target ``virtual_workspace`` itself, and an absent or
non-source backend fails with ``StorageResolutionError``. There is no fallback
backend or path-based backend inference.

Batch behavior
--------------

Batch loads group references with the same ``(backend, backend_address)``. The
source value is loaded once for each group, then each reference applies its own
leading-axis projection. This preserves source identity while avoiding repeated
reads of a shared stack.

Bounded sampling through a workspace
------------------------------------

``VirtualWorkspaceBackend.sample()`` follows the same authority path as
``load()``: it resolves the exact ``SourcePixelRef``, delegates the unchanged
``ImageSamplingRequest`` to that source's ``sample()`` method, and then applies
the reference's leading-axis selections to both ``data`` and
``statistics_data``. It projects ``source_shape`` and ``resolution_shape`` by
the same number of leading selections.

The selected resolution, resolution count, downsample factors, sample origin,
and statistics scope remain those reported by the source. The virtual workspace
does not resample pixels or infer new provenance. This makes a virtual path a
projection of a backend-owned native sample rather than a second sampling
implementation.

Process boundary
----------------

The virtual workspace's pickled connection state contains only ``plate_root``.
After reconstruction, the owning ``FileManager`` must bind the worker's complete
execution-local registry before the workspace can resolve source references.
Backend instances and credentials are never encoded into workspace mappings.
