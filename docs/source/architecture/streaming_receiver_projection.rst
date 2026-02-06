Streaming Receiver Projection
=============================

Modules
-------

- ``polystore.streaming.receivers.core.contracts``
- ``polystore.streaming.receivers.core.batch_engine``
- ``polystore.streaming.receivers.core.window_projection``
- ``polystore.streaming.receivers.napari.layer_key``

Purpose
-------

Provide reusable, viewer-agnostic receiver-side primitives for streaming
payload projection and batched update scheduling.

Boundary
--------

``polystore`` owns payload semantics and receiver projection mechanics:

- component-mode grouping into window/channel/slice/frame structures
- canonical layer-key derivation from component metadata
- debounced batch processing with bounded wait behavior
- receiver contracts via nominal ABCs

``polystore`` does not own ZMQ transport lifecycle. Transport/server ownership
belongs to ``zmqruntime``.

Core Contracts
--------------

``BatchEngineABC``
  Contract for enqueue/flush behavior in receiver-side batch schedulers.

``WindowProjectionABC``
  Contract for grouping stream items into projected window structures.

Reference Implementations
-------------------------

``DebouncedBatchEngine``
  Thread-safe debounce + max-wait engine for coalescing incoming items before
  projection/render updates.

``group_items_by_component_modes``
  Canonical grouping utility that projects incoming items by declared component
  modes and returns stable ``GroupedWindowItems`` output.

``build_layer_key``
  Canonical napari layer-key construction from slice-mode components and data
  type.

Design Outcome
--------------

Receiver implementations (for example napari and Fiji wrappers in downstream
applications) can share one projection/batching kernel while keeping
viewer-specific rendering code separate.

See Also
--------

- ``external/zmqruntime/docs/source/architecture/viewer_streaming_architecture.rst``
