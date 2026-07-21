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

Producer and component identity
-------------------------------

Every projected item carries its own ``StreamProducerIdentity`` alongside its
metadata. ``projection_key`` is the producer-declared grouping identity used in
the route; it is not recovered from a display label or list position. Pipeline
position, scope, and step name add route context when present, while
``output_key`` remains the exact output identity used in collision diagnostics.

``group_items_by_component_modes()`` obtains window, channel, slice, and frame
axes only from the declared display layout. Each item must provide metadata for
every declared component. Items are projected one by one, so the producer
identity is aligned with the exact payload it describes rather than maintained
in a parallel batch-level list.

Within one projected window, two distinct producers may not occupy the same
declared component coordinate with the same data type. That condition raises an
error instead of silently overwriting a layer or selecting one producer by
priority. Viewer-specific renderers consume the validated grouped result; they
do not redefine producer or component identity.

Design Outcome
--------------

Receiver implementations (for example napari and Fiji wrappers in downstream
applications) can share one projection/batching kernel while keeping
viewer-specific rendering code separate.

See Also
--------

- `ZMQRuntime viewer streaming architecture sources
  <https://github.com/OpenHCSDev/ZMQRuntime/tree/main/docs/source/architecture>`_
