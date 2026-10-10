"""
Batch processors for streaming receivers.

Provides reusable batching and debouncing logic for Fiji and Napari viewers.
"""

from polystore.streaming.receivers.core import (
    BatchEngineABC,
    WindowProjectionABC,
    DebouncedBatchEngine,
    GroupedWindowItems,
    WindowProjectionPayloadProvider,
    WindowProjectionSource,
    group_items_into_windows,
)
from polystore.streaming.receivers.fiji.fiji_batch_processor import FijiBatchProcessor
from polystore.streaming.receivers.napari import (
    NapariBatchProcessor,
    build_route_key,
)

__all__ = [
    "BatchEngineABC",
    "WindowProjectionABC",
    "DebouncedBatchEngine",
    "GroupedWindowItems",
    "WindowProjectionPayloadProvider",
    "WindowProjectionSource",
    "group_items_into_windows",
    "FijiBatchProcessor",
    "NapariBatchProcessor",
    "build_route_key",
]
