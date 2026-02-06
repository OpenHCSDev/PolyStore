"""
Batch processors for streaming receivers.

Provides reusable batching and debouncing logic for Fiji and Napari viewers.
"""

from polystore.streaming.receivers.core import (
    BatchEngineABC,
    WindowProjectionABC,
    DebouncedBatchEngine,
    GroupedWindowItems,
    group_items_by_component_modes,
)
from polystore.streaming.receivers.fiji.fiji_batch_processor import FijiBatchProcessor
from polystore.streaming.receivers.napari import (
    NapariBatchProcessor,
    normalize_component_layout,
    build_layer_key,
)

__all__ = [
    "BatchEngineABC",
    "WindowProjectionABC",
    "DebouncedBatchEngine",
    "GroupedWindowItems",
    "group_items_by_component_modes",
    "FijiBatchProcessor",
    "NapariBatchProcessor",
    "normalize_component_layout",
    "build_layer_key",
]
