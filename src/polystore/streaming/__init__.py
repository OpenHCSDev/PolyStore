"""
Streaming package for polystore.

This package contains:
- StreamingBackend base class
- receivers subpackage with batch processors for Fiji and Napari
"""

# Import StreamingBackend from the _streaming_backend module
# This allows both:
#   from polystore.streaming import StreamingBackend
#   from polystore.streaming.receivers import FijiBatchProcessor
from polystore.streaming._streaming_backend import (
    FilePath,
    RoiStreamPayload,
    StreamablePayload,
    StreamingBackend,
    StreamingBatchItemPreparationAuthority,
    StreamingBatchMessageBuilder,
    StreamingBatchMessageRequest,
    StreamingBuiltBatch,
    StreamingComponentNamesRequest,
    StreamingItemPreparationRequest,
    StreamingPreparedBatchItems,
    StreamingSharedMemoryAuthority,
    ViewerDisplayPayloadExtra,
)

__all__ = [
    "FilePath",
    "RoiStreamPayload",
    "StreamablePayload",
    "StreamingBatchItemPreparationAuthority",
    "StreamingBatchMessageBuilder",
    "StreamingBatchMessageRequest",
    "StreamingBuiltBatch",
    "StreamingPreparedBatchItems",
    "StreamingBackend",
    "StreamingComponentNamesRequest",
    "StreamingItemPreparationRequest",
    "StreamingSharedMemoryAuthority",
    "ViewerDisplayPayloadExtra",
]
