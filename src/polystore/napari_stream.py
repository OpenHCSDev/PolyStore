"""
Napari streaming backend for real-time visualization during processing.

This module provides a storage backend that streams image data to a napari viewer
for real-time visualization during pipeline execution. Uses ZeroMQ for IPC
and shared memory for efficient data transfer.

SHARED MEMORY OWNERSHIP MODEL:
- Sender (Worker): Creates shared memory, sends reference via ZMQ, closes handle (does NOT unlink)
- Receiver (Napari Server): Attaches to shared memory, copies data, closes handle, unlinks
- Only receiver calls unlink() to prevent FileNotFoundError
- REQ/REP socket pattern is blocking; worker waits for acknowledgment before closing shared memory
"""

import logging
from enum import Enum

from zmqruntime.viewer_protocol import (
    ViewerBatchItemWireField,
    ViewerWireValue,
)

from .constants import Backend
from .roi_converters import NapariROIConverter
from .streaming import (
    FilePath,
    RoiStreamPayload,
    StreamingBackend,
    StreamingItemPreparationRequest,
)
from .streaming.viewer_transport import ViewerStreamItemPayload

logger = logging.getLogger(__name__)


class NapariDisplayWireField(str, Enum):
    """Napari-specific display fields inside the shared viewer display payload."""

    COLORMAP = "colormap"
    VARIABLE_SIZE_HANDLING = "variable_size_handling"


class NapariStreamingBackend(StreamingBackend):
    """Napari streaming backend with automatic registration."""
    _backend_type = Backend.NAPARI_STREAM.value

    VIEWER_TYPE = 'napari'
    SHM_PREFIX = 'napari_'

    def _prepare_shapes_data(
        self,
        data: RoiStreamPayload,
        file_path: FilePath,
    ) -> dict[str, ViewerWireValue]:
        """
        Prepare shapes data for transmission.

        Args:
            data: ROI list
            file_path: Path identifier

        Returns:
            Dict with shapes data
        """
        shapes_data = NapariROIConverter.rois_to_shapes(data)

        return {
            ViewerBatchItemWireField.PATH.value: str(file_path),
            ViewerBatchItemWireField.SHAPES.value: shapes_data,
        }

    def _prepare_batch_item(
        self,
        request: StreamingItemPreparationRequest,
    ) -> ViewerStreamItemPayload:
        if request.streaming_data_type.uses_napari_vector_payload:
            item_data = self._prepare_shapes_data(
                request.data,
                request.item_path.value,
            )
        else:
            item_data = self.create_shared_memory_payload(
                request.data,
                request.item_path.value,
            )
        return ViewerStreamItemPayload(
            item_payload=item_data,
            streaming_data_type=request.streaming_data_type,
        )

    # cleanup() now inherited from ABC

    def __del__(self):
        """Cleanup on deletion."""
        self.cleanup()
