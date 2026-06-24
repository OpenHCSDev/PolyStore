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

from .constants import Backend
from .streaming import (
    FilePath,
    RoiStreamPayload,
    StreamingBackend,
    StreamingItemPreparationRequest,
    ViewerDisplayPayloadExtra,
)
from .streaming.viewer_transport import ViewerStreamItemPayload, ViewerStreamRequest
from .roi_converters import NapariROIConverter
from zmqruntime.viewer_protocol import (
    ViewerBatchItemWireField,
    ViewerWireMapping,
    ViewerWireValue,
)

logger = logging.getLogger(__name__)


class NapariDisplayWireField(str, Enum):
    """Napari-specific display fields inside the shared viewer display payload."""

    COLORMAP = "colormap"
    VARIABLE_SIZE_HANDLING = "variable_size_handling"


class NapariDisplayPayload:
    """Display payload projection for Napari stream messages."""

    @staticmethod
    def variable_size_handling_value(display_config):
        variable_size_handling = display_config.variable_size_handling
        if variable_size_handling is None:
            return None
        return variable_size_handling.value

    @classmethod
    def from_display_config(cls, display_config) -> dict[str, ViewerWireValue]:
        return {
            NapariDisplayWireField.COLORMAP.value: display_config.get_colormap_name(),
            NapariDisplayWireField.VARIABLE_SIZE_HANDLING.value: (
                cls.variable_size_handling_value(display_config)
            ),
        }


class NapariStreamingBackend(StreamingBackend):
    """Napari streaming backend with automatic registration."""
    _backend_type = Backend.NAPARI_STREAM.value

    VIEWER_TYPE = 'napari'
    SHM_PREFIX = 'napari_'

    def display_payload_extra(
        self,
        stream_request: ViewerStreamRequest,
    ) -> ViewerDisplayPayloadExtra:
        return ViewerDisplayPayloadExtra.from_mapping(
            NapariDisplayPayload.from_display_config(stream_request.display_config)
        )

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
