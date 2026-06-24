"""
Fiji streaming backend for polystore.

Streams image data to Fiji/ImageJ viewer using ZMQ for IPC.
Follows same architecture as Napari streaming for consistency.

SHARED MEMORY OWNERSHIP MODEL:
- Sender (Worker): Creates shared memory, sends reference via ZMQ, closes handle (does NOT unlink)
- Receiver (Fiji Server): Attaches to shared memory, copies data, closes handle, unlinks
- Only receiver calls unlink() to prevent FileNotFoundError
- REQ/REP socket pattern ensures receiver copies data before sender closes handle
"""

import logging
from enum import Enum

from .constants import Backend
from .streaming_constants import StreamingDataType
from .streaming import (
    FilePath,
    RoiStreamPayload,
    StreamingBuiltBatch,
    StreamingBackend,
    StreamingComponentNamesRequest,
    StreamingItemPreparationRequest,
    ViewerDisplayPayloadExtra,
)
from .streaming.viewer_transport import ViewerStreamItemPayload, ViewerStreamRequest
from .roi_converters import FijiROIConverter
from zmqruntime.viewer_protocol import (
    ViewerBatchItemWireField,
    ViewerBatchWireField,
    ViewerWireMapping,
    ViewerWireValue,
)

logger = logging.getLogger(__name__)


class FijiDisplayWireField(str, Enum):
    """Fiji-specific display fields inside the shared viewer display payload."""

    LUT = "lut"
    AUTO_CONTRAST = "auto_contrast"


class FijiDisplayPayload:
    """Display payload projection for Fiji stream messages."""

    @staticmethod
    def auto_contrast_value(display_config) -> bool:
        return display_config.auto_contrast

    @classmethod
    def from_display_config(cls, display_config) -> dict[str, ViewerWireValue]:
        return {
            FijiDisplayWireField.LUT.value: display_config.get_lut_name(),
            FijiDisplayWireField.AUTO_CONTRAST.value: cls.auto_contrast_value(
                display_config
            ),
        }


class FijiMessageMetadata:
    """Typed access to optional Fiji message metadata."""

    @staticmethod
    def component_names_metadata(message: ViewerWireMapping) -> ViewerWireValue:
        return message[ViewerBatchWireField.COMPONENT_NAMES_METADATA.value]


class FijiRoiPayload:
    """ROI payload inspection for Fiji logging."""

    @staticmethod
    def count(item_data: ViewerWireMapping) -> int:
        if ViewerBatchItemWireField.ROIS.value not in item_data:
            raise ValueError("Fiji ROI payload missing required 'rois' field")
        return len(item_data[ViewerBatchItemWireField.ROIS.value])


class FijiStreamingBackend(StreamingBackend):
    """Fiji streaming backend with ZMQ publisher pattern (matches Napari architecture)."""
    _backend_type = Backend.FIJI_STREAM.value

    VIEWER_TYPE = 'fiji'
    SHM_PREFIX = 'fiji_'

    def display_payload_extra(
        self,
        stream_request: ViewerStreamRequest,
    ) -> ViewerDisplayPayloadExtra:
        return ViewerDisplayPayloadExtra.from_mapping(
            FijiDisplayPayload.from_display_config(stream_request.display_config)
        )

    def message_extra(
        self,
        stream_request: ViewerStreamRequest,
    ) -> dict[str, ViewerWireValue]:
        return stream_request.message_extra_payload_with_images_dir()

    def component_names_request(
        self,
        stream_request: ViewerStreamRequest,
    ) -> StreamingComponentNamesRequest:
        return StreamingComponentNamesRequest.from_stream_request(
            stream_request,
            log_prefix="🏷️  FIJI BACKEND",
            verbose=True,
        )

    def after_batch_message_built(
        self,
        stream_request: ViewerStreamRequest,
        built_batch: StreamingBuiltBatch,
    ) -> None:
        logger.info(
            "🏷️  FIJI BACKEND: Final component_names_metadata: %s",
            FijiMessageMetadata.component_names_metadata(built_batch.message),
        )

        for item in built_batch.batch_images:
            logger.info(
                "🔍 FIJI BACKEND: Added %s item to batch",
                item[ViewerBatchItemWireField.DATA_TYPE.value],
            )

        data_types = [
            item[ViewerBatchItemWireField.DATA_TYPE.value]
            for item in built_batch.batch_images
        ]
        type_counts = {
            data_type: data_types.count(data_type)
            for data_type in set(data_types)
        }
        logger.info(
            "📤 FIJI BACKEND: Sending batch message with %d items to port %s: %s",
            len(built_batch.batch_images),
            stream_request.port,
            type_counts,
        )

    def _prepare_rois_data(
        self,
        data: RoiStreamPayload,
        file_path: FilePath,
    ) -> dict[str, ViewerWireValue]:
        """
        Prepare ROIs data for transmission.

        Args:
            data: ROI list
            file_path: Path identifier

        Returns:
            Dict with ROI data
        """
        # Convert ROI objects to bytes, then base64 encode for transmission
        roi_bytes_list = FijiROIConverter.rois_to_imagej_bytes(data)
        rois_encoded = FijiROIConverter.encode_rois_for_transmission(roi_bytes_list)

        return {
            ViewerBatchItemWireField.PATH.value: str(file_path),
            ViewerBatchItemWireField.ROIS.value: rois_encoded,
        }

    def _prepare_batch_item(
        self,
        request: StreamingItemPreparationRequest,
    ) -> ViewerStreamItemPayload:
        logger.info(
            "🔍 FIJI BACKEND: Detected data type: %s for path: %s",
            request.streaming_data_type,
            request.item_path.value,
        )
        if request.streaming_data_type == StreamingDataType.SHAPES:
            logger.info(
                "🔍 FIJI BACKEND: Preparing ROI data for %s",
                request.item_path.value,
            )
            item_data = self._prepare_rois_data(
                request.data,
                request.item_path.value,
            )
            output_streaming_data_type = StreamingDataType.ROIS
            logger.info(
                "🔍 FIJI BACKEND: ROI data prepared: %d ROIs",
                FijiRoiPayload.count(item_data),
            )
        else:
            logger.info(
                "🔍 FIJI BACKEND: Preparing image data for %s",
                request.item_path.value,
            )
            item_data = self.create_shared_memory_payload(
                request.data,
                request.item_path.value,
            )
            output_streaming_data_type = StreamingDataType.IMAGE
        return ViewerStreamItemPayload(
            item_payload=item_data,
            streaming_data_type=output_streaming_data_type,
        )

    # cleanup() now inherited from ABC

    def __del__(self):
        """Cleanup on deletion."""
        logger.info("🔥 FIJI __del__ called, about to call cleanup()")
        self.cleanup()
        logger.info("🔥 FIJI __del__ cleanup() returned")
