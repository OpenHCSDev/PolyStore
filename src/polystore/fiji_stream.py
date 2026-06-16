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
import time
from pathlib import Path
from typing import Any, List, Union

import zmq

from .constants import Backend, TransportMode
from .streaming_constants import StreamingDataType
from .streaming import StreamingBackend
from .roi_converters import FijiROIConverter
from .streaming.viewer_transport import (
    ViewerAckPolicy,
    ViewerStreamKwargs,
    ViewerTransportConfigAuthority,
    ViewerTransportDefaults,
)
from zmqruntime.transport import get_zmq_transport_url, coerce_transport_mode

logger = logging.getLogger(__name__)
FIJI_TRANSPORT_DEFAULTS = ViewerTransportDefaults()
FIJI_ACK_POLICY = ViewerAckPolicy(
    viewer_name="Fiji",
    timeout_ms=FIJI_TRANSPORT_DEFAULTS.ack_timeout_ms,
)


class FijiDisplayPayload:
    """Display payload projection for Fiji stream messages."""

    @staticmethod
    def auto_contrast_value(display_config) -> bool:
        if not hasattr(display_config, "auto_contrast"):
            return True
        return display_config.auto_contrast

    @classmethod
    def from_display_config(cls, display_config) -> dict[str, Any]:
        return {
            "lut": display_config.get_lut_name(),
            "auto_contrast": cls.auto_contrast_value(display_config),
        }


class FijiMessageMetadata:
    """Typed access to optional Fiji message metadata."""

    @staticmethod
    def component_names_metadata(message: dict) -> dict:
        if "component_names_metadata" in message:
            return message["component_names_metadata"]
        return {}


class FijiRoiPayload:
    """ROI payload inspection for Fiji logging."""

    @staticmethod
    def count(item_data: dict) -> int:
        if "rois" not in item_data:
            raise ValueError("Fiji ROI payload missing required 'rois' field")
        return len(item_data["rois"])


class FijiStreamingBackend(StreamingBackend):
    """Fiji streaming backend with ZMQ publisher pattern (matches Napari architecture)."""
    _backend_type = Backend.FIJI_STREAM.value

    VIEWER_TYPE = 'fiji'
    SHM_PREFIX = 'fiji_'

    def _prepare_rois_data(self, data: Any, file_path: Union[str, Path]) -> dict:
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
            'path': str(file_path),
            'rois': rois_encoded,
        }

    def _prepare_batch_item(self, data: Any, file_path: Union[str, Path], data_type):
        logger.info(f"🔍 FIJI BACKEND: Detected data type: {data_type} for path: {file_path}")
        if data_type == StreamingDataType.SHAPES:
            logger.info(f"🔍 FIJI BACKEND: Preparing ROI data for {file_path}")
            item_data = self._prepare_rois_data(data, file_path)
            data_type_value = "rois"
            logger.info(
                f"🔍 FIJI BACKEND: ROI data prepared: {FijiRoiPayload.count(item_data)} ROIs"
            )
        else:
            logger.info(f"🔍 FIJI BACKEND: Preparing image data for {file_path}")
            item_data = self._create_shared_memory(data, file_path)
            data_type_value = "image"
        return item_data, data_type_value

    def save_batch(self, data_list: List[Any], file_paths: List[Union[str, Path]], **kwargs) -> None:
        """Stream batch of images or ROIs to Fiji via ZMQ."""

        logger.info(f"📦 FIJI BACKEND: save_batch called with {len(data_list)} items")

        # Filter to only supported file types
        data_list, file_paths, skipped = self._filter_streamable_files(data_list, file_paths)
        if not data_list:
            return

        stream_request = ViewerStreamKwargs.from_kwargs(
            kwargs,
            FIJI_TRANSPORT_DEFAULTS,
            include_images_dir=True,
        )
        logger.info(f"🏷️  FIJI BACKEND: plate_path = {stream_request.plate_path}")
        logger.info(f"🏷️  FIJI BACKEND: microscope_handler = {stream_request.microscope_handler}")
        display_payload_extra = FijiDisplayPayload.from_display_config(
            stream_request.display_config
        )
        message_extra = {
            "images_dir": stream_request.images_dir,
        }

        message, batch_images, image_ids = self._build_batch_message(
            data_list,
            file_paths,
            stream_request.microscope_handler,
            stream_request.producer_identity,
            stream_request.display_config,
            self._prepare_batch_item,
            plate_path=stream_request.plate_path,
            component_metadata=stream_request.component_metadata,
            component_metadata_by_path=stream_request.component_metadata_by_path,
            component_names_kwargs={"log_prefix": "🏷️  FIJI BACKEND", "verbose": True},
            display_payload_extra=display_payload_extra,
            message_extra=message_extra,
        )

        logger.info(
            "🏷️  FIJI BACKEND: Final component_names_metadata: %s",
            FijiMessageMetadata.component_names_metadata(message),
        )

        for item in batch_images:
            logger.info(f"🔍 FIJI BACKEND: Added {item['data_type']} item to batch")

        # Log batch composition
        data_types = [item['data_type'] for item in batch_images]
        type_counts = {dt: data_types.count(dt) for dt in set(data_types)}
        logger.info(
            "📤 FIJI BACKEND: Sending batch message with %d items to port %s: %s",
            len(batch_images),
            stream_request.port,
            type_counts,
        )

        # Register sent images with queue tracker BEFORE sending
        # This prevents race condition with IPC mode where acks arrive before registration
        self._register_with_queue_tracker(
            stream_request.port,
            image_ids,
            transport_mode=stream_request.transport_mode,
            transport_config=stream_request.transport_config,
        )

        # Create FRESH REQ socket for each send - REQ sockets cannot be reused
        # This prevents the "Operation cannot be accomplished in current state" error
        # when multiple streams happen concurrently
        transport_config = ViewerTransportConfigAuthority.resolve(
            stream_request.transport_config,
            self._transport_config,
        )
        url = get_zmq_transport_url(
            stream_request.port,
            host=stream_request.host,
            mode=coerce_transport_mode(stream_request.transport_mode),
            config=transport_config,
        )

        if self._context is None:
            self._context = zmq.Context()

        socket = self._context.socket(zmq.REQ)
        FIJI_ACK_POLICY.apply_socket_options(socket)
        socket.connect(url)
        time.sleep(0.1)  # Brief delay for connection to establish

        try:
            # Send with REQ socket (BLOCKING - worker waits for Fiji to acknowledge)
            # Worker blocks until Fiji receives, copies data from shared memory, and sends ack
            # This guarantees no messages are lost and shared memory is only closed after Fiji is done
            logger.info(f"📤 FIJI BACKEND: Sending batch of {len(batch_images)} images to Fiji on port {stream_request.port} (REQ/REP - blocking until ack)")
            socket.send_json(message)  # Blocking send

            # Wait for acknowledgment from Fiji (REP socket)
            # Fiji will only reply after it has copied all data from shared memory
            ack_response = FIJI_ACK_POLICY.receive(
                socket,
                lambda: self._cleanup_shared_memory_blocks(batch_images, unlink=True),
                port=stream_request.port,
            )
            logger.info(f"✅ FIJI BACKEND: Received ack from Fiji: {FIJI_ACK_POLICY.status(ack_response)}")

        finally:
            # Always close the socket - never reuse REQ sockets
            socket.close()

        # Clean up publisher's handles after successful send
        # Receiver will unlink the shared memory after copying the data
        self._cleanup_shared_memory_blocks(batch_images, unlink=False)

    # cleanup() now inherited from ABC

    def __del__(self):
        """Cleanup on deletion."""
        logger.info("🔥 FIJI __del__ called, about to call cleanup()")
        self.cleanup()
        logger.info("🔥 FIJI __del__ cleanup() returned")
