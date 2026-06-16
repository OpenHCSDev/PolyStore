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
import time
from pathlib import Path
from typing import Any, List, Union

import zmq

from .constants import Backend, TransportMode
from .streaming import StreamingBackend
from .roi_converters import NapariROIConverter
from .streaming.viewer_transport import (
    ViewerAckPolicy,
    ViewerStreamKwargs,
    ViewerTransportConfigAuthority,
    ViewerTransportDefaults,
)
from zmqruntime.transport import get_zmq_transport_url, coerce_transport_mode

logger = logging.getLogger(__name__)
NAPARI_TRANSPORT_DEFAULTS = ViewerTransportDefaults()
NAPARI_ACK_POLICY = ViewerAckPolicy(
    viewer_name="Napari",
    timeout_ms=NAPARI_TRANSPORT_DEFAULTS.ack_timeout_ms,
)


class NapariDisplayPayload:
    """Display payload projection for Napari stream messages."""

    @staticmethod
    def variable_size_handling_value(display_config):
        if not hasattr(display_config, "variable_size_handling"):
            return None
        variable_size_handling = display_config.variable_size_handling
        if variable_size_handling is None:
            return None
        return variable_size_handling.value

    @classmethod
    def from_display_config(cls, display_config) -> dict[str, Any]:
        return {
            "colormap": display_config.get_colormap_name(),
            "variable_size_handling": cls.variable_size_handling_value(display_config),
        }


class NapariStreamingBackend(StreamingBackend):
    """Napari streaming backend with automatic registration."""
    _backend_type = Backend.NAPARI_STREAM.value

    VIEWER_TYPE = 'napari'
    SHM_PREFIX = 'napari_'

    def _prepare_shapes_data(self, data: Any, file_path: Union[str, Path]) -> dict:
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
            'path': str(file_path),
            'shapes': shapes_data,
        }

    def _prepare_batch_item(self, data: Any, file_path: Union[str, Path], data_type):
        if data_type.uses_napari_vector_payload:
            item_data = self._prepare_shapes_data(data, file_path)
            data_type_value = data_type.value
        else:
            item_data = self._create_shared_memory(data, file_path)
            data_type_value = data_type.value
        return item_data, data_type_value

    def save_batch(self, data_list: List[Any], file_paths: List[Union[str, Path]], **kwargs) -> None:
        """
        Stream multiple images or ROIs to napari as a batch.

        Args:
            data_list: List of image data or ROI lists
            file_paths: List of path identifiers
            **kwargs: Additional metadata
        """
        # Filter to only supported file types
        data_list, file_paths, skipped = self._filter_streamable_files(data_list, file_paths)
        if not data_list:
            return

        stream_request = ViewerStreamKwargs.from_kwargs(
            kwargs,
            NAPARI_TRANSPORT_DEFAULTS,
        )
        display_payload_extra = NapariDisplayPayload.from_display_config(
            stream_request.display_config
        )

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
            display_payload_extra=display_payload_extra,
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
        NAPARI_ACK_POLICY.apply_socket_options(socket)
        socket.connect(url)
        time.sleep(0.1)  # Brief delay for connection to establish

        try:
            # Send with REQ socket (BLOCKING - worker waits for Napari to acknowledge)
            # Worker blocks until Napari receives, copies data from shared memory, and sends ack
            # This guarantees no messages are lost and shared memory is only closed after Napari is done
            logger.info(f"📤 NAPARI BACKEND: Sending batch of {len(batch_images)} images to Napari on port {stream_request.port} (REQ/REP - blocking until ack)")
            socket.send_json(message)  # Blocking send

            # Wait for acknowledgment from Napari (REP socket)
            # Napari will only reply after it has copied all data from shared memory
            ack_response = NAPARI_ACK_POLICY.receive(
                socket,
                lambda: self._cleanup_shared_memory_blocks(batch_images, unlink=True),
                port=stream_request.port,
            )
            logger.info(f"✅ NAPARI BACKEND: Received ack from Napari: {NAPARI_ACK_POLICY.status(ack_response)}")

        finally:
            # Always close the socket - never reuse REQ sockets
            socket.close()

        # Clean up publisher's handles after successful send
        # Receiver will unlink the shared memory after copying the data
        self._cleanup_shared_memory_blocks(batch_images, unlink=False)

    # cleanup() now inherited from ABC

    def __del__(self):
        """Cleanup on deletion."""
        self.cleanup()
