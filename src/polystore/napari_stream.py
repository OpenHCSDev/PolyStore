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
from .streaming_constants import StreamingDataType
from .streaming import StreamingBackend
from .roi_converters import NapariROIConverter
from zmqruntime.transport import get_zmq_transport_url, coerce_transport_mode

logger = logging.getLogger(__name__)


class NapariStreamingBackend(StreamingBackend):
    """Napari streaming backend with automatic registration."""
    _backend_type = Backend.NAPARI_STREAM.value

    # Configure ABC attributes
    VIEWER_TYPE = 'napari'
    SHM_PREFIX = 'napari_'

    # __init__, _get_publisher, save, cleanup now inherited from ABC

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
        if data_type in (StreamingDataType.SHAPES, StreamingDataType.POINTS):
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

        # Extract kwargs using generic polymorphic names
        host = kwargs.get('host', 'localhost')
        port = kwargs['port']
        transport_mode = kwargs['transport_mode']
        transport_config = kwargs.get('transport_config')
        display_config = kwargs['display_config']
        microscope_handler = kwargs['microscope_handler']
        source = kwargs.get('source', 'unknown_source')  # Pre-built source value
        plate_path = kwargs.get('plate_path')
        display_payload_extra = {
            "colormap": display_config.get_colormap_name(),
            "variable_size_handling": display_config.variable_size_handling.value
            if hasattr(display_config, "variable_size_handling") and display_config.variable_size_handling
            else None,
        }

        message, batch_images, image_ids = self._build_batch_message(
            data_list,
            file_paths,
            microscope_handler,
            source,
            display_config,
            self._prepare_batch_item,
            plate_path=plate_path,
            display_payload_extra=display_payload_extra,
        )

        # Register sent images with queue tracker BEFORE sending
        # This prevents race condition with IPC mode where acks arrive before registration
        self._register_with_queue_tracker(
            port,
            image_ids,
            transport_mode=transport_mode,
            transport_config=transport_config,
        )

        # Create FRESH REQ socket for each send - REQ sockets cannot be reused
        # This prevents the "Operation cannot be accomplished in current state" error
        # when multiple streams happen concurrently
        transport_config = transport_config or self._transport_config
        url = get_zmq_transport_url(
            port,
            host=host,
            mode=coerce_transport_mode(transport_mode),
            config=transport_config,
        )

        if self._context is None:
            self._context = zmq.Context()

        socket = self._context.socket(zmq.REQ)
        socket.connect(url)
        time.sleep(0.1)  # Brief delay for connection to establish

        try:
            # Send with REQ socket (BLOCKING - worker waits for Napari to acknowledge)
            # Worker blocks until Napari receives, copies data from shared memory, and sends ack
            # This guarantees no messages are lost and shared memory is only closed after Napari is done
            logger.info(f"📤 NAPARI BACKEND: Sending batch of {len(batch_images)} images to Napari on port {port} (REQ/REP - blocking until ack)")
            socket.send_json(message)  # Blocking send

            # Wait for acknowledgment from Napari (REP socket)
            # Napari will only reply after it has copied all data from shared memory
            ack_response = socket.recv_json()
            logger.info(f"✅ NAPARI BACKEND: Received ack from Napari: {ack_response.get('status', 'unknown')}")

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
