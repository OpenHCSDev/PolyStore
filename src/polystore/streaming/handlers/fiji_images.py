"""
Fiji-specific handler for image data.

Handles ImageJ hyperstack building from accumulated image batches.
"""

import logging
from polystore.streaming.handlers import HandlerBase
from polystore.streaming.base import HandlerContext

logger = logging.getLogger(__name__)


class ImageData:
    """Concrete type for image items with validation."""

    def __init__(self, items: list):
        """Validate all items have required fields."""
        for item in items:
            assert 'data' in item, "Image items must have 'data' field"
            assert 'metadata' in item, "Image items must have 'metadata' field"
        self._items = items

    @property
    def data(self) -> list:
        """Get the validated image items."""
        return self._items


class FijiImageHandler(HandlerBase):
    """Handler for ImageJ hyperstack display."""

    _handler_data_type = "image"

    @staticmethod
    def can_handle(data_type: str) -> bool:
        """Check if this handler can process the given data type."""
        return data_type == "image"

    @staticmethod
    def handle(context: HandlerContext) -> None:
        """Build hyperstack from accumulated images."""
        # Access data via typed wrapper
        images = context.data

        # Access components generically (no hardcoded 3 dimensions!)
        channel_comps = context.components.get_by_mode("channel")
        slice_comps = context.components.get_by_mode("slice")
        frame_comps = context.components.get_by_mode("frame")

        # Collect values for each component group
        channel_values = context.components.collect_values(channel_comps)
        slice_values = context.components.collect_values(slice_comps)
        frame_values = context.components.collect_values(frame_comps)

        logger.info(
            f"🔬 FIJI IMAGE HANDLER: Processing {len(images.data)} images: "
            f"{len(channel_values)}C x {len(slice_values)}Z x {len(frame_values)}T"
        )

        # Build hyperstack using server's method
        # Note: We don't call _build_single_hyperstack directly - we need to adapt
        # the existing server code to work with our new architecture.
        # For now, this is a placeholder that logs the handler was called.
        # In a full implementation, this would call the actual hyperstack building
        # logic from FijiViewerServer.

        # The actual implementation would be:
        # context.server._build_single_hyperstack(
        #     window_key=context.window_key,
        #     images=images.data,
        #     display_config_dict=context.display_config,
        #     channel_components=channel_comps,
        #     slice_components=slice_comps,
        #     frame_components=frame_comps,
        #     channel_values=channel_values,
        #     slice_values=slice_values,
        #     frame_values=frame_values,
        #     component_names_metadata=context.display_config.get('component_names_metadata', {}),
        # )
