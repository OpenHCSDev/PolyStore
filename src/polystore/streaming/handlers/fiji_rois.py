"""
Fiji-specific handler for ROI data.

Handles ImageJ ROI Manager integration with proper component positioning.
"""

import logging
from polystore.streaming.handlers import HandlerBase
from polystore.streaming.base import HandlerContext

logger = logging.getLogger(__name__)


class FijiROIHandler(HandlerBase):
    """Handler for ImageJ ROI Manager display."""

    _handler_data_type = "rois"

    @staticmethod
    def can_handle(data_type: str) -> bool:
        """Check if this handler can process the given data type."""
        return data_type == "rois"

    @staticmethod
    def handle(context: HandlerContext) -> None:
        """Add ROIs to ImageJ ROI Manager."""
        # Access data via typed wrapper
        roi_data = context.data

        # Get or create RoiManager on EDT
        import scyjava as sj
        RoiManager = sj.jimport("ij.plugin.frame.RoiManager")
        rm = RoiManager.getInstance()

        if rm is None:
            from jpype import JImplements, JOverride

            @JImplements("java.lang.Runnable")
            class CreateRoiManagerRunnable:
                @JOverride
                def run(self):
                    holder[0] = RoiManager()

            from javax.swing import SwingUtilities
            holder = [None]
            SwingUtilities.invokeAndWait(CreateRoiManagerRunnable())
            rm = holder[0]

        # Get or assign integer group ID for this window
        group_id = context.server._get_or_create_group_id(context.window_key)

        # Process ROIs with component positioning
        channel_comps = context.components.get_by_mode("channel")
        slice_comps = context.components.get_by_mode("slice")
        frame_comps = context.components.get_by_mode("frame")

        channel_values = context.components.collect_values(channel_comps)
        slice_values = context.components.collect_values(slice_comps)
        frame_values = context.components.collect_values(frame_comps)

        total_rois_added = 0

        for roi_item in roi_data.items:
            rois_encoded = roi_item.get("rois", [])
            if not rois_encoded:
                if image_id := roi_item.get("image_id"):
                    context.server._send_ack(image_id, status="success")
                continue

            meta = roi_item.get("metadata", {})
            file_path = roi_item.get("path", "unknown")

            logger.info(f"🔬 FIJI ROI HANDLER: Processing {len(rois_encoded)} ROIs")

            # Convert ROIs to ImageJ format
            from polystore.roi_converters import FijiROIConverter
            rois_list = FijiROIConverter.to_fiji_rois(
                rois_encoded,
                channel_values=channel_values,
                slice_values=slice_values,
                frame_values=frame_values,
                channel_components=channel_comps,
                slice_components=slice_comps,
                frame_components=frame_comps,
            )

            # Add ROIs to manager with group ID
            for roi_obj in rois_list:
                roi_obj.setProperty("group", group_id)
                rm.addRoi(roi_obj)

            total_rois_added += len(rois_list)

        logger.info(
            f"🔬 FIJI ROI HANDLER: Added {total_rois_added} ROIs to window '{context.window_key}'"
        )
