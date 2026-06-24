"""
Fiji-specific handler for ROI data.

Handles ImageJ ROI Manager integration with proper component positioning.
"""

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from polystore.streaming.handlers import HandlerBase
from polystore.streaming.base import HandlerContext
from zmqruntime.viewer_protocol import (
    ViewerBatchItemWireField,
    ViewerWireMapping,
    ViewerWireValue,
)

logger = logging.getLogger(__name__)
FijiROIComponentValue = (
    str
    | int
    | float
    | bool
    | None
    | tuple["FijiROIComponentValue", ...]
)
FijiROIComponentKey = tuple[FijiROIComponentValue, ...]


def fiji_roi_component_value(value: ViewerWireValue) -> FijiROIComponentValue:
    """Return a hashable ImageJ-axis component value from viewer wire data."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, tuple):
        return tuple(fiji_roi_component_value(item) for item in value)
    if isinstance(value, list):
        return tuple(fiji_roi_component_value(item) for item in value)
    raise TypeError(
        "Fiji ROI component metadata values must be scalar or tuple-like, "
        f"got {type(value).__name__}."
    )


@dataclass(frozen=True, slots=True)
class FijiROIWireItem:
    """Typed ROI wire item used by the legacy Fiji handler path."""

    payload: ViewerWireMapping

    @classmethod
    def from_payload(cls, payload: Mapping[str, ViewerWireValue]) -> "FijiROIWireItem":
        return cls(payload)

    @property
    def rois(self) -> list[str]:
        field = ViewerBatchItemWireField.ROIS.value
        if field not in self.payload:
            raise ValueError("Fiji ROI item missing required 'rois' field.")
        value = self.payload[field]
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            raise TypeError("Fiji ROI item 'rois' field must be a sequence.")
        return [str(encoded_roi) for encoded_roi in value]

    @property
    def metadata(self) -> ViewerWireMapping:
        field = ViewerBatchItemWireField.METADATA.value
        if field not in self.payload:
            raise ValueError("Fiji ROI item missing required 'metadata' field.")
        value = self.payload[field]
        if not isinstance(value, Mapping):
            raise TypeError("Fiji ROI item 'metadata' field must be a mapping.")
        return value

    @property
    def image_id(self) -> str | None:
        field = ViewerBatchItemWireField.IMAGE_ID.value
        if field not in self.payload or self.payload[field] is None:
            return None
        return str(self.payload[field])

    def component_value_tuple(
        self,
        component_names: Sequence[str],
    ) -> FijiROIComponentKey:
        return tuple(
            fiji_roi_component_value(self.metadata[component_name])
            for component_name in component_names
        )


@dataclass(frozen=True, slots=True)
class FijiROIAxisPosition:
    """Strict one-based ImageJ coordinate resolver for legacy Fiji ROI handling."""

    component_names: Sequence[str]
    values: Sequence[FijiROIComponentKey]

    @classmethod
    def from_items(
        cls,
        items: Sequence[FijiROIWireItem],
        component_names: Sequence[str],
    ) -> "FijiROIAxisPosition":
        values = tuple(
            sorted(
                {
                    item.component_value_tuple(component_names)
                    for item in items
                }
            )
        )
        return cls(component_names, values)

    def one_based_position(self, item: FijiROIWireItem) -> int:
        if not self.component_names:
            return 1
        value_tuple = item.component_value_tuple(self.component_names)
        if value_tuple not in self.values:
            raise ValueError(
                f"Fiji ROI component value {value_tuple!r} is outside axis domain "
                f"{self.values!r}."
            )
        return self.values.index(value_tuple) + 1


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

        roi_items = tuple(
            FijiROIWireItem.from_payload(item)
            for item in context.data.items
        )

        # Process ROIs with component positioning
        channel_comps = context.components.get_by_mode("channel")
        slice_comps = context.components.get_by_mode("slice")
        frame_comps = context.components.get_by_mode("frame")

        channel_position = FijiROIAxisPosition.from_items(roi_items, channel_comps)
        slice_position = FijiROIAxisPosition.from_items(roi_items, slice_comps)
        frame_position = FijiROIAxisPosition.from_items(roi_items, frame_comps)

        total_rois_added = 0

        from polystore.roi_converters import FijiROIConverter

        for roi_item in roi_items:
            rois_encoded = roi_item.rois
            if not rois_encoded:
                if image_id := roi_item.image_id:
                    context.server._send_ack(image_id, status="success")
                continue

            logger.info(f"🔬 FIJI ROI HANDLER: Processing {len(rois_encoded)} ROIs")

            # Convert ROIs to ImageJ format
            java_rois = FijiROIConverter.transmission_to_java_rois(
                rois_encoded,
                sj,
            )
            imagej_position = (
                channel_position.one_based_position(roi_item),
                slice_position.one_based_position(roi_item),
                frame_position.one_based_position(roi_item),
            )

            # Add ROIs to manager with group ID
            for roi_obj in java_rois:
                roi_obj.setPosition(*imagej_position)
                roi_obj.setGroup(group_id)
                rm.addRoi(roi_obj)

            total_rois_added += len(java_rois)

            if image_id := roi_item.image_id:
                context.server._send_ack(image_id, status="success")

        logger.info(
            f"🔬 FIJI ROI HANDLER: Added {total_rois_added} ROIs to window '{context.window_key}'"
        )
