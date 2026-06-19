"""Canonical napari route-key construction."""

from __future__ import annotations

from collections.abc import Mapping

from polystore.streaming.identity import StreamProducerIdentity, StreamRouteKeyAuthority
from polystore.streaming_constants import StreamingDataType
from zmqruntime.viewer_protocol import (
    ViewerBatchDisplayPayload,
    ViewerComponentMode,
    ViewerDisplayConfigWireField,
    ViewerWireMapping,
    ViewerWireValue,
)


def normalize_component_layout(
    display_config: ViewerBatchDisplayPayload | ViewerWireMapping,
) -> ViewerBatchDisplayPayload:
    """Return canonical display layout from a viewer display-config payload."""
    if isinstance(display_config, ViewerBatchDisplayPayload):
        return display_config
    if isinstance(display_config, dict):
        return ViewerBatchDisplayPayload(
            component_modes=_required_mapping(
                display_config,
                ViewerDisplayConfigWireField.COMPONENT_MODES.value,
            ),
            component_order=_required_sequence(
                display_config,
                ViewerDisplayConfigWireField.COMPONENT_ORDER.value,
            ),
        )

    raise TypeError(
        "Napari component layout requires ViewerBatchDisplayPayload or mapping, "
        f"got {type(display_config).__name__}."
    )


def build_route_key(
    producer_identity: StreamProducerIdentity | Mapping[str, ViewerWireValue],
    component_info: Mapping[str, ViewerWireValue],
    display_layout: ViewerBatchDisplayPayload,
    data_type: StreamingDataType,
) -> str:
    """Build hidden route key from producer identity, slice components, and type."""
    producer = StreamProducerIdentity.from_payload(producer_identity)
    route_parts: list[str] = list(producer.route_parts())
    for component in display_layout.components_for_mode(ViewerComponentMode.SLICE):
        if component not in component_info:
            raise ValueError(
                f"Napari route key missing slice component {component!r}."
            )
        route_parts.append(f"{component}_{component_info[component]}")

    route_key = StreamRouteKeyAuthority.join(route_parts)

    return f"{route_key}{data_type.napari_layer_suffix}"


def _required_mapping(
    payload: Mapping[str, ViewerWireValue],
    field_name: str,
) -> dict[str, str]:
    if field_name not in payload:
        raise ValueError(f"Display config missing required field {field_name!r}.")
    value = payload[field_name]
    if not isinstance(value, Mapping):
        raise TypeError(
            f"Display config field {field_name!r} must be a mapping, "
            f"got {type(value).__name__}."
        )
    return {
        str(component): str(mode)
        for component, mode in value.items()
    }


def _required_sequence(
    payload: Mapping[str, ViewerWireValue],
    field_name: str,
) -> list[str]:
    if field_name not in payload:
        raise ValueError(f"Display config missing required field {field_name!r}.")
    value = payload[field_name]
    if isinstance(value, str) or not isinstance(value, list | tuple):
        raise TypeError(
            f"Display config field {field_name!r} must be a sequence, "
            f"got {type(value).__name__}."
        )
    return [str(component) for component in value]
