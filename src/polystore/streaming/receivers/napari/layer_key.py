"""Canonical napari route-key construction."""

from __future__ import annotations

from typing import Any

from polystore.streaming.identity import StreamProducerIdentity, StreamRouteKeyAuthority
from polystore.streaming_constants import StreamingDataType


def normalize_component_layout(display_config: Any) -> tuple[dict[str, str], list[str]]:
    """Return canonical (component_modes, component_order) from display config."""
    if isinstance(display_config, dict):
        component_modes = display_config["component_modes"]
        component_order = display_config["component_order"]
        return component_modes, component_order

    return display_config.component_modes(), list(display_config.COMPONENT_ORDER)


def build_route_key(
    producer_identity: StreamProducerIdentity | dict[str, Any],
    component_info: dict[str, Any],
    component_modes: dict[str, str],
    component_order: list[str],
    data_type: StreamingDataType,
) -> str:
    """Build hidden route key from producer identity, slice components, and type."""
    producer = StreamProducerIdentity.from_payload(producer_identity)
    route_parts: list[str] = list(producer.route_parts())
    for component in component_order:
        mode = component_modes[component]
        if mode == "slice" and component in component_info:
            route_parts.append(f"{component}_{component_info[component]}")

    route_key = StreamRouteKeyAuthority.join(route_parts)

    return f"{route_key}{data_type.napari_layer_suffix}"
