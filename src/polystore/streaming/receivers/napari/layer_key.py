"""Canonical napari layer-key construction from component metadata."""

from __future__ import annotations

from typing import Any

from polystore.streaming_constants import StreamingDataType


def normalize_component_layout(display_config: Any) -> tuple[dict[str, str], list[str]]:
    """Return canonical (component_modes, component_order) from display config."""
    if isinstance(display_config, dict):
        component_modes = display_config["component_modes"]
        component_order = display_config["component_order"]
        return component_modes, component_order

    component_order = list(display_config.COMPONENT_ORDER)
    component_modes: dict[str, str] = {}
    for component in component_order:
        mode_field = f"{component}_mode"
        mode_value = display_config.__getattribute__(mode_field)
        component_modes[component] = mode_value.value
    return component_modes, component_order


def build_layer_key(
    component_info: dict[str, Any],
    component_modes: dict[str, str],
    component_order: list[str],
    data_type: StreamingDataType,
) -> str:
    """Build canonical layer key from slice-mode components and payload type."""
    layer_key_parts: list[str] = []
    for component in component_order:
        mode = component_modes[component]
        if mode == "slice" and component in component_info:
            layer_key_parts.append(f"{component}_{component_info[component]}")

    layer_key = "_".join(layer_key_parts) if layer_key_parts else "default_layer"

    if data_type == StreamingDataType.SHAPES:
        return f"{layer_key}_shapes"
    if data_type == StreamingDataType.POINTS:
        return f"{layer_key}_points"
    return layer_key

