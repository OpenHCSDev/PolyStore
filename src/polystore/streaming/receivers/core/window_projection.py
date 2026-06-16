"""Generic window projection utilities for receiver-side batch processing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from polystore.streaming.identity import (
    StreamProducerDisplayNameAuthority,
    StreamProducerIdentity,
    StreamRouteKeyAuthority,
)


@dataclass(frozen=True)
class GroupedWindowItems:
    """Projection result for a single batch."""

    window_components: list[str]
    channel_components: list[str]
    slice_components: list[str]
    frame_components: list[str]
    windows: dict[str, list[dict[str, Any]]]
    fixed_window_labels: dict[str, list[tuple[str, Any]]]


def group_items_by_component_modes(
    items: list[dict[str, Any]],
    component_modes: dict[str, str],
    component_order: list[str],
) -> GroupedWindowItems:
    """Project items into window groups using declared component modes."""
    result: dict[str, list[str]] = {
        "window": [],
        "channel": [],
        "slice": [],
        "frame": [],
    }
    for comp_name in component_order:
        mode = component_modes[comp_name]
        result[mode].append(comp_name)

    window_components = result["window"]
    channel_components = result["channel"]
    slice_components = result["slice"]
    frame_components = result["frame"]

    windows: dict[str, list[dict[str, Any]]] = {}
    fixed_window_labels: dict[str, list[tuple[str, Any]]] = {}

    for item in items:
        meta = item.get("metadata", {})
        producer = StreamProducerIdentity.from_payload(item.get("producer_identity"))
        key_parts: list[str] = list(producer.route_parts())
        fixed_labels: list[tuple[str, Any]] = [
            ("producer", StreamProducerDisplayNameAuthority.output_label(producer))
        ]

        for comp in window_components:
            if comp not in meta:
                continue
            value = meta[comp]
            key_parts.append(f"{comp}_{value}")
            fixed_labels.append((comp, value))

        window_key = StreamRouteKeyAuthority.join(key_parts)
        windows.setdefault(window_key, []).append(item)
        if window_key not in fixed_window_labels:
            fixed_window_labels[window_key] = fixed_labels

    return GroupedWindowItems(
        window_components=window_components,
        channel_components=channel_components,
        slice_components=slice_components,
        frame_components=frame_components,
        windows=windows,
        fixed_window_labels=fixed_window_labels,
    )
