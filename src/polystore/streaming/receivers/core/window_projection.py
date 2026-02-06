"""Generic window projection utilities for receiver-side batch processing."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


WindowValueNormalizer = Callable[[str, Any, dict[str, Any], str | None], Any]


@dataclass(frozen=True)
class GroupedWindowItems:
    """Projection result for a single batch."""

    window_components: list[str]
    channel_components: list[str]
    slice_components: list[str]
    frame_components: list[str]
    windows: dict[str, list[dict[str, Any]]]
    fixed_window_labels: dict[str, list[tuple[str, Any]]]


def _default_normalizer(
    component_name: str,
    value: Any,
    item: dict[str, Any],
    images_dir: str | None,
) -> Any:
    """Normalize window component values for stable keying across payload types."""
    data_type = item.get("data_type")
    if component_name == "source" and images_dir and data_type == "rois":
        value_str = str(value)
        if "_results" in value_str or "/" in value_str:
            return Path(images_dir).name
    return value


def group_items_by_component_modes(
    items: list[dict[str, Any]],
    component_modes: dict[str, str],
    component_order: list[str],
    *,
    images_dir: str | None = None,
    normalizer: WindowValueNormalizer | None = None,
) -> GroupedWindowItems:
    """Project items into window groups using declared component modes."""
    if normalizer is None:
        normalizer = _default_normalizer

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
        key_parts: list[str] = []
        fixed_labels: list[tuple[str, Any]] = []

        for comp in window_components:
            if comp not in meta:
                continue
            value = normalizer(comp, meta[comp], item, images_dir)
            key_parts.append(f"{comp}_{value}")
            fixed_labels.append((comp, value))

        window_key = "_".join(key_parts) if key_parts else "default_window"
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

