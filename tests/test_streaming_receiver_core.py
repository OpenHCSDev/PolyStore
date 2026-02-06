from __future__ import annotations

import threading
import time

from polystore.streaming_constants import StreamingDataType
from polystore.streaming.receivers.core import (
    DebouncedBatchEngine,
    group_items_by_component_modes,
)
from polystore.streaming.receivers.napari import (
    normalize_component_layout,
    build_layer_key,
)


def test_group_items_by_component_modes_source_normalization_for_rois() -> None:
    items = [
        {
            "data_type": "rois",
            "metadata": {"source": "/tmp/foo_results", "well": "A01", "channel": 1},
        },
        {
            "data_type": "image",
            "metadata": {"source": "step_1", "well": "A01", "channel": 1},
        },
    ]
    component_modes = {"source": "window", "well": "frame", "channel": "channel"}
    component_order = ["source", "well", "channel"]

    grouped = group_items_by_component_modes(
        items,
        component_modes=component_modes,
        component_order=component_order,
        images_dir="/my/plate/images",
    )

    assert grouped.window_components == ["source"]
    assert grouped.channel_components == ["channel"]
    assert grouped.frame_components == ["well"]
    assert "source_images" in grouped.windows
    assert "source_step_1" in grouped.windows


def test_napari_layer_key_builder_uses_slice_components_and_payload_type() -> None:
    component_modes = {"well": "slice", "channel": "stack", "site": "slice"}
    component_order = ["well", "channel", "site"]
    component_info = {"well": "A01", "channel": 2, "site": 3}

    key_image = build_layer_key(
        component_info=component_info,
        component_modes=component_modes,
        component_order=component_order,
        data_type=StreamingDataType.IMAGE,
    )
    key_shapes = build_layer_key(
        component_info=component_info,
        component_modes=component_modes,
        component_order=component_order,
        data_type=StreamingDataType.SHAPES,
    )

    assert key_image == "well_A01_site_3"
    assert key_shapes == "well_A01_site_3_shapes"


def test_normalize_component_layout_dict_config() -> None:
    component_modes, component_order = normalize_component_layout(
        {
            "component_modes": {"well": "slice", "channel": "stack"},
            "component_order": ["well", "channel"],
        }
    )
    assert component_order == ["well", "channel"]
    assert component_modes["well"] == "slice"


def test_debounced_batch_engine_flush_processes_pending_once() -> None:
    processed: list[tuple[list[dict], dict]] = []

    def _process(items, context):
        processed.append((items, context))

    engine = DebouncedBatchEngine(
        process_fn=_process, debounce_delay_ms=10_000, max_debounce_wait_ms=20_000
    )
    engine.enqueue(
        items=[{"id": 1}],
        context={"display_config": {}, "layer_key": "layer_a"},
    )
    engine.flush()

    assert len(processed) == 1
    assert processed[0][0] == [{"id": 1}]
    assert processed[0][1]["layer_key"] == "layer_a"


def test_debounced_batch_engine_enqueue_not_blocked_by_processing() -> None:
    processed: list[tuple[list[dict], dict]] = []
    processing_started = threading.Event()
    release_processing = threading.Event()

    def _process(items, context):
        processed.append((items, context))
        processing_started.set()
        release_processing.wait(timeout=2.0)

    engine = DebouncedBatchEngine(
        process_fn=_process, debounce_delay_ms=10_000, max_debounce_wait_ms=20_000
    )
    engine.enqueue(
        items=[{"id": 1}],
        context={"display_config": {}, "layer_key": "layer_a"},
    )

    flush_thread = threading.Thread(target=engine.flush, daemon=True)
    flush_thread.start()
    assert processing_started.wait(timeout=1.0)

    start = time.perf_counter()
    engine.enqueue(
        items=[{"id": 2}],
        context={"display_config": {}, "layer_key": "layer_b"},
    )
    elapsed = time.perf_counter() - start
    assert elapsed < 0.1

    release_processing.set()
    flush_thread.join(timeout=1.0)
    engine.flush()

    assert len(processed) == 2
    assert processed[0][0] == [{"id": 1}]
    assert processed[1][0] == [{"id": 2}]
