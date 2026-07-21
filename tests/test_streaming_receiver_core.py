from __future__ import annotations

import threading
import time

from polystore.streaming_constants import StreamingDataType
from polystore.streaming.identity import (
    FixedStreamProducerIdentityKind,
    StreamProducerDisplayNameAuthority,
    StreamProducerIdentity,
)
from polystore.streaming.receivers.core import (
    DebouncedBatchEngine,
    WindowProjectionSource,
    group_items_by_component_modes,
)
from polystore.streaming.receivers.napari import (
    normalize_component_layout,
    build_route_key,
)
from zmqruntime.viewer_protocol import ViewerBatchDisplayPayload

class PipelineProducerFixture:
    """Nominal producer fixtures for receiver-core tests."""

    MAIN_KIND = "main"
    MAIN_KEY = "main"
    ARTIFACT_KIND = "artifact"

    @classmethod
    def main_output(
        cls,
        *,
        step_name: str,
        pipeline_position: int,
        output_key: str = MAIN_KEY,
    ) -> StreamProducerIdentity:
        return StreamProducerIdentity.pipeline_output(
            output_kind=cls.MAIN_KIND,
            output_key=output_key,
            projection_key=cls.MAIN_KIND,
            step_name=step_name,
            pipeline_position=pipeline_position,
        )

    @classmethod
    def artifact_output(
        cls,
        *,
        output_key: str,
        step_name: str,
        pipeline_position: int,
        artifact_kind: str | None = None,
    ) -> StreamProducerIdentity:
        return StreamProducerIdentity.pipeline_output(
            output_kind=cls.ARTIFACT_KIND,
            output_key=output_key,
            projection_key=output_key,
            step_name=step_name,
            pipeline_position=pipeline_position,
            artifact_kind=artifact_kind,
        )


def test_group_items_by_component_modes_keys_windows_by_producer_identity() -> None:
    image_identity = PipelineProducerFixture.main_output(
        step_name="RawLoad",
        pipeline_position=1,
    )
    roi_identity = PipelineProducerFixture.artifact_output(
        output_key="Nuclei",
        step_name="Segment",
        pipeline_position=2,
        artifact_kind="object_labels",
    )
    items = [
        {
            "data_type": "rois",
            "metadata": {"well": "A01", "channel": 1},
            "producer_identity": roi_identity.to_payload(),
        },
        {
            "data_type": "image",
            "metadata": {"well": "A01", "channel": 1},
            "producer_identity": image_identity.to_payload(),
        },
    ]
    component_modes = {"well": "frame", "channel": "channel"}
    component_order = ["well", "channel"]

    grouped = group_items_by_component_modes(
        WindowProjectionSource.from_wire_payloads(items),
        display_layout=ViewerBatchDisplayPayload(
            component_modes=component_modes,
            component_order=component_order,
        ),
    )

    assert grouped.window_components == []
    assert grouped.channel_components == ["channel"]
    assert grouped.frame_components == ["well"]
    assert grouped.slice_components == []
    assert grouped.fixed_window_labels[
        "origin_pipeline_kind_artifact_projection_Nuclei_step_2_name_Segment"
    ] == (("producer", "3. Segment Nuclei"),)
    assert set(grouped.windows) == {
        "origin_pipeline_kind_artifact_projection_Nuclei_step_2_name_Segment",
        "origin_pipeline_kind_main_projection_main_step_1_name_RawLoad",
    }


def test_named_main_outputs_share_projection_and_keep_exact_provenance() -> None:
    first = PipelineProducerFixture.main_output(
        output_key="Stain1",
        step_name="Align",
        pipeline_position=2,
    )
    second = PipelineProducerFixture.main_output(
        output_key="Stain2",
        step_name="Align",
        pipeline_position=2,
    )

    grouped = group_items_by_component_modes(
        WindowProjectionSource.from_wire_payloads(
            [
                {
                    "data_type": "image",
                    "metadata": {"channel": 1},
                    "producer_identity": first.to_payload(),
                },
                {
                    "data_type": "image",
                    "metadata": {"channel": 2},
                    "producer_identity": second.to_payload(),
                },
            ]
        ),
        display_layout=ViewerBatchDisplayPayload(
            component_modes={"channel": "channel"},
            component_order=("channel",),
        ),
    )

    assert tuple(grouped.windows) == (
        "origin_pipeline_kind_main_projection_main_step_2_name_Align",
    )
    sources = tuple(
        WindowProjectionSource.from_wire_payload(payload)
        for payload in next(iter(grouped.windows.values()))
    )
    assert tuple(source.producer.output_key for source in sources) == (
        "Stain1",
        "Stain2",
    )
    assert build_route_key(
        producer_identity=first,
        component_info={"channel": 1},
        display_layout=ViewerBatchDisplayPayload(
            component_modes={"channel": "stack"},
            component_order=("channel",),
        ),
        data_type=StreamingDataType.IMAGE,
    ) == build_route_key(
        producer_identity=second,
        component_info={"channel": 2},
        display_layout=ViewerBatchDisplayPayload(
            component_modes={"channel": "stack"},
            component_order=("channel",),
        ),
        data_type=StreamingDataType.IMAGE,
    )
    slice_layout = ViewerBatchDisplayPayload(
        component_modes={"channel": "slice"},
        component_order=("channel",),
    )
    assert build_route_key(
        producer_identity=first,
        component_info={"channel": 1},
        display_layout=slice_layout,
        data_type=StreamingDataType.IMAGE,
    ) != build_route_key(
        producer_identity=second,
        component_info={"channel": 2},
        display_layout=slice_layout,
        data_type=StreamingDataType.IMAGE,
    )


def test_named_main_outputs_cannot_claim_the_same_projection_slot() -> None:
    first = PipelineProducerFixture.main_output(
        output_key="Stain1",
        step_name="Align",
        pipeline_position=2,
    )
    second = PipelineProducerFixture.main_output(
        output_key="Stain2",
        step_name="Align",
        pipeline_position=2,
    )

    try:
        group_items_by_component_modes(
            WindowProjectionSource.from_wire_payloads(
                [
                    {
                        "data_type": "image",
                        "metadata": {"channel": 1},
                        "producer_identity": first.to_payload(),
                    },
                    {
                        "data_type": "image",
                        "metadata": {"channel": 1},
                        "producer_identity": second.to_payload(),
                    },
                ]
            ),
            display_layout=ViewerBatchDisplayPayload(
                component_modes={"channel": "channel"},
                component_order=("channel",),
            ),
        )
    except ValueError as error:
        assert "same component coordinate" in str(error)
    else:
        raise AssertionError("distinct producer slot collision must fail loudly")


def test_group_items_by_component_modes_rejects_missing_metadata() -> None:
    producer = PipelineProducerFixture.main_output(
        step_name="RawLoad",
        pipeline_position=1,
    )

    try:
        group_items_by_component_modes(
            WindowProjectionSource.from_wire_payloads(
                [{"producer_identity": producer.to_payload()}]
            ),
            display_layout=ViewerBatchDisplayPayload(
                component_modes={"well": "window"},
                component_order=["well"],
            ),
        )
    except ValueError as error:
        assert "metadata" in str(error)
    else:
        raise AssertionError("missing metadata must fail loudly")


def test_stream_producer_display_name_authority_matches_pipeline_editor_indexing() -> None:
    main_output = PipelineProducerFixture.main_output(
        step_name="ConvertObjectsToImage",
        pipeline_position=8,
    )
    artifact_output = PipelineProducerFixture.artifact_output(
        output_key="NucleiObjects3D",
        step_name="ConvertObjectsToImage",
        pipeline_position=8,
        artifact_kind="object_labels",
    )
    manual_output = StreamProducerIdentity.fixed_output(
        FixedStreamProducerIdentityKind.MANUAL,
        "selected_rois",
    )

    assert (
        StreamProducerDisplayNameAuthority.producer_label(main_output)
        == "9. ConvertObjectsToImage"
    )
    assert (
        StreamProducerDisplayNameAuthority.output_label(main_output)
        == "9. ConvertObjectsToImage"
    )
    assert (
        StreamProducerDisplayNameAuthority.output_label(artifact_output)
        == "9. ConvertObjectsToImage NucleiObjects3D"
    )
    assert StreamProducerDisplayNameAuthority.output_label(manual_output) == "selected_rois"
    assert (
        StreamProducerDisplayNameAuthority.disambiguation_label(main_output)
        == "step 9"
    )


def test_napari_route_key_builder_uses_producer_slice_components_and_payload_type() -> None:
    producer = PipelineProducerFixture.artifact_output(
        output_key="Nuclei",
        step_name="Segment",
        pipeline_position=2,
    )
    component_modes = {"well": "slice", "channel": "stack", "site": "slice"}
    component_order = ["well", "channel", "site"]
    component_info = {"well": "A01", "channel": 2, "site": 3}

    key_image = build_route_key(
        producer_identity=producer,
        component_info=component_info,
        display_layout=ViewerBatchDisplayPayload(
            component_modes=component_modes,
            component_order=component_order,
        ),
        data_type=StreamingDataType.IMAGE,
    )
    key_shapes = build_route_key(
        producer_identity=producer,
        component_info=component_info,
        display_layout=ViewerBatchDisplayPayload(
            component_modes=component_modes,
            component_order=component_order,
        ),
        data_type=StreamingDataType.SHAPES,
    )

    assert key_image == "origin_pipeline_kind_artifact_projection_Nuclei_step_2_name_Segment_well_A01_site_3"
    assert key_shapes == "origin_pipeline_kind_artifact_projection_Nuclei_step_2_name_Segment_well_A01_site_3_shapes"


def test_napari_route_key_builder_rejects_missing_slice_component() -> None:
    producer = PipelineProducerFixture.artifact_output(
        output_key="Nuclei",
        step_name="Segment",
        pipeline_position=2,
    )

    try:
        build_route_key(
            producer_identity=producer,
            component_info={"well": "A01"},
            display_layout=ViewerBatchDisplayPayload(
                component_modes={"well": "slice", "site": "slice"},
                component_order=["well", "site"],
            ),
            data_type=StreamingDataType.IMAGE,
        )
    except ValueError as error:
        assert "site" in str(error)
    else:
        raise AssertionError("missing slice component must fail loudly")


def test_normalize_component_layout_dict_config() -> None:
    display_layout = normalize_component_layout(
        {
            "component_modes": {"well": "slice", "channel": "stack"},
            "component_order": ["well", "channel"],
        }
    )
    assert list(display_layout.component_order) == ["well", "channel"]
    assert display_layout.component_modes["well"] == "slice"


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
