from types import SimpleNamespace

import pytest

from polystore.streaming._streaming_backend import StreamingBackend
from polystore.streaming._streaming_backend import StreamingBatchItemPreparationAuthority
from polystore.streaming._streaming_backend import StreamingBatchMessageBuilder
from polystore.streaming._streaming_backend import StreamingBatchMessageRequest
from polystore.streaming._streaming_backend import StreamingComponentNamesRequest
from polystore.streaming._streaming_backend import StreamingItemPath
from polystore.streaming._streaming_backend import StreamingItemPreparationRequest
from polystore.streaming_constants import StreamingDataType
from polystore.streaming.identity import StreamProducerIdentity
from polystore.streaming.viewer_transport import BatchViewerStreamSourceMetadata
from polystore.streaming.viewer_transport import IndexedViewerStreamSourceMetadata
from polystore.streaming.viewer_transport import PathMappedViewerStreamSourceMetadata
from polystore.streaming.viewer_transport import ViewerDisplayConfigABC
from polystore.streaming.viewer_transport import ViewerMicroscopeHandlerABC
from polystore.streaming.viewer_transport import ViewerStreamProducer
from polystore.streaming.viewer_transport import ViewerStreamItemPayload
from polystore.streaming.viewer_transport import ViewerStreamRequest
from polystore.streaming.viewer_transport import ViewerStreamSource
from polystore.streaming.viewer_transport import ViewerStreamSourceIdentity
from polystore.streaming.viewer_transport import ViewerStreamSourceMetadata
from zmqruntime.config import TransportMode
from zmqruntime.viewer_protocol import ViewerAckPolicy
from zmqruntime.viewer_protocol import ViewerTransportEndpoint


class MetadataProbeStreamingBackend(StreamingBackend):
    VIEWER_TYPE = "probe"
    SHM_PREFIX = "probe_"

    def _prepare_batch_item(self, request: StreamingItemPreparationRequest):
        return ViewerStreamItemPayload(
            item_payload={"path": request.item_path.wire_value, "payload": "ok"},
            streaming_data_type=StreamingDataType.IMAGE,
        )

    def save_batch(self, data_list, file_paths, **kwargs):
        raise NotImplementedError


class DisplayConfigStub(ViewerDisplayConfigABC):
    COMPONENT_ORDER = ("well", "site", "channel")

    def component_modes(self):
        return {
            "well": "stack",
            "site": "stack",
            "channel": "stack",
        }


PRODUCER_IDENTITY = StreamProducerIdentity(
    origin="pipeline",
    output_kind="main",
    output_key="main",
    projection_key="main",
    step_name="IdentifyPrimaryObjects",
)


EMPTY_SOURCE_METADATA = BatchViewerStreamSourceMetadata(
    {"well": "A01", "site": 1, "channel": 1}
)


def stream_request(
    microscope_handler,
    source_metadata=EMPTY_SOURCE_METADATA,
    *,
    plate_path=None,
    message_extra=None,
    producer=None,
):
    return ViewerStreamRequest(
        viewer_transport=ViewerTransportEndpoint(
            host="127.0.0.1",
            port=5555,
            transport_mode=TransportMode.TCP,
        ),
        display_config=DisplayConfigStub(),
        source=ViewerStreamSource(
            identity=ViewerStreamSourceIdentity(
                microscope_handler=microscope_handler,
                plate_path=plate_path,
            ),
            metadata=source_metadata,
        ),
        producer=(
            ViewerStreamProducer.from_identity(PRODUCER_IDENTITY)
            if producer is None
            else producer
        ),
        message_extra=message_extra,
    )


def batch_message_request(data_list, file_paths, viewer_request):
    return StreamingBatchMessageRequest(
        data_list=data_list,
        file_paths=file_paths,
        stream_request=viewer_request,
        component_names_request=(
            StreamingComponentNamesRequest.from_stream_request(viewer_request)
        ),
    )


def test_streaming_backend_accepts_all_declared_pixel_payload_formats() -> None:
    backend = MetadataProbeStreamingBackend()

    assert backend.supports_file_path("result.npy")
    assert backend.supports_file_path("result.tif")
    assert backend.supports_file_path("result.mat")
    assert backend.supports_file_path("result.roi.zip")
    assert not backend.supports_file_path("result.csv")
    assert not backend.supports_file_path("result.txt")


def microscope_handler_with_parser(parser):
    class MicroscopeHandlerStub(ViewerMicroscopeHandlerABC):
        pass

    microscope_handler = MicroscopeHandlerStub()
    microscope_handler.parser = parser
    microscope_handler.metadata_handler = SimpleNamespace(
        get_component_values=lambda _plate_path, _component_name: None
    )
    return microscope_handler


def test_streaming_source_metadata_is_abstract_boundary() -> None:
    with pytest.raises(TypeError, match="abstract"):
        ViewerStreamSourceMetadata().component_metadata_for_item(
            "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip",
            0,
        )


def test_streaming_batch_items_reject_unparsed_artifact_filename() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = microscope_handler_with_parser(
        SimpleNamespace(parse_filename=lambda _filename: None)
    )

    with pytest.raises(KeyError, match="path-mapped component metadata"):
        StreamingBatchItemPreparationAuthority.prepare(
            backend,
            batch_message_request(
                [object()],
                ["A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip"],
                stream_request(
                    microscope_handler,
                    PathMappedViewerStreamSourceMetadata(metadata_by_path={}),
                ),
            )
        )


def test_streaming_batch_items_accept_per_path_component_metadata() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = microscope_handler_with_parser(
        SimpleNamespace(parse_filename=lambda _filename: None)
    )

    prepared_items = StreamingBatchItemPreparationAuthority.prepare(
        backend,
        batch_message_request(
            [object()],
            ["A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip"],
            stream_request(
                microscope_handler,
                PathMappedViewerStreamSourceMetadata(
                    metadata_by_path={
                        "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip": {
                            "well": "A01",
                            "site": 1,
                            "channel": 1,
                        },
                    }
                ),
            ),
        )
    )

    assert prepared_items.batch_images[0]["metadata"] == {
        "well": "A01",
        "site": 1,
        "channel": 1,
    }
    assert (
        prepared_items.batch_images[0]["producer_identity"]
        == PRODUCER_IDENTITY.to_payload()
    )


def test_streaming_batch_items_preserve_item_aligned_producer_identities() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = microscope_handler_with_parser(
        SimpleNamespace(parse_filename=lambda _filename: None)
    )
    producers = tuple(
        StreamProducerIdentity.pipeline_output(
            output_kind="main",
            output_key=output_key,
            projection_key="main",
            step_name="Align",
            pipeline_position=3,
        )
        for output_key in ("Stain1", "Stain2")
    )

    prepared_items = StreamingBatchItemPreparationAuthority.prepare(
        backend,
        batch_message_request(
            [object(), object()],
            ["A01_s001_w1.tif", "A01_s001_w2.tif"],
            stream_request(
                microscope_handler,
                IndexedViewerStreamSourceMetadata(
                    metadata_by_index=(
                        {"well": "A01", "site": 1, "channel": 1},
                        {"well": "A01", "site": 1, "channel": 2},
                    ),
                ),
                producer=ViewerStreamProducer.from_identities(producers),
            ),
        ),
    )

    assert tuple(
        item["producer_identity"]["output_key"]
        for item in prepared_items.batch_images
    ) == ("Stain1", "Stain2")


def test_streaming_item_component_metadata_preserves_explicit_fields() -> None:
    metadata = BatchViewerStreamSourceMetadata(
        {"well": "A01", "site": 1, "channel": 1},
    ).component_metadata_for_item(
        StreamingItemPath("A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip").value,
        0,
    )

    assert metadata == {
        "well": "A01",
        "site": 1,
        "channel": 1,
    }


def test_streaming_batch_message_declares_component_value_domain() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = microscope_handler_with_parser(
        SimpleNamespace(parse_filename=lambda _filename: None)
    )

    built_batch = StreamingBatchMessageBuilder.build(
        backend,
        batch_message_request(
            [object(), object()],
            ["A01_s001_w1_z001_t001.tif", "A01_s002_w2_z001_t001.tif"],
            stream_request(
                microscope_handler,
                IndexedViewerStreamSourceMetadata(
                    metadata_by_index=(
                        {"well": "A01", "site": 1, "channel": 1},
                        {"well": "A01", "site": 2, "channel": 2},
                    ),
                ),
            ),
        ),
    )

    assert built_batch.message["component_value_domain"] == {
        "well": ["A01"],
        "site": [1, 2],
        "channel": [1, 2],
    }


def test_streaming_batch_message_honors_declared_component_metadata_payload() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = microscope_handler_with_parser(
        SimpleNamespace(parse_filename=lambda _filename: None)
    )

    built_batch = StreamingBatchMessageBuilder.build(
        backend,
        batch_message_request(
            [object()],
            ["A01_s001_w1_z001_t001.tif"],
            stream_request(
                microscope_handler,
                BatchViewerStreamSourceMetadata(
                    {"well": "A01", "site": 1, "channel": 1}
                ),
                message_extra={
                    "component_value_domain": {"well": ["A01", "B01"]},
                    "component_names_metadata": {"well": {"A01": "control"}},
                },
            ),
        ),
    )

    assert built_batch.message["component_value_domain"] == {"well": ["A01", "B01"]}
    assert built_batch.message["component_names_metadata"] == {
        "well": {"A01": "control"}
    }


def test_streaming_batch_message_rejects_partial_declared_component_metadata_payload() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = microscope_handler_with_parser(
        SimpleNamespace(parse_filename=lambda _filename: None)
    )

    with pytest.raises(ValueError, match="component_names_metadata"):
        StreamingBatchMessageBuilder.build(
            backend,
            batch_message_request(
                [object()],
                ["A01_s001_w1_z001_t001.tif"],
                stream_request(
                    microscope_handler,
                    BatchViewerStreamSourceMetadata(
                        {"well": "A01", "site": 1, "channel": 1}
                    ),
                    message_extra={"component_value_domain": {"well": ["A01"]}},
                ),
            ),
        )


def test_streaming_component_metadata_rejects_invalid_explicit_metadata() -> None:
    with pytest.raises(TypeError, match="must be a mapping"):
        BatchViewerStreamSourceMetadata(
            ["not", "metadata"],
        ).component_metadata_for_item(
            StreamingItemPath("A01_s001_w1_z001_t001.TIF").value,
            0,
        )


class ViewerAckSocketStub:
    def __init__(self, response):
        self.response = response

    def recv_json(self):
        return self.response


def test_viewer_ack_policy_rejects_error_status_and_cleans_up() -> None:
    cleanup_calls = []
    policy = ViewerAckPolicy(viewer_name="Napari", timeout_ms=30_000)

    with pytest.raises(RuntimeError, match="Napari rejected stream batch"):
        policy.receive(
            ViewerAckSocketStub(
                {"status": "error", "message": "missing component_value_domain"}
            ),
            lambda: cleanup_calls.append("cleanup"),
            port=5555,
        )

    assert cleanup_calls == ["cleanup"]
