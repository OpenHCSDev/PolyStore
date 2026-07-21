import pytest

from polystore.streaming.identity import StreamProducerIdentity
from polystore.streaming.viewer_transport import BatchViewerStreamSourceMetadata
from polystore.streaming.viewer_transport import ExplicitViewerTransportConfig
from polystore.streaming.viewer_transport import IndexedViewerStreamSourceMetadata
from polystore.streaming.viewer_transport import ViewerStreamBackendKwargs
from polystore.streaming.viewer_transport import ViewerStreamKwarg
from polystore.streaming.viewer_transport import ViewerDisplayConfigABC
from polystore.streaming.viewer_transport import ViewerFilenameParserABC
from polystore.streaming.viewer_transport import ViewerMetadataHandlerABC
from polystore.streaming.viewer_transport import ViewerMicroscopeHandlerABC
from polystore.streaming.viewer_transport import ViewerStreamRequest
from polystore.streaming.viewer_transport import ViewerStreamProducer
from polystore.streaming.viewer_transport import ViewerStreamSource
from polystore.streaming.viewer_transport import ViewerStreamSourceIdentity
from polystore.streaming.viewer_transport import ViewerStreamSourceMetadata
from zmqruntime.config import TransportMode, ZMQConfig
from zmqruntime.viewer_protocol import ViewerTransportEndpoint


class DisplayConfigFixture(ViewerDisplayConfigABC):
    COMPONENT_ORDER = ("well", "site", "channel")

    def component_modes(self):
        return {
            "well": "stack",
            "site": "slice",
            "channel": "channel",
        }


class FilenameParserFixture(ViewerFilenameParserABC):
    def parse_filename(self, filename):
        return {"filename": filename}


class MetadataHandlerFixture(ViewerMetadataHandlerABC):
    def get_component_values(self, plate_path, component_name):
        return f"{plate_path}:{component_name}"


class MicroscopeHandlerFixture(ViewerMicroscopeHandlerABC):
    parser = FilenameParserFixture()
    metadata_handler = MetadataHandlerFixture()


EMPTY_SOURCE_METADATA = BatchViewerStreamSourceMetadata(
    {"well": "A01", "site": 1, "channel": 1}
)


def stream_source(
    source_metadata=EMPTY_SOURCE_METADATA,
    *,
    plate_path="/tmp/plate",
):
    return ViewerStreamSource(
        identity=ViewerStreamSourceIdentity(
            microscope_handler=MicroscopeHandlerFixture(),
            plate_path=plate_path,
        ),
        metadata=source_metadata,
    )


def required_stream_request(**kwargs):
    values = {
        "viewer_transport": ViewerTransportEndpoint(
            host="127.0.0.1",
            port=5555,
            transport_mode=TransportMode.TCP,
        ),
        "display_config": DisplayConfigFixture(),
        "source": stream_source(),
        "producer": ViewerStreamProducer.from_identity(
            StreamProducerIdentity.pipeline_output(
                output_kind="main",
                output_key="main",
                projection_key="main",
                step_name="IdentifyPrimaryObjects",
                pipeline_position=2,
            )
        ),
    }
    values.update(kwargs)
    return ViewerStreamRequest(**values)


def test_viewer_stream_kwargs_declares_explicit_backend_request() -> None:
    stream_kwargs = required_stream_request(
        source=stream_source(
            IndexedViewerStreamSourceMetadata(
                metadata_by_index=(
                    {"well": "A01", "site": 1},
                    {"well": "A01", "site": 2},
                ),
            ),
            plate_path="/tmp/plate",
        ),
        message_extra={"component_value_domain": {"well": ["A01"]}},
        images_dir="/tmp/images",
    )

    backend_kwargs = ViewerStreamBackendKwargs(stream_kwargs).to_kwargs()

    assert backend_kwargs == {ViewerStreamKwarg.STREAM_REQUEST.value: stream_kwargs}
    assert ViewerStreamBackendKwargs.from_kwargs(backend_kwargs).stream_request is stream_kwargs
    assert stream_kwargs.host == "127.0.0.1"
    assert stream_kwargs.port == 5555
    assert stream_kwargs.transport_mode is TransportMode.TCP
    assert stream_kwargs.producer.identities == (
        StreamProducerIdentity.pipeline_output(
            output_kind="main",
            output_key="main",
            projection_key="main",
            step_name="IdentifyPrimaryObjects",
            pipeline_position=2,
        ),
    )
    assert stream_kwargs.source.metadata.metadata_by_index == (
                {"well": "A01", "site": 1},
                {"well": "A01", "site": 2},
    )
    default_config = ZMQConfig(default_port=9001)
    assert stream_kwargs.transport_config.resolve(default_config) is default_config


def test_viewer_stream_source_metadata_is_abstract_boundary() -> None:
    with pytest.raises(TypeError, match="abstract"):
        ViewerStreamSourceMetadata()


def test_viewer_stream_backend_rejects_flat_kwargs() -> None:
    with pytest.raises(ValueError, match="stream_request"):
        ViewerStreamBackendKwargs.from_kwargs(
            {"display_config": DisplayConfigFixture()}
        )


def test_viewer_stream_kwargs_preserves_explicit_transport_config() -> None:
    explicit_config = ZMQConfig(shared_ack_port=8111)
    default_config = ZMQConfig(shared_ack_port=8222)

    stream_kwargs = required_stream_request(
        transport_config=ExplicitViewerTransportConfig(explicit_config)
    )

    assert stream_kwargs.transport_config.resolve(default_config) is explicit_config


def test_viewer_stream_backend_rejects_non_request_payload() -> None:
    with pytest.raises(TypeError, match="ViewerStreamRequest"):
        ViewerStreamBackendKwargs.from_kwargs(
            {ViewerStreamKwarg.STREAM_REQUEST.value: DisplayConfigFixture()}
        )
