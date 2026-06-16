from types import SimpleNamespace

import pytest

from polystore.streaming._streaming_backend import StreamingBackend
from polystore.streaming._streaming_backend import StreamingBatchRequest
from polystore.streaming.identity import StreamProducerIdentity


class MetadataProbeStreamingBackend(StreamingBackend):
    VIEWER_TYPE = "probe"
    SHM_PREFIX = "probe_"

    def save_batch(self, data_list, file_paths, **kwargs):
        raise NotImplementedError


PRODUCER_IDENTITY = StreamProducerIdentity(
    origin="pipeline",
    output_kind="main",
    output_key="main",
    step_name="IdentifyPrimaryObjects",
)


def test_streaming_component_metadata_rejects_unparsed_artifact_filename() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    with pytest.raises(ValueError, match="explicit component_metadata"):
        backend._parse_component_metadata(
            "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip",
            microscope_handler,
        )


def test_streaming_batch_items_reject_unparsed_artifact_filename() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    with pytest.raises(ValueError, match="explicit component_metadata"):
        backend._prepare_batch_items(
            StreamingBatchRequest(
                data_list=[object()],
                file_paths=["A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip"],
                microscope_handler=microscope_handler,
                producer_identity=PRODUCER_IDENTITY,
                prepare_item=lambda _data, _path, _data_type: ({"payload": "ok"}, "image"),
            )
        )


def test_streaming_batch_items_accept_per_path_component_metadata() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    batch_images, _image_ids = backend._prepare_batch_items(
        StreamingBatchRequest(
            data_list=[object()],
            file_paths=["A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip"],
            microscope_handler=microscope_handler,
            producer_identity=PRODUCER_IDENTITY,
            prepare_item=lambda _data, _path, _data_type: ({"payload": "ok"}, "image"),
            component_metadata_by_path={
                "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip": {
                    "well": "A01",
                    "site": 1,
                    "channel": 1,
                },
            },
        )
    )

    assert batch_images[0]["metadata"] == {
        "well": "A01",
        "site": 1,
        "channel": 1,
    }
    assert batch_images[0]["producer_identity"] == PRODUCER_IDENTITY.to_payload()


def test_streaming_component_metadata_preserves_parsed_filename_fields() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(
            parse_filename=lambda _filename: {"well": "A01", "channel": 1}
        )
    )

    metadata = backend._parse_component_metadata(
        "A01_s001_w1_z001_t001.TIF",
        microscope_handler,
    )

    assert metadata == {"well": "A01", "channel": 1}


def test_streaming_component_metadata_prefers_explicit_metadata() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    metadata = backend._parse_component_metadata(
        "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip",
        microscope_handler,
        component_metadata={"well": "A01", "site": 1, "channel": 1},
    )

    assert metadata == {
        "well": "A01",
        "site": 1,
        "channel": 1,
    }


def test_streaming_component_metadata_rejects_invalid_parser_result() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: ["not", "metadata"])
    )

    with pytest.raises(TypeError, match="must be a mapping"):
        backend._parse_component_metadata(
            "A01_s001_w1_z001_t001.TIF",
            microscope_handler,
        )
