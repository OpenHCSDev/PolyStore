from types import SimpleNamespace

import pytest

from polystore.streaming._streaming_backend import StreamingBackend
from polystore.streaming._streaming_backend import StreamingBatchRequest


class MetadataProbeStreamingBackend(StreamingBackend):
    VIEWER_TYPE = "probe"
    SHM_PREFIX = "probe_"

    def save_batch(self, data_list, file_paths, **kwargs):
        raise NotImplementedError


def test_streaming_component_metadata_rejects_unparsed_artifact_filename() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    with pytest.raises(ValueError, match="explicit component_metadata"):
        backend._parse_component_metadata(
            "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip",
            microscope_handler,
            source="IdentifyPrimaryObjects",
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
                source="IdentifyPrimaryObjects",
                prepare_item=lambda _data, _path, _data_type: ({"payload": "ok"}, "image"),
            )
        )


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
        source="Crop",
    )

    assert metadata == {"well": "A01", "channel": 1, "source": "Crop"}


def test_streaming_component_metadata_prefers_explicit_metadata() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    metadata = backend._parse_component_metadata(
        "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip",
        microscope_handler,
        source="IdentifyPrimaryObjects",
        component_metadata={"well": "A01", "site": 1, "channel": 1},
    )

    assert metadata == {
        "well": "A01",
        "site": 1,
        "channel": 1,
        "source": "IdentifyPrimaryObjects",
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
            source="Crop",
        )
