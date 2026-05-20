from types import SimpleNamespace

import pytest

from polystore.streaming._streaming_backend import StreamingBackend


class MetadataProbeStreamingBackend(StreamingBackend):
    VIEWER_TYPE = "probe"
    SHM_PREFIX = "probe_"

    def save_batch(self, data_list, file_paths, **kwargs):
        raise NotImplementedError


def test_streaming_component_metadata_accepts_unparsed_artifact_filename() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    metadata = backend._parse_component_metadata(
        "A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip",
        microscope_handler,
        source="IdentifyPrimaryObjects",
    )

    assert metadata == {"source": "IdentifyPrimaryObjects"}


def test_streaming_batch_items_accept_unparsed_artifact_filename() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: None)
    )

    batch_images, image_ids = backend._prepare_batch_items(
        [object()],
        ["A01_s001_w1_z001_t001_Nuclei_step3_rois.roi.zip"],
        microscope_handler,
        "IdentifyPrimaryObjects",
        lambda _data, _path, _data_type: ({"payload": "ok"}, "image"),
    )

    assert len(image_ids) == 1
    assert batch_images[0]["metadata"] == {"source": "IdentifyPrimaryObjects"}
    assert batch_images[0]["payload"] == "ok"


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


def test_streaming_component_metadata_rejects_invalid_parser_result() -> None:
    backend = MetadataProbeStreamingBackend()
    microscope_handler = SimpleNamespace(
        parser=SimpleNamespace(parse_filename=lambda _filename: ["not", "metadata"])
    )

    with pytest.raises(TypeError, match="mapping or None"):
        backend._parse_component_metadata(
            "A01_s001_w1_z001_t001.TIF",
            microscope_handler,
            source="Crop",
        )
