"""OMERO virtual paths answer the storage directory and file queries."""

import pytest

from polystore.omero_local import OMEROLocalBackend


@pytest.mark.parametrize(
    ("path", "is_dir", "is_file"),
    (
        ("/omero/plate_1", True, False),
        ("/omero/plate_1_outputs/images", True, False),
        ("/omero/plate_1_outputs/images/A01_s001_w1_z001_t001.tif", False, True),
        ("/omero/plate_1_outputs/images_results/A01_measurements.csv", False, True),
        ("/data/plate_1", False, False),
        ("/omero/not_a_plate", False, False),
    ),
)
def test_omero_paths_are_directories_unless_they_name_a_file(
    path: str, is_dir: bool, is_file: bool
) -> None:
    # The path queries need no server connection.
    backend = object.__new__(OMEROLocalBackend)

    assert backend.is_dir(path) is is_dir
    assert backend.is_file(path) is is_file
