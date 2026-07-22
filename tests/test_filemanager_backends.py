"""Parameterized tests for FileManager routing across backends.

These tests are intentionally compact: they reuse fixtures and exercise many
code paths via the `FileManager` router to achieve high coverage with little
test code.
"""
import pytest
import numpy as np
from pathlib import Path

from polystore import FileManager
from polystore.exceptions import StorageResolutionError
from polystore.omero_local import OMEROLocalBackend, PlateStructure


def _backend_path(tmp_path: Path, backend_name: str, filename: str) -> str:
    """Return a backend-appropriate path string for tests.

    - disk: use tmp_path (filesystem)
    - memory: use virtual posix path under /test
    """
    if backend_name == "disk":
        return str(tmp_path / filename)
    return "/test/" + filename


def test_filemanager_delegates_listed_address_resolution(file_manager):
    assert (
        file_manager.resolve_listed_address(
            "opaque-address",
            "memory",
            directory="/ignored",
        )
        == "opaque-address"
    )


def test_omero_backend_qualifies_relative_listed_addresses() -> None:
    backend = object.__new__(OMEROLocalBackend)

    assert backend.resolve_listed_address(
        "nested/A01_s001_w1_z001_t001.tif",
        directory="/omero/plate_7",
    ) == "/omero/plate_7/nested/A01_s001_w1_z001_t001.tif"
    assert backend.resolve_listed_address(
        "/omero/plate_8/A01_s001_w1_z001_t001.tif",
        directory="/omero/plate_7",
    ) == "/omero/plate_8/A01_s001_w1_z001_t001.tif"


def test_omero_backend_projects_save_context_from_base_plate_metadata() -> None:
    backend = object.__new__(OMEROLocalBackend)
    backend._plate_metadata = {
        7: PlateStructure(
            plate_id=7,
            parser_name="ImageXpressFilenameParser",
            microscope_type="ImageXpress",
            wells={},
            all_well_ids=set(),
            max_sites=0,
            max_z=0,
            max_c=0,
            max_t=0,
        )
    }

    assert backend.contextual_save_kwargs(
        images_dir="/omero/plate_7_outputs/images"
    ) == {
        "images_dir": "/omero/plate_7_outputs/images",
        "parser_name": "ImageXpressFilenameParser",
        "microscope_type": "ImageXpress",
    }


def test_omero_backend_loads_base_plate_metadata_for_save_context(monkeypatch) -> None:
    backend = object.__new__(OMEROLocalBackend)
    backend._plate_metadata = {}
    loaded_plate_ids = []

    def load_plate_structure(plate_id: int) -> None:
        loaded_plate_ids.append(plate_id)
        backend._plate_metadata[plate_id] = PlateStructure(
            plate_id=plate_id,
            parser_name="OperaPhenixFilenameParser",
            microscope_type="OperaPhenix",
            wells={},
            all_well_ids=set(),
            max_sites=0,
            max_z=0,
            max_c=0,
            max_t=0,
        )

    monkeypatch.setattr(backend, "_load_plate_structure", load_plate_structure)

    assert backend.contextual_save_kwargs(
        images_dir="/omero/plate_11_outputs/checkpoints_step0"
    ) == {
        "images_dir": "/omero/plate_11_outputs/checkpoints_step0",
        "parser_name": "OperaPhenixFilenameParser",
        "microscope_type": "OperaPhenix",
    }
    assert loaded_plate_ids == [11]


def test_physical_source_path_is_declared_by_backend_capability(file_manager) -> None:
    assert file_manager.physical_source_path(
        "/test/source.tif",
        "memory",
        base_path="/test",
    ) == "/test/source.tif"

    virtual_file_manager = FileManager(
        {"omero_local": object.__new__(OMEROLocalBackend)}
    )
    assert (
        virtual_file_manager.physical_source_path(
            "/omero/plate_7/A01_s001_w1_z001_t001.tif",
            "omero_local",
            base_path="/omero/plate_7",
        )
        is None
    )
    with pytest.raises(StorageResolutionError, match="not a DataSource"):
        virtual_file_manager.source_path(
            "/omero/plate_7/A01_s001_w1_z001_t001.tif",
            "omero_local",
            base_path="/omero/plate_7",
        )


@pytest.mark.parametrize("backend_name", ["memory", "disk"])
def test_save_load_roundtrip(file_manager, registry, sample_payloads, tmp_path, backend_name):
    # arrange
    fm = file_manager
    payloads = sample_payloads

    # Ensure directory exists for the backend
    if backend_name == "disk":
        fm.ensure_directory(str(tmp_path), backend=backend_name)
    else:
        fm.ensure_directory("/test", backend=backend_name)

    # numpy array roundtrip
    arr = payloads["array"]
    p = _backend_path(tmp_path, backend_name, "arr.npy")
    fm.save(arr, p, backend=backend_name)
    loaded = fm.load(p, backend=backend_name)
    assert np.array_equal(arr, loaded)

    # text roundtrip
    text = payloads["text"]
    p2 = _backend_path(tmp_path, backend_name, "hello.txt")
    fm.save(text, p2, backend=backend_name)
    assert fm.load(p2, backend=backend_name) == text

    # json roundtrip
    js = payloads["json"]
    p3 = _backend_path(tmp_path, backend_name, "data.json")
    fm.save(js, p3, backend=backend_name)
    assert fm.load(p3, backend=backend_name) == js


@pytest.mark.parametrize("backend_name", ["memory", "disk"])
def test_batch_save_and_load(file_manager, registry, tmp_path, backend_name):
    fm = file_manager

    if backend_name == "disk":
        fm.ensure_directory(str(tmp_path), backend=backend_name)
    else:
        fm.ensure_directory("/test", backend=backend_name)

    data_list = [np.array([1]), np.array([2]), np.array([3])]
    paths = [
        _backend_path(tmp_path, backend_name, f"b{i}.npy") for i in range(len(data_list))
    ]

    fm.save_batch(data_list, paths, backend=backend_name)
    loaded = fm.load_batch(paths, backend=backend_name)
    assert len(loaded) == len(data_list)
    for a, b in zip(data_list, loaded):
        assert np.array_equal(a, b)

    # mismatched lengths should surface as StorageResolutionError via FileManager
    from polystore.exceptions import StorageResolutionError
    with pytest.raises(StorageResolutionError):
        fm.save_batch([np.array([1])], ["/x/one.npy", "/x/two.npy"], backend=backend_name)


@pytest.mark.parametrize("backend_name", ["memory", "disk"])
def test_listing_and_find(file_manager, registry, tmp_path, backend_name):
    fm = file_manager

    # create nested structure
    if backend_name == "disk":
        base = tmp_path / "root"
        fm.ensure_directory(str(base), backend=backend_name)
        fm.save("a", str(base / "a.txt"), backend=backend_name)
        fm.ensure_directory(str(base / "sub"), backend=backend_name)
        fm.save("b", str(base / "sub" / "b.txt"), backend=backend_name)
        files = fm.list_files(str(base), backend=backend_name, recursive=True)
        assert any(f.endswith("a.txt") for f in files)
        found = fm.find_file_recursive(str(base), "b.txt", backend=backend_name)
        assert found is not None
    else:
        # memory backend with virtual paths
        fm.ensure_directory("/test", backend=backend_name)
        fm.save("a", "/test/a.txt", backend=backend_name)
        fm.ensure_directory("/test/sub", backend=backend_name)
        fm.save("b", "/test/sub/b.txt", backend=backend_name)
        files = fm.list_files("/test", backend=backend_name, recursive=True)
        assert any(str(f).endswith("a.txt") for f in files)
        found = fm.find_file_recursive("/test", "b.txt", backend=backend_name)
        assert found is not None


@pytest.mark.parametrize("backend_name", ["memory", "disk"])
def test_list_image_files_natural_sort(file_manager, registry, tmp_path, backend_name):
    fm = file_manager

    if backend_name == "disk":
        base = tmp_path / "imgs"
        fm.ensure_directory(str(base), backend=backend_name)
        # create files with numeric parts that natural sort should order
        # Use .txt to avoid requiring image writers in DiskBackend
        fm.save("x", str(base / "img2.txt"), backend=backend_name)
        fm.save("x", str(base / "img10.txt"), backend=backend_name)
        files = fm.list_image_files(str(base), backend=backend_name, extensions={'.txt'})
        assert files[0].endswith("img2.txt")
        assert files[1].endswith("img10.txt")
    else:
        fm.ensure_directory("/test", backend=backend_name)
        fm.save("x", "/test/img2.txt", backend=backend_name)
        fm.save("x", "/test/img10.txt", backend=backend_name)
        files = fm.list_image_files("/test", backend=backend_name, extensions={'.txt'})
        assert files[0].endswith("img2.txt")
        assert files[1].endswith("img10.txt")


def test_unknown_backend_raises(file_manager, sample_payloads, tmp_path):
    fm = file_manager
    with pytest.raises(StorageResolutionError):
        fm.save(sample_payloads["text"], _path := str(tmp_path / "x.txt"), backend="nope")
