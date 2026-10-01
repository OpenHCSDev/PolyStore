"""Fresh native Python recipient of a real FileManager handoff."""

import pickle
import sys
from pathlib import Path

import numpy as np

import polystore
from polystore.base import ImageSamplingRequest


def main() -> None:
    assert Path(polystore.__file__).resolve().parents[2] == Path(__file__).resolve().parents[1]
    with Path(sys.argv[1]).open("rb") as stream:
        manager, root, config, expected = pickle.load(stream)
    workspace = manager.registry["virtual_workspace"]
    assert workspace.metadata_config == config
    assert workspace._registry is manager.registry
    assert workspace._resolve_ref(root / "virtual.npy").source_axis_indices == (1,)
    np.testing.assert_array_equal(
        manager.load(root / "virtual.npy", backend="virtual_workspace"), expected
    )
    sample = manager.sample(
        root / "virtual.npy",
        backend="virtual_workspace",
        request=ImageSamplingRequest(origin_yx=(1, 1), shape_yx=(2, 3)),
    )
    np.testing.assert_array_equal(sample.data, expected[1:3, 1:4])
    assert sample.source_shape == expected.shape
    print("native handoff: exact namespace, registry, pixels, axes and sample provenance")


if __name__ == "__main__":
    main()
