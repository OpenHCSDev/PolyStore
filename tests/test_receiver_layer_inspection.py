import numpy as np
import pytest

from polystore.streaming.receivers.core.layer_inspection import NativeLayerTransformInspectionABC


def test_napari_inspector_reads_native_image_and_shapes_without_mutation():
    layers = pytest.importorskip("napari.layers")
    from polystore.streaming.receivers.napari.layer_inspection import (
        NapariNativeLayerTransformInspection,
    )

    image = layers.Image(np.zeros((4, 8)), scale=(0.65, 0.65), translate=(2, 3))
    shapes = layers.Shapes(
        [np.array([[0, 0], [0, 3], [3, 3]])], shape_type="polygon", scale=(1, 1), translate=(2, 3)
    )
    image_inspector = NapariNativeLayerTransformInspection(image)
    assert isinstance(image_inspector, NativeLayerTransformInspectionABC)
    image_snapshot = image_inspector.snapshot()
    shape_snapshot = NapariNativeLayerTransformInspection(shapes).snapshot()
    assert image_snapshot.scale == (0.65, 0.65)
    assert shape_snapshot.scale == (1.0, 1.0)
    assert image_snapshot.translate == shape_snapshot.translate == (2.0, 3.0)
    np.testing.assert_array_equal(image.scale, (0.65, 0.65))
    np.testing.assert_array_equal(shapes.scale, (1, 1))


@pytest.mark.parametrize("rgb", [False, True])
def test_native_image_intensity_control_changes_only_native_presentation(rgb):
    layers = pytest.importorskip("napari.layers")
    from polystore.streaming.receivers.core.image_presentation import NativeImageIntensityPresentationABC
    from polystore.streaming.receivers.napari.image_presentation import NapariNativeImageIntensityPresentation
    from zmqruntime.viewer_protocol import ViewerNativeImageIntensityPresentation

    pixels = np.arange(96 if rgb else 32, dtype=np.uint16).reshape((4, 8, 3) if rgb else (4, 8))
    layer = layers.Image(pixels, rgb=rgb, scale=(0.65, 0.65), translate=(2, 3),
                         contrast_limits=(0, 100), gamma=1)
    control = NapariNativeImageIntensityPresentation.for_layer(layer)
    assert isinstance(control, NativeImageIntensityPresentationABC)
    events = []
    layer.events.gamma.connect(lambda event: events.append(event.type))
    layer.events.contrast_limits.connect(lambda event: events.append(event.type))
    original = layer.data.copy()
    control.apply(ViewerNativeImageIntensityPresentation((10, 80), 1.5))
    assert control.snapshot() == ViewerNativeImageIntensityPresentation((10, 80), 1.5)
    assert set(events) == {"gamma", "contrast_limits"}
    assert layer.data is pixels
    np.testing.assert_array_equal(layer.data, original)
    np.testing.assert_array_equal(layer.scale, (0.65, 0.65))
    np.testing.assert_array_equal(layer.translate, (2, 3))
    assert layer.visible is True
    layer.gamma = 2
    assert control.snapshot().gamma == 2
    assert NapariNativeImageIntensityPresentation.for_layer(layers.Labels(np.zeros((4, 8), dtype=int))) is None
