"""Declaration-owned semantic layout for dense Zarr image batches."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum

_ATTR_PREFIX = "polystore"
ATTR_FILENAME_MAP = f"{_ATTR_PREFIX}_filename_map"
ATTR_OUTPUT_PATHS = f"{_ATTR_PREFIX}_output_paths"
ATTR_DIMENSIONS = f"{_ATTR_PREFIX}_dimensions"
ATTR_IMAGE_COORDINATE = f"{_ATTR_PREFIX}_image_coordinate"


class ZarrBatchAxisRole(Enum):
    """Storage role owned by one declared batch axis."""

    ARRAY = "array"
    HCS_IMAGE = "hcs_image"


@dataclass(frozen=True, slots=True)
class ZarrBatchAxis:
    """One declared non-pixel axis in a dense Zarr image batch.

    ``values`` preserves the semantic coordinate labels represented by array
    indices. PolyStore treats the labels as opaque values; applications own
    their meaning and project them into this storage declaration.
    """

    name: str
    axis_type: str
    values: tuple[str, ...]
    role: ZarrBatchAxisRole = ZarrBatchAxisRole.ARRAY

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Zarr batch axis name cannot be empty")
        if not self.axis_type:
            raise ValueError("Zarr batch axis type cannot be empty")
        if self.name in {"x", "y"}:
            raise ValueError("Zarr batch axes cannot redeclare pixel axes x or y")
        if not self.values:
            raise ValueError(f"Zarr batch axis {self.name!r} has no values")
        if len(set(self.values)) != len(self.values):
            raise ValueError(
                f"Zarr batch axis {self.name!r} contains duplicate values"
            )

    @property
    def size(self) -> int:
        """Return the declared axis extent."""

        return len(self.values)

    def ngff_declaration(self) -> dict[str, str]:
        """Return this axis in the representation consumed by ome-zarr."""

        return {"name": self.name, "type": self.axis_type}


@dataclass(frozen=True, slots=True)
class ZarrBatchLayout:
    """Dense non-pixel axis layout and exact coordinate for every batch item."""

    axes: tuple[ZarrBatchAxis, ...]
    item_coordinates: tuple[tuple[int, ...], ...]

    def __post_init__(self) -> None:
        axis_names = tuple(axis.name for axis in self.axes)
        if len(set(axis_names)) != len(axis_names):
            raise ValueError(f"Zarr batch axis names must be unique: {axis_names!r}")

        image_axis_positions = tuple(
            index
            for index, axis in enumerate(self.axes)
            if axis.role is ZarrBatchAxisRole.HCS_IMAGE
        )
        if len(image_axis_positions) > 1:
            raise ValueError("A Zarr batch can declare at most one HCS image axis")

        expected_rank = len(self.axes)
        for coordinate in self.item_coordinates:
            if len(coordinate) != expected_rank:
                raise ValueError(
                    "Zarr batch item coordinate rank must match its declared axes: "
                    f"got {coordinate!r} for {axis_names!r}"
                )
            for index, axis in zip(coordinate, self.axes, strict=True):
                if index < 0 or index >= axis.size:
                    raise ValueError(
                        f"Zarr batch coordinate {index} is outside axis "
                        f"{axis.name!r} with extent {axis.size}"
                    )

        if len(set(self.item_coordinates)) != len(self.item_coordinates):
            raise ValueError("Zarr batch item coordinates must be unique")
        expected_items = math.prod(self.shape)
        if len(self.item_coordinates) != expected_items:
            raise ValueError(
                "Zarr batch layout must declare one item for every coordinate: "
                f"got {len(self.item_coordinates)}, expected {expected_items} "
                f"for shape {self.shape!r}"
            )

    @property
    def shape(self) -> tuple[int, ...]:
        """Return the declared non-pixel array shape."""

        return tuple(axis.size for axis in self.axes)

    @property
    def image_axis_position(self) -> int | None:
        """Return the coordinate position projected into HCS image groups."""

        return next(
            (
                index
                for index, axis in enumerate(self.axes)
                if axis.role is ZarrBatchAxisRole.HCS_IMAGE
            ),
            None,
        )

    @property
    def image_count(self) -> int:
        """Return the number of HCS image groups represented by the layout."""

        position = self.image_axis_position
        return 1 if position is None else self.axes[position].size

    @property
    def array_axes(self) -> tuple[ZarrBatchAxis, ...]:
        """Return axes represented inside each HCS image array."""

        return tuple(
            axis for axis in self.axes if axis.role is ZarrBatchAxisRole.ARRAY
        )

    @property
    def array_shape(self) -> tuple[int, ...]:
        """Return the leading array shape inside each HCS image group."""

        return tuple(axis.size for axis in self.array_axes)

    @property
    def ngff_axes(self) -> tuple[dict[str, str], ...]:
        """Return declared axes followed by the storage-owned pixel axes."""

        return tuple(axis.ngff_declaration() for axis in self.array_axes) + (
            {"name": "y", "type": "space"},
            {"name": "x", "type": "space"},
        )

    def dimensions_attribute(self) -> dict[str, Mapping[str, object]]:
        """Return the PolyStore metadata representation of semantic axes."""

        return {
            axis.name: {
                "size": axis.size,
                "values": list(axis.values),
                "role": axis.role.value,
            }
            for axis in self.axes
        }

    def image_index(self, coordinate: tuple[int, ...]) -> int:
        """Return the HCS image-group index for one full coordinate."""

        position = self.image_axis_position
        return 0 if position is None else coordinate[position]

    def array_coordinate(self, coordinate: tuple[int, ...]) -> tuple[int, ...]:
        """Remove the image-group coordinate from a full item coordinate."""

        return tuple(
            index
            for axis, index in zip(self.axes, coordinate, strict=True)
            if axis.role is ZarrBatchAxisRole.ARRAY
        )

    def image_coordinate_attribute(
        self,
        coordinate: tuple[int, ...],
    ) -> dict[str, str]:
        """Return semantic values represented by the HCS image group."""

        return {
            axis.name: axis.values[index]
            for axis, index in zip(self.axes, coordinate, strict=True)
            if axis.role is ZarrBatchAxisRole.HCS_IMAGE
        }


@dataclass(frozen=True, slots=True)
class ZarrStoredBatchSemantics:
    """Read semantic axis labels from PolyStore-owned Zarr attributes."""

    axis_values: Mapping[str, tuple[str, ...]]
    image_coordinate: Mapping[str, str]

    @classmethod
    def from_attrs(cls, attrs: Mapping[str, object]) -> ZarrStoredBatchSemantics:
        """Decode available PolyStore attributes without requiring them."""

        dimensions = attrs.get(ATTR_DIMENSIONS, {})
        axis_values: dict[str, tuple[str, ...]] = {}
        if isinstance(dimensions, Mapping):
            for axis_name, declaration in dimensions.items():
                if not isinstance(declaration, Mapping):
                    continue
                values = declaration.get("values")
                if isinstance(values, Sequence) and not isinstance(values, str):
                    axis_values[str(axis_name)] = tuple(str(value) for value in values)

        image_coordinate_payload = attrs.get(ATTR_IMAGE_COORDINATE, {})
        image_coordinate = (
            {
                str(axis_name): str(value)
                for axis_name, value in image_coordinate_payload.items()
            }
            if isinstance(image_coordinate_payload, Mapping)
            else {}
        )
        return cls(axis_values, image_coordinate)

    def array_axis_value(self, axis_name: str, index: int, fallback: str) -> str:
        """Return a stored semantic array coordinate or its caller fallback."""

        values = self.axis_values.get(axis_name)
        return fallback if values is None else values[index]

    def image_axis_value(self, axis_name: str, fallback: str) -> str:
        """Return a stored semantic HCS image coordinate or caller fallback."""

        return self.image_coordinate.get(axis_name, fallback)
