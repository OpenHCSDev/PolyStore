"""Container-independent acquisition tile coordinates, in fractional pixels."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, fields
from math import isfinite
from typing import ClassVar


@dataclass(frozen=True, slots=True)
class SourceTileGeometry:
    """An acquisition-relative XY placement, not an integer crop origin.

    Row/column coordinates are optional, zero-based discrete acquisition grid
    coordinates. They never imply an ordering of the source files.
    """

    metadata_field: ClassVar[str] = "source_tile_geometry"
    x_pixels: float
    y_pixels: float
    row: int | None = None
    column: int | None = None
    width_pixels: int | None = None
    height_pixels: int | None = None

    def __post_init__(self) -> None:
        x, y = float(self.x_pixels), float(self.y_pixels)
        if not isfinite(x) or not isfinite(y):
            raise ValueError("Source tile offsets must be finite pixel coordinates.")
        object.__setattr__(self, "x_pixels", x)
        object.__setattr__(self, "y_pixels", y)
        if (self.row is None) != (self.column is None):
            raise ValueError("Source tile row and column must be declared together.")
        for value in (self.row, self.column):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(
                    "Source tile grid coordinates must be nonnegative integers."
                )
        if (self.width_pixels is None) != (self.height_pixels is None):
            raise ValueError("Source tile width and height must be declared together.")
        for value in (self.width_pixels, self.height_pixels):
            if value is not None and (type(value) is not int or value <= 0):
                raise ValueError("Source tile dimensions must be positive integers.")

    def as_metadata_value(self) -> dict[str, float | int | None]:
        """Project the declaration's fields without a parallel field inventory."""
        return asdict(self)

    @classmethod
    def from_source_metadata(
        cls, metadata: Mapping | None
    ) -> SourceTileGeometry | None:
        if metadata is None or cls.metadata_field not in metadata:
            return None
        value = metadata[cls.metadata_field]
        if not isinstance(value, Mapping):
            raise ValueError("Source tile geometry must be a scalar field mapping.")
        unknown = set(value) - {field.name for field in fields(cls)}
        if unknown:
            raise ValueError(
                f"Unknown source tile geometry fields: {sorted(unknown)!r}."
            )
        return cls(**value)

    @classmethod
    def rectangular_grid_dimensions(
        cls,
        geometries: Iterable[SourceTileGeometry],
        *,
        require_row_major: bool = False,
    ) -> tuple[int, int] | None:
        """Prove a complete rectangle; unknown, holes and duplicates stay unknown."""
        values = tuple(geometries)
        if not values or any(value.row is None for value in values):
            return None
        coordinates = tuple((value.row, value.column) for value in values)
        rows = max(value.row for value in values) + 1
        columns = max(value.column for value in values) + 1
        expected = tuple(
            (row, column) for row in range(rows) for column in range(columns)
        )
        if len(coordinates) != len(set(coordinates)) or set(coordinates) != set(
            expected
        ):
            return None
        if require_row_major and coordinates != expected:
            return None
        return rows, columns
