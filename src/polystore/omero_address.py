"""Canonical virtual addresses for OMERO plate image planes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from functools import cache
from pathlib import PurePosixPath
from typing import ClassVar


class OMEROPlaneAxis(Enum):
    """Declare one OMERO virtual-filename axis and its wire presentation."""

    SITE = ("s", 3)
    CHANNEL = ("w", 0)
    Z_INDEX = ("z", 3)
    TIMEPOINT = ("t", 3)

    def __init__(self, prefix: str, padding: int) -> None:
        self._prefix = prefix
        self._padding = padding

    @property
    def component_name(self) -> str:
        """Return the generic filename-parser name derived from this declaration."""

        return self.name.lower()

    def normalize(self, value: int | str) -> int:
        """Normalize one positive, one-based coordinate for this axis."""

        if isinstance(value, bool):
            raise ValueError(f"{self.component_name} must be a positive integer")
        if isinstance(value, int):
            normalized = value
        elif isinstance(value, str) and value.strip().isdecimal():
            normalized = int(value.strip())
        else:
            raise ValueError(f"{self.component_name} must be a positive integer")
        if normalized < 1:
            raise ValueError(f"{self.component_name} must be a positive integer")
        return normalized

    def filename_pattern(self) -> str:
        """Return this axis member's named-capture filename grammar."""

        return f"_{self._prefix}(?P<{self.component_name}>\\d+)"

    def filename_fragment(self, value: int | str) -> str:
        """Render one coordinate through this axis member's wire declaration."""

        normalized = self.normalize(value)
        return f"_{self._prefix}{normalized:0{self._padding}d}"

    def captured_value(self, match: re.Match[str]) -> int:
        """Read and normalize this axis member from a regex match."""

        return self.normalize(match.group(self.component_name))


@dataclass(frozen=True, slots=True)
class OMEROWellAddress:
    """Zero-based OMERO well coordinates with a canonical plate label."""

    row_index: int
    column_index: int

    component_name: ClassVar[str] = "well"

    @classmethod
    def filename_pattern(cls) -> str:
        """Return the named-capture grammar for a canonical well label."""

        return f"(?P<{cls.component_name}>[A-Za-z]+\\d+)"

    @classmethod
    @cache
    def label_pattern(cls) -> re.Pattern[str]:
        """Compile the canonical well-label grammar for direct parsing."""

        return re.compile(f"^{cls.filename_pattern()}$")

    def __post_init__(self) -> None:
        for field_name, value in (
            ("row_index", self.row_index),
            ("column_index", self.column_index),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{field_name} must be a nonnegative integer")

    @classmethod
    def from_label(cls, label: str) -> OMEROWellAddress:
        """Parse an OMERO well label such as ``A01`` or ``AA01``."""

        match = cls.label_pattern().fullmatch(str(label).strip())
        if match is None:
            raise ValueError(f"Invalid OMERO well label: {label!r}")

        normalized = match.group(cls.component_name)
        column_start = next(
            index for index, character in enumerate(normalized) if character.isdecimal()
        )
        row_label = normalized[:column_start]
        column_label = normalized[column_start:]

        row_number = 0
        for character in row_label.upper():
            row_number = row_number * 26 + ord(character) - ord("A") + 1

        column_number = int(column_label)
        if column_number < 1:
            raise ValueError(f"OMERO well columns are one-based: {label!r}")
        return cls(row_index=row_number - 1, column_index=column_number - 1)

    @property
    def row_label(self) -> str:
        """Return the spreadsheet-style label for this OMERO row."""

        value = self.row_index + 1
        characters: list[str] = []
        while value:
            value, remainder = divmod(value - 1, 26)
            characters.append(chr(ord("A") + remainder))
        return "".join(reversed(characters))

    @property
    def label(self) -> str:
        """Return the canonical zero-padded well label."""

        return f"{self.row_label}{self.column_index + 1:02d}"


@dataclass(frozen=True, slots=True, init=False)
class OMEROPlaneCoordinates:
    """Complete OMERO plane coordinates keyed by nominal axis declarations."""

    _axis_values: tuple[tuple[OMEROPlaneAxis, int], ...]

    def __init__(
        self,
        axis_values: Mapping[OMEROPlaneAxis, int | str],
    ) -> None:
        if any(not isinstance(axis, OMEROPlaneAxis) for axis in axis_values):
            raise TypeError("OMERO coordinates require OMEROPlaneAxis keys")
        missing = frozenset(OMEROPlaneAxis) - frozenset(axis_values)
        if missing:
            raise ValueError(
                "OMERO plane coordinates must bind every declared axis: "
                + ", ".join(
                    f"missing {axis.component_name}" for axis in OMEROPlaneAxis if axis in missing
                )
            )

        object.__setattr__(
            self,
            "_axis_values",
            tuple((axis, axis.normalize(axis_values[axis])) for axis in OMEROPlaneAxis),
        )

    @classmethod
    def from_match(cls, match: re.Match[str]) -> OMEROPlaneCoordinates:
        """Construct coordinates by asking every axis to read its own capture."""

        return cls({axis: axis.captured_value(match) for axis in OMEROPlaneAxis})

    def __getitem__(self, axis: OMEROPlaneAxis) -> int:
        if not isinstance(axis, OMEROPlaneAxis):
            raise TypeError("OMERO coordinates are indexed by OMEROPlaneAxis")
        for declared_axis, value in self.declared_values():
            if declared_axis is axis:
                return value
        raise KeyError(axis)

    def declared_values(self) -> tuple[tuple[OMEROPlaneAxis, int], ...]:
        """Return values in the wire order owned by the axis declaration."""

        return self._axis_values

    def zero_based(self, axis: OMEROPlaneAxis) -> int:
        """Project a declared one-based coordinate onto an OMERO array index."""

        return self[axis] - 1

    @classmethod
    @cache
    def filename_pattern(cls) -> str:
        """Derive the complete coordinate grammar from nominal axes."""

        return "".join(axis.filename_pattern() for axis in OMEROPlaneAxis)

    def filename_fragment(self) -> str:
        """Render all coordinates through their nominal axis declarations."""

        return "".join(axis.filename_fragment(value) for axis, value in self.declared_values())


@dataclass(frozen=True, slots=True)
class OMEROPlaneAddress:
    """Canonical virtual filename identity for one OMERO image plane."""

    well: OMEROWellAddress
    coordinates: OMEROPlaneCoordinates
    extension: str = ".tif"

    def __post_init__(self) -> None:
        if not isinstance(self.well, OMEROWellAddress):
            raise TypeError("well must be an OMEROWellAddress")
        if not isinstance(self.coordinates, OMEROPlaneCoordinates):
            raise TypeError("coordinates must be OMEROPlaneCoordinates")

        extension = str(self.extension).strip()
        if not extension or any(separator in extension for separator in ("/", "\\")):
            raise ValueError(f"Invalid OMERO plane extension: {self.extension!r}")
        if not extension.startswith("."):
            extension = f".{extension}"
        object.__setattr__(self, "extension", extension)

    @classmethod
    @cache
    def filename_pattern(cls) -> re.Pattern[str]:
        """Derive the complete virtual filename grammar from its declarations."""

        return re.compile(
            "^"
            + OMEROWellAddress.filename_pattern()
            + OMEROPlaneCoordinates.filename_pattern()
            + r"(?:_[^.]*)?(?P<extension>(?:\.[A-Za-z0-9]+)+)$"
        )

    @classmethod
    @cache
    def image_name_pattern(cls) -> re.Pattern[str]:
        """Derive persisted OMERO image-name grammar from the site declaration."""

        return re.compile(
            "^" + OMEROWellAddress.filename_pattern() + OMEROPlaneAxis.SITE.filename_pattern() + "$"
        )

    @classmethod
    def image_name(
        cls,
        *,
        well: str | OMEROWellAddress,
        site: int | str,
    ) -> str:
        """Render the persisted OMERO image identity for one well/site."""

        well_address = (
            well if isinstance(well, OMEROWellAddress) else OMEROWellAddress.from_label(well)
        )
        return f"{well_address.label}{OMEROPlaneAxis.SITE.filename_fragment(site)}"

    @classmethod
    def site_for_well_sample(
        cls,
        *,
        well: str | OMEROWellAddress,
        image_name: str,
        ordinal: int,
    ) -> int:
        """Resolve a well-sample site from a declared image name or OMERO order."""

        well_address = (
            well if isinstance(well, OMEROWellAddress) else OMEROWellAddress.from_label(well)
        )
        match = cls.image_name_pattern().fullmatch(str(image_name))
        if (
            match is not None
            and OMEROWellAddress.from_label(match.group(OMEROWellAddress.component_name))
            == well_address
        ):
            return OMEROPlaneAxis.SITE.captured_value(match)
        return OMEROPlaneAxis.SITE.normalize(ordinal)

    @classmethod
    def from_filename(cls, filename: str) -> OMEROPlaneAddress | None:
        """Parse a canonical OMERO virtual filename."""

        basename = PurePosixPath(str(filename).replace("\\", "/")).name
        match = cls.filename_pattern().fullmatch(basename)
        if match is None:
            return None
        return cls(
            well=OMEROWellAddress.from_label(match.group(OMEROWellAddress.component_name)),
            coordinates=OMEROPlaneCoordinates.from_match(match),
            extension=match.group("extension"),
        )

    def filename(self) -> str:
        """Render the canonical OMERO virtual filename."""

        return f"{self.well.label}{self.coordinates.filename_fragment()}{self.extension}"
