"""Canonical virtual addresses for OMERO plate image planes."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from functools import cache
from pathlib import PurePosixPath
from typing import cast

OMEROAddressValue = str | int


class OMEROAddressComponentBehaviorABC(ABC):
    """Leaf behavior owned by one OMERO address component declaration."""

    @property
    @abstractmethod
    def is_plane_coordinate(self) -> bool:
        """Return whether this component indexes pixels within a well image."""

    @abstractmethod
    def normalize(self, component_name: str, value: object) -> OMEROAddressValue:
        """Normalize one declared component value."""

    @abstractmethod
    def filename_pattern(self, component_name: str) -> str:
        """Return this component's named-capture filename grammar."""

    @abstractmethod
    def filename_fragment(self, component_name: str, value: object) -> str:
        """Render one normalized component value."""

    def zero_based(self, component_name: str, value: object) -> int:
        """Project a plane coordinate onto a zero-based OMERO array index."""

        raise TypeError(f"{component_name} is not an OMERO plane coordinate")


@dataclass(frozen=True, slots=True)
class OMEROWellComponentBehavior(OMEROAddressComponentBehaviorABC):
    """Own well-label parsing and rendering behavior."""

    @property
    def is_plane_coordinate(self) -> bool:
        return False

    def normalize(self, component_name: str, value: object) -> str:
        del component_name
        if isinstance(value, OMEROWellAddress):
            return value.label
        return OMEROWellAddress.from_label(str(value)).label

    def filename_pattern(self, component_name: str) -> str:
        return OMEROWellAddress.filename_pattern(component_name)

    def filename_fragment(self, component_name: str, value: object) -> str:
        return self.normalize(component_name, value)


@dataclass(frozen=True, slots=True)
class OMEROPlaneCoordinateBehavior(OMEROAddressComponentBehaviorABC):
    """Own one positive one-based plane coordinate's wire behavior."""

    prefix: str
    padding: int

    @property
    def is_plane_coordinate(self) -> bool:
        return True

    def normalize(self, component_name: str, value: object) -> int:
        if isinstance(value, bool):
            raise ValueError(f"{component_name} must be a positive integer")
        if isinstance(value, int):
            normalized = value
        elif isinstance(value, str) and value.strip().isdecimal():
            normalized = int(value.strip())
        else:
            raise ValueError(f"{component_name} must be a positive integer")
        if normalized < 1:
            raise ValueError(f"{component_name} must be a positive integer")
        return normalized

    def filename_pattern(self, component_name: str) -> str:
        return f"_{self.prefix}(?P<{component_name}>\\d+)"

    def filename_fragment(self, component_name: str, value: object) -> str:
        normalized = self.normalize(component_name, value)
        return f"_{self.prefix}{normalized:0{self.padding}d}"

    def zero_based(self, component_name: str, value: object) -> int:
        return self.normalize(component_name, value) - 1


class OMEROAddressComponent(Enum):
    """Single declaration of every component in an OMERO virtual address."""

    WELL = OMEROWellComponentBehavior()
    SITE = OMEROPlaneCoordinateBehavior("s", 3)
    CHANNEL = OMEROPlaneCoordinateBehavior("w", 0)
    Z_INDEX = OMEROPlaneCoordinateBehavior("z", 3)
    TIMEPOINT = OMEROPlaneCoordinateBehavior("t", 3)

    @property
    def component_name(self) -> str:
        return self.name.lower()

    @property
    def is_plane_coordinate(self) -> bool:
        return self.value.is_plane_coordinate

    def normalize(self, value: object) -> OMEROAddressValue:
        return self.value.normalize(self.component_name, value)

    def filename_pattern(self) -> str:
        return self.value.filename_pattern(self.component_name)

    def filename_fragment(self, value: object) -> str:
        return self.value.filename_fragment(self.component_name, value)

    def captured_value(self, match: re.Match[str]) -> OMEROAddressValue:
        return self.normalize(match.group(self.component_name))

    def zero_based(self, value: object) -> int:
        return self.value.zero_based(self.component_name, value)

    @classmethod
    def plane_coordinates(cls) -> tuple[OMEROAddressComponent, ...]:
        """Derive the ordered coordinate subset from member-owned behavior."""

        return tuple(component for component in cls if component.is_plane_coordinate)

    @classmethod
    def project_values(
        cls,
        source_values: Iterable[tuple[Enum, object]],
    ) -> tuple[tuple[OMEROAddressComponent, object], ...]:
        """Project another nominal enum through matching declaration names."""

        projected_by_name: dict[str, object] = {}
        for component, value in source_values:
            if not isinstance(component, Enum):
                raise TypeError("Projected OMERO components must be enum members")
            if component.name in projected_by_name:
                raise ValueError(f"Projected component {component.name!r} was bound more than once")
            projected_by_name[component.name] = value

        missing = tuple(
            component.name for component in cls if component.name not in projected_by_name
        )
        if missing:
            raise ValueError(
                "OMERO component projection lacks declared members: " + ", ".join(missing)
            )
        return tuple((component, projected_by_name[component.name]) for component in cls)


@dataclass(frozen=True, slots=True)
class OMEROWellAddress:
    """Zero-based OMERO well coordinates with a canonical plate label."""

    row_index: int
    column_index: int

    @classmethod
    def filename_pattern(cls, component_name: str) -> str:
        """Return the named-capture grammar for a canonical well label."""

        return f"(?P<{component_name}>[A-Za-z]+\\d+)"

    @classmethod
    @cache
    def label_pattern(cls) -> re.Pattern[str]:
        """Compile the canonical well-label grammar for direct parsing."""

        return re.compile(f"^{cls.filename_pattern(OMEROAddressComponent.WELL.component_name)}$")

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

        normalized = match.group(OMEROAddressComponent.WELL.component_name)
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
class OMEROPlaneAddress:
    """Canonical virtual filename identity for one OMERO image plane."""

    _component_values: tuple[tuple[OMEROAddressComponent, OMEROAddressValue], ...]
    extension: str

    def __init__(
        self,
        component_values: Iterable[tuple[OMEROAddressComponent, object]],
        *,
        extension: str = ".tif",
    ) -> None:
        supplied_values = tuple(component_values)
        if any(
            not isinstance(component, OMEROAddressComponent) for component, _ in supplied_values
        ):
            raise TypeError("OMERO addresses require OMEROAddressComponent members")
        values_by_component = dict(supplied_values)
        if len(values_by_component) != len(supplied_values):
            raise ValueError("Each OMERO address component may be bound only once")
        missing = tuple(
            component for component in OMEROAddressComponent if component not in values_by_component
        )
        if missing:
            raise ValueError(
                "OMERO plane address must bind every declared component: "
                + ", ".join(f"missing {component.component_name}" for component in missing)
            )

        normalized_values = tuple(
            (component, component.normalize(values_by_component[component]))
            for component in OMEROAddressComponent
        )
        normalized_extension = str(extension).strip()
        if not normalized_extension or any(
            separator in normalized_extension for separator in ("/", "\\")
        ):
            raise ValueError(f"Invalid OMERO plane extension: {extension!r}")
        if not normalized_extension.startswith("."):
            normalized_extension = f".{normalized_extension}"
        object.__setattr__(self, "_component_values", normalized_values)
        object.__setattr__(self, "extension", normalized_extension)

    @classmethod
    def from_component_values(
        cls,
        component_values: (
            Mapping[OMEROAddressComponent, object] | Iterable[tuple[OMEROAddressComponent, object]]
        ),
        *,
        extension: str = ".tif",
    ) -> OMEROPlaneAddress:
        """Construct an address from its single component declaration surface."""

        return cls(
            component_values.items() if isinstance(component_values, Mapping) else component_values,
            extension=extension,
        )

    @classmethod
    def from_plane_indices(
        cls,
        *,
        well: str | OMEROWellAddress,
        site: int,
        channel: int,
        z_index: int,
        timepoint: int,
        extension: str = ".tif",
    ) -> OMEROPlaneAddress:
        """Bind a one-based site and OMERO's zero-based C/Z/T indices."""

        return cls(
            (
                (OMEROAddressComponent.WELL, well),
                (OMEROAddressComponent.SITE, site),
                (OMEROAddressComponent.CHANNEL, channel + 1),
                (OMEROAddressComponent.Z_INDEX, z_index + 1),
                (OMEROAddressComponent.TIMEPOINT, timepoint + 1),
            ),
            extension=extension,
        )

    @classmethod
    def from_member_projection(
        cls,
        source_values: Iterable[tuple[Enum, object]],
        *,
        extension: str = ".tif",
    ) -> OMEROPlaneAddress:
        """Construct from another nominal component declaration."""

        return cls.from_component_values(
            OMEROAddressComponent.project_values(source_values),
            extension=extension,
        )

    @classmethod
    def from_wire_mapping(
        cls,
        component_values: Mapping[str, object],
        *,
        extension: str = ".tif",
    ) -> OMEROPlaneAddress:
        """Construct at an explicit string-keyed API boundary."""

        missing = tuple(
            component.component_name
            for component in OMEROAddressComponent
            if component.component_name not in component_values
        )
        if missing:
            raise ValueError("OMERO component mapping lacks declared fields: " + ", ".join(missing))
        return cls.from_component_values(
            (
                (component, component_values[component.component_name])
                for component in OMEROAddressComponent
            ),
            extension=extension,
        )

    def declared_values(
        self,
    ) -> tuple[tuple[OMEROAddressComponent, OMEROAddressValue], ...]:
        """Return every address value from the canonical component declaration."""

        return self._component_values

    def value_for(self, component: OMEROAddressComponent) -> OMEROAddressValue:
        """Return one value through its exact OMERO component declaration."""

        if not isinstance(component, OMEROAddressComponent):
            raise TypeError("OMERO address lookup requires OMEROAddressComponent")
        for declared_component, value in self._component_values:
            if declared_component is component:
                return value
        raise KeyError(component)

    @property
    def well(self) -> OMEROWellAddress:
        """Return the canonical well address."""

        return OMEROWellAddress.from_label(cast(str, self.value_for(OMEROAddressComponent.WELL)))

    def coordinate(self, component: OMEROAddressComponent) -> int:
        """Return one positive one-based plane coordinate."""

        if not component.is_plane_coordinate:
            raise TypeError(f"{component.component_name} is not a plane coordinate")
        return cast(int, self.value_for(component))

    def zero_based(self, component: OMEROAddressComponent) -> int:
        """Project one declared coordinate onto an OMERO array index."""

        return component.zero_based(self.coordinate(component))

    @classmethod
    @cache
    def filename_pattern(cls) -> re.Pattern[str]:
        """Derive the complete virtual filename grammar from its declarations."""

        return re.compile(
            "^"
            + "".join(component.filename_pattern() for component in OMEROAddressComponent)
            + r"(?:_[^.]*)?(?P<extension>(?:\.[A-Za-z0-9]+)+)$"
        )

    @classmethod
    @cache
    def image_name_pattern(cls) -> re.Pattern[str]:
        """Derive persisted OMERO image-name grammar from declared components."""

        return re.compile(
            "^"
            + OMEROAddressComponent.WELL.filename_pattern()
            + OMEROAddressComponent.SITE.filename_pattern()
            + "$"
        )

    @classmethod
    def image_name(
        cls,
        *,
        well: str | OMEROWellAddress,
        site: int | str,
    ) -> str:
        """Render the persisted OMERO image identity for one well/site."""

        return OMEROAddressComponent.WELL.filename_fragment(
            well
        ) + OMEROAddressComponent.SITE.filename_fragment(site)

    @classmethod
    def site_for_well_sample(
        cls,
        *,
        well: str | OMEROWellAddress,
        image_name: str,
        ordinal: int,
    ) -> int:
        """Resolve a well-sample site from a declared image name or OMERO order."""

        well_label = OMEROAddressComponent.WELL.normalize(well)
        match = cls.image_name_pattern().fullmatch(str(image_name))
        if match is not None and OMEROAddressComponent.WELL.captured_value(match) == well_label:
            return cast(int, OMEROAddressComponent.SITE.captured_value(match))
        return cast(int, OMEROAddressComponent.SITE.normalize(ordinal))

    @classmethod
    def from_filename(cls, filename: str) -> OMEROPlaneAddress | None:
        """Parse a canonical OMERO virtual filename."""

        basename = PurePosixPath(str(filename).replace("\\", "/")).name
        match = cls.filename_pattern().fullmatch(basename)
        if match is None:
            return None
        return cls.from_component_values(
            ((component, component.captured_value(match)) for component in OMEROAddressComponent),
            extension=match.group("extension"),
        )

    def filename(self) -> str:
        """Render the canonical OMERO virtual filename."""

        return (
            "".join(
                component.filename_fragment(value) for component, value in self.declared_values()
            )
            + self.extension
        )
