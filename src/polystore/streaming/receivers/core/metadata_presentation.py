"""Component-blind presentation derived from routed coordinates and display names."""

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Generic, TypeVar

ComponentValueT = TypeVar("ComponentValueT")


class ComponentMetadataPresentationABC(ABC, Generic[ComponentValueT]):
    """Nominal domain hooks with shared coordinate-label presentation.

    Missing friendly names do not remove authoritative coordinate metadata.
    Domain adapters own abbreviations and named-label formatting.
    """

    @abstractmethod
    def display_name(self, component: str, value: ComponentValueT) -> str | None:
        """Resolve the existing authoritative display name, if supplied."""

    @abstractmethod
    def abbreviation(self, component: str) -> str:
        """Return the domain's display name for a declared component."""

    @abstractmethod
    def named_axis_label(self, component: str, value: ComponentValueT, name: str) -> str:
        """Format a coordinate with its domain-owned friendly name."""

    def compact_label(self, component: str, value: ComponentValueT) -> str:
        name = self.display_name(component, value)
        return name if name is not None else f"{self.abbreviation(component)} {value}"

    def axis_label(self, component: str, value: ComponentValueT) -> str:
        name = self.display_name(component, value)
        if name is None:
            return self.compact_label(component, value)
        return self.named_axis_label(component, value, name)

    def axis_labels(self, component: str, values: Sequence[ComponentValueT]) -> list[str]:
        return [self.axis_label(component, value) for value in values]

    def scalar_labels(
        self, component_values: Mapping[str, Sequence[ComponentValueT]]
    ) -> tuple[str, ...]:
        labels = []
        for component, values in component_values.items():
            if len(values) != 1:
                raise ValueError(
                    f"Collapsed component {component!r} must have one value, got {values!r}."
                )
            labels.append(self.axis_label(component, values[0]))
        return tuple(labels)
