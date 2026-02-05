"""
Generic base classes for streaming item handlers.

Provides type-safe protocols, generic data wrappers, and component accessors
that work with arbitrary numbers of components (not hardcoded to 3).
"""

from typing import TypeVar, Generic, Dict, Any, Protocol, runtime_checkable
from dataclasses import dataclass

T = TypeVar('T')


@dataclass(frozen=True)
class TypedData(Generic[T]):
    """
    Generic wrapper for items with metadata.

    Type parameter T specifies the concrete item type:
    - ImageDataType for images
    - ROIType for ROIs
    - PointsType for points
    - etc.

    This provides type safety while allowing arbitrary item types.
    """
    items: List[T]
    metadata: Dict[str, Any]
    source: str


class ComponentAccessor(Protocol):
    """Protocol for component metadata access (arbitrary number of components)."""

    def get_by_mode(self, mode: str) -> list:
        """
        Get all component names that have this mode (stack/slice/window).

        Args:
            mode: One of 'stack', 'slice', 'window', 'frame', etc.

        Returns:
            List of component names (not hardcoded to 3!)

        Example:
            If config has {'channel': 'stack', 'z_index': 'slice', 'well': 'window'}
            Then get_by_mode('stack') returns ['channel']
        """
        ...

    def get_value(self, item: Dict[str, Any], component_name: str) -> Any:
        """
        Get component value for an item.

        Returns:
            Value or default (0) if component not in metadata.
        """
        ...

    def collect_values(self, component_names: list) -> list[tuple]:
        """
        Collect unique values for given components across all items.

        Returns:
            Sorted list of tuples for consistent indexing.
        """
        ...


class HandlerContext(Protocol):
    """Protocol for handler context with generic component access."""

    server: Any
    window_key: str
    data: 'TypedData[Any]'
    display_config: Dict[str, Any]
    components: ComponentAccessor
    images_dir: str | None


class ItemHandler(Protocol):
    """Type-safe protocol for item handlers with automatic discovery."""

    @staticmethod
    def can_handle(data_type: str) -> bool:
        """
        Check if this handler can process the given data type.

        Args:
            data_type: The data type string (e.g., 'image', 'rois', 'points')

        Returns:
            True if this handler can process this type.
        """
        ...

    @staticmethod
    def handle(context: HandlerContext) -> None:
        """
        Process items using type-safe context object.

        Args:
            context: HandlerContext with typed data and component accessor.
        """
        ...


@dataclass(frozen=True)
class GenericComponentAccessor:
    """
    Type-safe component accessor supporting arbitrary component counts.

    Not limited to 3 dimensions - works with any number!
    """

    _display_config: Dict[str, Any]
    _items: list[Dict[str, Any]]

    def __post_init__(self):
        """Validate display config structure."""
        if 'component_modes' not in self._display_config:
            raise ValueError("Display config must have 'component_modes'")
        if 'component_order' not in self._display_config:
            raise ValueError("Display config must have 'component_order'")

    def get_by_mode(self, mode: str) -> list:
        """
        Get all component names that have the given mode.

        Args:
            mode: One of 'stack', 'slice', 'window', 'frame', etc.

        Returns:
            List of component names (not hardcoded to 3!)

        Example:
            If config has {'channel': 'stack', 'z_index': 'slice', 'well': 'window'}
            Then get_by_mode('stack') returns ['channel']
        """
        component_modes = self._display_config['component_modes']
        component_order = self._display_config['component_order']

        return [c for c in component_order if component_modes.get(c) == mode]

    def get_value(self, item: Dict[str, Any], component_name: str) -> Any:
        """
        Get component value for an item.

        Returns:
            Value or default (0) if component not in metadata.
        """
        metadata = item.get('metadata', {})
        return metadata.get(component_name, 0)

    def collect_values(self, component_names: list) -> list[tuple]:
        """
        Collect unique values for given components across all items.

        Returns:
            Sorted list of tuples for consistent indexing.
        """
        values_set = set()
        for item in self._items:
            metadata = item.get('metadata', {})
            value_tuple = tuple(
                self.get_value(item, comp) for comp in component_names
            )
            values_set.add(value_tuple)

        return sorted(values_set)


@dataclass(frozen=True)
class SimpleHandlerContext(HandlerContext):
    """Concrete implementation of HandlerContext protocol."""

    server: Any
    window_key: str
    data: 'TypedData[Any]'
    display_config: Dict[str, Any]
    components: GenericComponentAccessor
    images_dir: str | None = None
