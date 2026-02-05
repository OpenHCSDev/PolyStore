"""
Handler registry with metaclass-driven discovery.

Uses AutoRegisterMeta for automatic registration of item handlers
with lazy loading and caching support.
"""

from typing import Type, Dict
from metaclass_registry.core import AutoRegisterMeta, RegistryConfig
from metaclass_registry.lazy import LazyDiscoveryDict
from polystore.streaming.base import ItemHandler

# Handler registry dict (will be populated by AutoRegisterMeta)
_ITEM_HANDLERS: Dict[str, Type[ItemHandler]] = {}

# Configure metaclass for handler discovery
ITEM_HANDLER_REGISTRY_CONFIG = RegistryConfig(
    registry_dict=_ITEM_HANDLERS,
    key_attribute='_handler_data_type',
    key_extractor=None,
    skip_if_no_key=True,
    discovery_package='polystore.streaming.handlers',
    discovery_recursive=True,
    log_registration=True,
    registry_name='item handler'
)

# Create lazy registry for caching
_ITEM_HANDLERS_LAZY = LazyDiscoveryDict(enable_cache=True)
# Point to the lazy dict (auto-populated by metaclass)
_ITEM_HANDLERS.update(_ITEM_HANDLERS_LAZY)


class HandlerBase(ItemHandler, metaclass=AutoRegisterMeta):
    """
    Base class for all item handlers.

    Handlers auto-register when defined with metaclass.
    Just set _handler_data_type attribute.

    Example:
        class FijiImageHandler(HandlerBase):
            _handler_data_type = "image"
            # Auto-registered!
    """
    _handler_data_type: str

    # Optional: override for custom key extraction
    @classmethod
    def _extract_handler_key(cls, handler_class: Type) -> str | None:
        return getattr(handler_class, '_handler_data_type', None)
