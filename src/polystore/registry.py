"""
Metaclass-based auto-registration system for storage backends.

This module provides a generic metaclass that automatically registers
classes with a specified registry key attribute.
"""

from abc import ABCMeta
from typing import Any, Dict, Type


class AutoRegisterMeta(ABCMeta):
    """
    Metaclass that automatically registers classes in a class-level registry.

    Classes using this metaclass can specify a registry key via a class attribute
    defined by __registry_key__. The registry is stored in __registry__ on the base class.

    Example:
        class Base(metaclass=AutoRegisterMeta):
            __registry_key__ = '_backend_type'

        class Derived(Base):
            _backend_type = 'my_backend'

        # Derived is now in Base.__registry__['my_backend']
    """

    def __new__(mcs, name: str, bases: tuple, namespace: Dict[str, Any]) -> Type:
        """Create a new class and register it if it has a registry key."""
        cls = super().__new__(mcs, name, bases, namespace)

        # Find the base class that defines the registry key
        registry_base = None
        registry_key_attr = None

        # Check if this class or any of its bases define __registry_key__
        for base_cls in [cls] + list(cls.__mro__[1:]):
            if hasattr(base_cls, '__registry_key__') and not base_cls.__name__.startswith('ABC'):
                if not registry_base or registry_key_attr is None:
                    registry_base = base_cls
                    registry_key_attr = base_cls.__registry_key__
                    break

        # Initialize registry on the registry base if needed
        if registry_base and not hasattr(registry_base, '__registry__'):
            registry_base.__registry__ = {}

        # Register this class if it has a registry key value
        if registry_base and registry_key_attr:
            if hasattr(cls, registry_key_attr):
                key_value = getattr(cls, registry_key_attr)
                if key_value is not None:
                    registry_base.__registry__[key_value] = cls

        return cls
