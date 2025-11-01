"""
Registry support for polystore.

Uses metaclass-registry package if available, otherwise provides a simple fallback.
"""

try:
    # Try to import from external metaclass-registry package
    from metaclass_registry import AutoRegisterMeta
except ImportError:
    # Fallback to simple local implementation
    from abc import ABCMeta
    from typing import Any, Dict, Type

    class AutoRegisterMeta(ABCMeta):
        """
        Simple metaclass for automatic class registration.
        
        This is a fallback implementation used when metaclass-registry package
        is not installed. For full features, install:
            pip install git+https://github.com/trissim/metaclass-registry.git@main
        
        Classes using this metaclass can specify a registry key via __registry_key__.
        The registry is stored in __registry__ on the base class.
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


__all__ = ['AutoRegisterMeta']
