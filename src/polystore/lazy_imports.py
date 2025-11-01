"""
Lazy imports for optional GPU frameworks.

This module provides lazy imports for GPU frameworks (PyTorch, JAX, TensorFlow, CuPy)
to avoid importing heavy dependencies unless they are actually used.
"""

import os
from typing import Optional

# Module-level cache for lazy imports
_torch = None
_jax = None
_jnp = None
_cupy = None
_tf = None
_imports_attempted = False


def _attempt_imports():
    """Attempt to import GPU frameworks once."""
    global _torch, _jax, _jnp, _cupy, _tf, _imports_attempted

    if _imports_attempted:
        return

    _imports_attempted = True

    # Skip GPU libraries if running in no-GPU mode
    if os.getenv('POLYSTORE_NO_GPU') == '1':
        return

    # PyTorch
    try:
        import torch as _torch_module
        _torch = _torch_module
    except ImportError:
        pass

    # JAX
    try:
        import jax as _jax_module
        import jax.numpy as _jnp_module
        _jax = _jax_module
        _jnp = _jnp_module
    except ImportError:
        pass

    # CuPy
    try:
        import cupy as _cupy_module
        _cupy = _cupy_module
    except ImportError:
        pass

    # TensorFlow
    try:
        import tensorflow as _tf_module
        _tf = _tf_module
    except ImportError:
        pass


@property
def torch():
    """Lazy import PyTorch."""
    _attempt_imports()
    return _torch


@property
def jax():
    """Lazy import JAX."""
    _attempt_imports()
    return _jax


@property
def jnp():
    """Lazy import JAX NumPy."""
    _attempt_imports()
    return _jnp


@property
def cupy():
    """Lazy import CuPy."""
    _attempt_imports()
    return _cupy


@property
def tf():
    """Lazy import TensorFlow."""
    _attempt_imports()
    return _tf


# Simple function-based API
def get_torch():
    """Get PyTorch module if available."""
    _attempt_imports()
    return _torch


def get_jax():
    """Get JAX module if available."""
    _attempt_imports()
    return _jax


def get_jnp():
    """Get JAX NumPy module if available."""
    _attempt_imports()
    return _jnp


def get_cupy():
    """Get CuPy module if available."""
    _attempt_imports()
    return _cupy


def get_tf():
    """Get TensorFlow module if available."""
    _attempt_imports()
    return _tf
