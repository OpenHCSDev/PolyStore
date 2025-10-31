Polystore Documentation
=======================

**Framework-agnostic multi-backend storage abstraction for ML and scientific computing**

Polystore provides a pluggable storage backend system with multi-framework I/O support for NumPy, PyTorch, JAX, TensorFlow, CuPy, and Zarr.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   installation
   quickstart
   api/index

Features
--------

* **Pluggable Backends**: Disk, memory, Zarr, and streaming backends with auto-registration
* **Multi-Framework I/O**: Seamless support for NumPy, PyTorch, JAX, TensorFlow, CuPy
* **Atomic Operations**: Cross-platform atomic file writes with automatic locking
* **Batch Operations**: Efficient batch loading and saving
* **Format Detection**: Automatic format detection and routing
* **Type-Safe**: Full type hints and mypy support
* **Zero Dependencies**: Core requires only NumPy (framework support is optional)

Quick Start
-----------

.. code-block:: python

   from polystore import FileManager, BackendRegistry

   # Create registry and file manager
   registry = BackendRegistry()
   fm = FileManager(registry)

   # Save data to disk
   import numpy as np
   data = np.array([[1, 2], [3, 4]])
   fm.save(data, "output.npy", backend="disk")

   # Load data back
   loaded = fm.load("output.npy", backend="disk")

Installation
------------

.. code-block:: bash

   # Base installation (NumPy only)
   pip install polystore

   # With specific frameworks
   pip install polystore[zarr]
   pip install polystore[torch]
   pip install polystore[all]

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

