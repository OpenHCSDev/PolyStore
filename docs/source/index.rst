PolyStore documentation
=======================

PolyStore provides nominal storage backends, execution-local ``FileManager``
routing, formats, ROI values, virtual-workspace references, and streaming
payload mechanics for scientific applications.

.. toctree::
   :maxdepth: 2

   installation
   quickstart
   architecture/index
   api/index
   guides/custom_backends
   guides/omero_backend

Ownership rules
---------------

- ``BackendBase.__registry__`` / ``STORAGE_BACKENDS`` is the backend class
  catalog.
- A ``FileManager`` receives an explicit mapping of names to backend instances.
- FileManager operations always receive a backend; path names do not select one.
- Context-specific backends are constructed and registered by the application.
- Applications own domain artifacts and materialization policy.

Requirements
------------

PolyStore requires Python 3.11 or newer. Its declared core dependencies include
NumPy, ArrayBridge, metaclass-registry, portalocker, imageio, Zarr, and
OME-Zarr. Optional extras add array frameworks and viewer transport runtimes.
