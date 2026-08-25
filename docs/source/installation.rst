Installation
============

Requirements
------------

PolyStore requires Python 3.11 or newer.

.. code-block:: console

   python -m pip install polystore

The declared core includes NumPy, ArrayBridge, metaclass-registry,
portalocker, platformdirs, imageio, Zarr, OME-Zarr, and ZMQRuntime. Optional
extras add individual array frameworks or streaming support:

.. code-block:: console

   python -m pip install "polystore[torch]"
   python -m pip install "polystore[streaming]"

Development
-----------

.. code-block:: console

   git clone https://github.com/OpenHCSDev/PolyStore.git
   cd PolyStore
   python -m venv .venv
   source .venv/bin/activate
   python -m pip install -e ".[dev,docs]"
   python -m pytest

ImageJ runtimes
---------------

Install the optional bridge with ``pip install "polystore[bioformats]"``.
``FIJI_IMAGEJ_RUNTIME`` selects a platform-specific, checksummed application
bundle from one immutable official Fiji archive and the tested PyImageJ bridge
release. On first use, PolyStore downloads and atomically extracts that bundle
into the operating system's user cache. The bundle includes its compatible
JDK, so initialization does not resolve a mutable Maven dependency graph. The
first initialization downloads several hundred megabytes and reports progress;
later processes reuse the verified cache.

Applications using PolyStore's Bio-Formats bridge or a Fiji streaming process
should initialize PyImageJ through that policy before any JVM starts. The
policy validates both the active Java feature version and the ImageJ
distribution. Process owners must close the gateway through its ``shutdown``
method so the JVM is destroyed while the Python runtime is still valid, rather
than during interpreter finalization.
