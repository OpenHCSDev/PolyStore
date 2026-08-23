Installation
============

Requirements
------------

PolyStore requires Python 3.11 or newer.

.. code-block:: console

   python -m pip install polystore

The declared core includes NumPy, ArrayBridge, metaclass-registry,
portalocker, imageio, Zarr, and OME-Zarr. Optional extras add individual array
frameworks or streaming support:

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

``FIJI_IMAGEJ_RUNTIME`` declares the Fiji Maven endpoint and its compatible
managed Java runtime. Applications using PolyStore's Bio-Formats bridge or
Fiji streaming process should initialize PyImageJ through that policy before
any JVM starts. This keeps the endpoint and Java compatibility requirement in
one package-owned declaration. The Fiji declaration also owns a bounded retry
schedule for transient artifact-resolution failures. Retries stop once a JVM
has started, because a partially initialized process cannot safely change its
Java runtime.
