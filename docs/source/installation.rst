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
