ImageJ runtime
==============

``FIJI_IMAGEJ_DISTRIBUTION`` is the package-owned Fiji archive declaration.
Its platform members own the official bundle filenames and SHA-256 digests.
``FIJI_IMAGEJ_RUNTIME`` owns the matching Java feature version and process
lifecycle.

Applications should initialize and close the gateway through the runtime
policy:

.. code-block:: python

   import imagej
   import scyjava

   from polystore import FIJI_IMAGEJ_RUNTIME

   gateway = FIJI_IMAGEJ_RUNTIME.initialize(imagej, scyjava, mode="headless")
   try:
       reader_type = scyjava.jimport("loci.formats.ImageReader")
   finally:
       FIJI_IMAGEJ_RUNTIME.shutdown(gateway, scyjava)

Public declarations
-------------------

.. autoclass:: polystore.imagej_distribution.ImageJDistributionABC

.. autoclass:: polystore.imagej_distribution.ImageJRuntimeLaunch

.. autoclass:: polystore.imagej_distribution.ImageJRuntimeArchive

.. autoclass:: polystore.imagej_distribution.ImageJArchiveDownloadPolicy

.. autoclass:: polystore.imagej_distribution.ImageJRuntimeOverlay

.. autoclass:: polystore.imagej_distribution.FijiArchiveDistribution

.. autoclass:: polystore.imagej_distribution.FijiBundleAsset

.. autoclass:: polystore.imagej_runtime.ImageJRuntimePolicy

.. autoclass:: polystore.imagej_distribution.ImageJDistributionUnavailableError

.. autoclass:: polystore.imagej_runtime.ImageJRuntimeUnavailableError
