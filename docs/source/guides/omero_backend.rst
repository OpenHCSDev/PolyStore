OMERO local backend
===================

``OMEROLocalBackend`` projects an OMERO plate as PolyStore virtual paths for
server-side reads and writes. It is an optional, application-constructed backend
and is never part of the default ``FileManager`` registry.

Requirements and registration
-----------------------------

Install ``omero-py`` in the deployment environment. The host application creates
the OMERO gateway, constructs ``OMEROLocalBackend``, and places that instance in
the explicit registry passed to ``FileManager`` under ``"omero_local"``.
Credentials and connection lifecycle remain host/deployment responsibilities.

Virtual source model
--------------------

Paths use the form ``/omero/plate_<id>/...``. The backend queries the plate once
to build a lightweight well/site/channel/Z/time structure and generates virtual
image filenames. Plate annotations under the configured namespace identify the
filename parser and microscope type expected by the source projection.

The live Ice gateway is not pickled. The backend records connection parameters
and worker processes reconnect when needed; deployments must provide the worker
credential environment explicitly.

Outputs
-------

The backend's ``save()`` surface supports image materialization and writes for
ROIs, tables, JSON/CSV/text annotations, and provenance. Generic artifact
materializers obtain backend-owned arguments through the released
``DataSink.contextual_save_kwargs()`` hook. For OMERO, the image workspace
identifies a base plate; ``OMEROLocalBackend`` loads that plate's authoritative
``PlateStructure`` when needed and projects ``images_dir``, ``parser_name``, and
``microscope_type`` for ``save_batch()``. Callers do not inspect OMERO metadata
or reconstruct those arguments themselves.

OMERO addresses remain virtual POSIX paths on every host. The OMERO path parser
normalizes separators through ``PurePosixPath`` before extracting the base plate
and derived output name, so a Windows host cannot rewrite virtual identity into
host-path syntax.

Current limitation
------------------

The present parser-loading path imports the OpenHCS ``FilenameParser`` registry.
That is a transitional host coupling, so ``OMEROLocalBackend`` is not yet a
standalone generic PolyStore extension despite living in this package. Do not
copy that dependency into other backends. The owning fix is to inject a nominal
parser/source projection protocol at construction time.

OMERO deployment and application workflows belong to ``omero_openhcs``;
OpenHCS pipeline/source integration is documented in the OpenHCS integration
guide.
