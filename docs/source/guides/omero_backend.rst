OMERO local backend
===================

``OMEROLocalBackend`` projects an OMERO plate as PolyStore virtual paths for
server-side reads and writes. It is an optional, application-constructed backend
and is never part of the default ``FileManager`` registry.

Requirements and registration
-----------------------------

Install the platform's ZeroC Ice wheel first, then install
``polystore[omero]``. The host application creates the OMERO gateway, constructs
``OMEROLocalBackend``, and places that instance in the explicit registry passed
to ``FileManager`` under ``"omero_local"``. Credentials and connection lifecycle
remain host/deployment responsibilities.

Virtual source model
--------------------

Paths use the form ``/omero/plate_<id>/...``. The backend queries the plate once
to build a lightweight well/site/channel/Z/time structure and generates virtual
image filenames. ``OMEROPlaneAddress`` owns that filename grammar and
``OMEROWellAddress`` owns conversion between OMERO row/column coordinates and
labels such as ``A01`` or ``AA01``. Reading a plate does not require annotations
from a host application.

``OMEROPlaneAddress`` represents a concrete image plane and therefore requires
exact component values. ``OMEROPlaneFilenameTemplate`` represents the same
component-owned grammar at pattern-discovery boundaries, where a component may
instead contain a symbolic field such as ``{iii}``.

The live Ice gateway is not pickled. The backend records connection parameters
and worker processes reconnect when needed; deployments must provide the worker
credential environment explicitly.

Outputs
-------

The backend's ``save()`` surface supports image materialization and writes for
ROIs, tables, JSON/CSV/text annotations, and provenance. Generic artifact
materializers obtain backend-owned arguments through the released
``DataSink.contextual_save_kwargs()`` hook. For OMERO, the image workspace
identifies a base plate and ``OMEROLocalBackend`` projects the virtual
``images_dir`` required to link related artifacts. Image batches are parsed
through ``OMEROPlaneAddress``; callers do not provide a parser registry or copy
the address grammar.

``OMEROTextFormat`` is the declaration authority for supported text extensions,
MIME types, and optional table parsing. CSV, JSON, and plain-text members carry
their own parsing behaviour; the backend does not maintain separate extension,
MIME-type, and parser tables. Tabular content is created through
``OMEROTableService`` only after OMERO reports that its table service is ready
and declares a managed table repository. A service that remains unavailable or
returns no table fails at that boundary instead of being inferred from exception
text.

Image batches are normalised into ``ImagePlaneBatch`` before upload. That value
owns the two-dimensional plane constraint, dtype consistency, dimensions,
missing-plane padding, and Z/C/T iteration order used by the OMERO gateway.

OMERO addresses remain virtual POSIX paths on every host. The OMERO path parser
normalizes separators through ``PurePosixPath`` before extracting the base plate
and derived output name, so a Windows host cannot rewrite virtual identity into
host-path syntax.

Application boundary
--------------------

PolyStore owns OMERO address, storage, and persistence semantics. The
``OMEROAddressComponent`` members own well and plane-coordinate parsing,
normalization, rendering, and zero-based projection. Applications may project
the ordered nominal values exposed by an ``OMEROPlaneAddress`` into their own
component declarations, but the backend never imports those declarations. This
keeps ``OMEROLocalBackend`` usable as a standalone PolyStore extension.

OMERO deployment and application workflows belong to ``omero_openhcs``;
OpenHCS pipeline/source integration is documented in the OpenHCS integration
guide.
