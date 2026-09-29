# Native ImageJ point archive repair checkpoint

Reference evidence and proposed closure, not an implemented repair.

Base and source audited: `0efe67fdd14985bf90cee6e0f0c4735d41d265d1`,
OpenHCSDev/PolyStore main. Issue: OpenHCSDev/PolyStore#12.
Dependent work: OpenHCSDev/openhcs#134 and draft OpenHCSDev/openhcs#153.
Branch: `fix/native-imagej-point-roi-20260929`.
Own worktree: `/home/ts/code/projects/polystore-point-roi-20260929`.

## Native evidence

The synthetic codec diagnostic imports only the worker's pinned dependency
sources. The original completed run used CPython 3.14.6, NumPy 2.5.1,
PolyStore 0.2.19, roifile 2026.2.10, ZMQRuntime 0.2.24, and numcodecs 0.17.0.
Three cases: polygon control passed, two point cases failed. Runtime 0.39 s,
peak RSS 53168 KiB. The canonical-metadata repeat had the same outcomes:
0.28 s, peak RSS 51808 KiB. These are native codec diagnostics, not transport,
installed OpenHCS, managed viewer or scientific acceptance.

Receipt root: `/tmp/openhcs-pr153.LeLQct`. Original completed evidence:
`scan-recovery-point-native-prerequisite-768.{json,stderr,guard}`.
Canonical repeat: `scan-recovery-point-native-canonical-768.{json,stderr,guard}`.
The earlier missing-numcodecs import failure remains separate and is not a
codec failure. The canonical comparison derives logical metadata from public
`roi_zip_metadata_payload` and JSON decoding; native coordinate checks are
independent of tuple/list wire representation.

- `PointImageJROIShapeConverter`, `src/polystore/roi_converters.py:547`, emits
  FREEHAND through `ImagejRoi.frompoints`, not POINT. The archive preserves
  exact XY `(3.5, 1.25)`, metadata label 7, object/source names, and zero-based
  `plane_indices=(2,)`. Native position/C/Z/T fields are all 0 (unset).
- The writer's integer extents top/left/bottom/right are `(1,4,2,5)` while its
  subpixel XY coordinates remain exact. Do not treat rounded extents as loss
  of the subpixel coordinates or invent an image extent.
- `load_rois_from_zip`, `src/polystore/roi.py:581`, maps non-POLYLINE shapes to
  PolygonShape. Both the production point archive and a standard synthetic
  POINT archive fail with `Polygon must have at least 3 vertices, got 1`.
- The standard POINT fixture preserves exact XY and native one-based Z=3
  (zero-based Z=2) through roifile before the PolyStore reader fails.
- `DiskStorageBackend._save_rois`, `src/polystore/disk.py:814`, owns archive
  assembly and the existing per-member metadata. The polygon control passes
  exact native XY plus logical label/object/source/plane metadata identity.

## Owner-level closure proposal

Pattern leads: IMPL-4, an existing nominal family owns encoding while decoding
is outside it; IMPL-2, external ROI enum cases are recovered in a consumer;
BOUND-2, decoding should consume the existing shape/codec authority. These
source-checked leads are not completed NRA claims.

Extend the existing nominal ImageJ geometry/codec owners. Shared archive
loading calls the owned decoding contract; leaf declarations own external
ImageJ kind acceptance and geometry construction. Compose actual encode/decode
capabilities through MI/MRO where needed. Derive any lookup from those
declarations, never maintain a second shape roster, string dispatch ladder or
OpenHCS-specific decoder. A typed carrier or relocation alone does not close
the ownership gap.

External ImageJ bytes and existing archive metadata remain the format
authority. POINT encoding uses POINT, and decoding preserves exact YX and
source/object/plane identity. Native Z/C/T projection requires declared axis
meaning; generic leading plane indices are not automatically a Z axis. Reject
ambiguous or malformed identity instead of guessing. Polygon, polyline and
ellipse behaviour remain protected by the same family contract.

Proposed production write scope: existing `src/polystore/roi.py`,
`src/polystore/roi_converters.py`, and existing native codec tests. Change
`src/polystore/disk.py` only if archive-owner closure genuinely requires it and
coordinate the extension first. No source implementation has been changed.

New-case experiment: a supported geometry declares its own external kind and
encode/decode behaviour once; neither the archive reader nor OpenHCS adds a
dispatch branch. Guards exclude duplicate codec maps, silent polygon fallback,
raw-string ladders and weakened validation. Unsupported geometry remains an
explicit failure, not an alias.

## Common audit gate and integration boundary

The common complete-context audit retains OpenHCS plus all eight recorded
dependency source roots and all 79 detectors. It is not complete. The first
compact attempt exceeded 768 MiB; the all-root compact attempt stayed bounded
but hit the 160-second deadline (`complete=false`). Required full/raw R1,
omitted-detector coverage and source-export evidence remain unavailable.
No production Python changes may begin on partial cache evidence.

After complete coverage: trace R1 raw consumers and declarations, finish the
surface receipt, simulate/apply revision-checked NRA transformations, and
label behavioural additions/native proof gaps honestly. Native tests cover
POINT/standard POINT, exact fractional YX, explicit native Z and sidecar plane
identity, label/object/source, polygon/polyline/ellipse, malformed and unsupported
types. Test shards are bounded to 60 seconds with CPU-only/offscreen settings
where applicable and owned cache roots.

This dependency worktree is independent of the common scan's pinned clone.
The OpenHCS parent gitlink stays at the recorded base. Integration requires a
validated/merged dependency PR and main's explicit recorded-SHA authorisation.
Do not merge or mark this draft ready from tracking evidence.
