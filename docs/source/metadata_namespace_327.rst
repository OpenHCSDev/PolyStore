Issue327: workspace-owned metadata namespace
===========================================

SOURCE-ONLY implementation owner; parent remains integration owner.
Audited dependency: PR16 at4939979978439a6f38145788df9aac3bce65bc70.
Paired application: PR206 ata3179b214b888308133e55682e964f77c2781879.
This is a dependency-stack extension, not another input-preparation implementation.

Boundary, witnesses and ownership
--------------------------------

Before: metadata_writer.py:54 declares frozen MetadataConfig, but
virtual_workspace.py:242 resolves metadata through the module-global helper.
Its connection parameters at218-238 carry only plate_root. OpenHCS's richer
OpenHCSMetadataConfig declares its own filename, paths and lock ownership in
openhcs/core/virtual_workspace_metadata.py:39-59, while openhcs/__init__.py:20
projects the application filename into the dependency's environment. A dependency
import before that projection leaves its decoded config unchanged.

BOUND-2: the consumer bypassed the richer application's existing config owner.
IMPL-13: application path/lock behavior was repeated rather than inherited.
TIME-7: bootstrap supplied another component's ambient default. No application
namespace is added to PolyStore; the application declares that specialization.

After: MetadataConfig owns metadata_path and managed_paths. VirtualWorkspaceBackend
retains the exact MetadataConfig instance, resolves through its method, and carries
the owner through the existing PicklableBackend connection-parameter contract.
FileManager's existing registry and native reconstruction remain unchanged.
Its frozen generic default remains the same object used by get_metadata_path.
Explicit custom configurations are not overwritten by another importing consumer.
OpenHCSMetadataConfig inherits shared behavior and declares only its application
filename. Parent must bind this owner at microscope backend registration.

Durable state and new-case experiment
------------------------------------

Metadata remains durable JSON at its declared path, with the existing structured
SourcePixelRef mapping. No new store, copy, rename, alternate reader or converter.
The runtime FileManager handoff carries the config object along with plate_root;
old runtime connection dictionaries are not accepted through a fallback.

Before, embedding a new namespace required coordinating an environment projection,
dependency import timing and application producer declaration. After, one frozen
MetadataConfig declaration/value is passed to the workspace constructor. The
new-case test changes filename and SUBDIRECTORIES_KEY to collections and loads
exact pixels without changing workspace/registry/consumer code. The native test
uses actual FileManager pickle reconstruction in a fresh Python process, not a
mock registry or serialization replacement.

Source evidence and guards
--------------------------

Seven focused checks pass in4.45s, peak combined process RSS298.66 MiB, one CPU.
They cover generic/custom namespace, explicit environment config with no mutation
of the generic snapshot, real persisted reopen, connection replacement and cache
refresh, absence of alternate-filename readers, source axes, exact pixels,
sample provenance and native handoff. Site-specific AST guard requires the path
call to use self.metadata_config and the handoff methods to carry that owner.
No guard exceptions or assertion weakening.

Lightweight full-package AST census at pinned HEAD completes in1.06s with12,209
code lines; it is not a global semantic audit. The original application failure
was separately reproduced unchanged in4.35s at275.77 MiB RSS. Original XML/logs
and the earlier native-extension collection error are retained in the paired
application receipt's source archive. Own native extensions were built normally
from source, not copied from the installation.

This is a hand-authored semantic patch based on the fully read NRA/refactor-audit
rules, pattern catalog and inspected owners. No NRA transaction, completed R1
raw-findings scan, native-equivalence certificate or global architecture approval
is claimed. Uncompleted heavy scans do not block these bounded source checks;
their missing proof remains missing, not waived.

Resource and acceptance boundary
-------------------------------

Own persistent source worktree: /home/ts/wt/polystore-metadata-namespace-327-20261001.
Disposable output owner: this agent; path
/home/ts/.cache/agent-scratch/metadata-namespace-327, maximum256 MiB.
Every source shard is bounded60s and512 MiB combined RSS. Resource guard reports
the owner's warning-only20 GiB disk threshold; actual8.4-8.5 GiB free and more
than15 GiB RAM available provide room for these explicit bounds.
No installed packages, parent/foreign sources, UI/MCP, scientific datasets,
science-runtime locks, Fiji/environments/interpreter downloads are changed.

Working source contract is ready for parent review. Complete microscope startup,
application-first/dependency-first entrypoints, installed native/MCP and affected
installed acceptance require parent binding and paired integration. Neither this
receipt nor passing source checks closes issue327 or certifies live readiness.
