"""A git repository as a storage backend: versioned, content-addressed, durable text.

Each backend instance is one branch of a bare repository. Writes stage into the instance's
own index; :meth:`GitBackend.checkpoint` records the staged tree as a commit and moves the
branch to it atomically, which is the durability point. Identical content is stored once
(by hash) and packed with deltas, so many revisions of a mostly unchanged document cost
about one copy plus its differences.

Everything here is git plumbing run against the repository: no working tree, no user
configuration, no hooks. The result is an ordinary repository any git tool can read.

The backend stores bytes and text. Arrays belong in an array backend (zarr); what belongs
here is the state whose history matters and the identity of anything derived from it:
:meth:`GitBackend.blob_id` and :meth:`GitBackend.tree_id` name content, so a cache keyed
by them is valid exactly while the content it was computed from is unchanged.
"""

from __future__ import annotations

import fnmatch
import os
import subprocess
import tempfile
import weakref
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .base import PicklableBackend, StorageBackend
from .constants import Backend
from .exceptions import StorageResolutionError

_FILE_MODE = "100644"
_NO_OBJECT = "0" * 40


@dataclass(frozen=True)
class GitCommit:
    """One checkpoint: its id, the tree it recorded, when, and why."""

    id: str
    tree: str
    timestamp: int
    message: str


class GitBackend(StorageBackend, PicklableBackend):
    """One branch of a bare git repository, written through its own staging index."""

    _backend_type = Backend.GIT.value
    requires_context = True

    def __init__(self, repository: str | Path, ref: str = "refs/heads/main") -> None:
        self.repository = Path(repository)
        self.ref = ref
        if not (self.repository / "HEAD").is_file():
            self.repository.mkdir(parents=True, exist_ok=True)
            self._run("init", "--bare", "--quiet", str(self.repository), bound=False)
            # A checkpoint that returned is on disk: objects and the ref are fsynced.
            self._run(f"--git-dir={self.repository}", "config", "core.fsync", "all", bound=False)
        staging = self.repository / "polystore-index"
        staging.mkdir(exist_ok=True)
        handle, index = tempfile.mkstemp(dir=staging, suffix=".index")
        os.close(handle)
        os.unlink(index)  # git creates it; a zero-byte file is not a valid index
        self._index = index
        self._finalizer = weakref.finalize(self, _discard, index)
        # The checkpoint this instance's staged tree is based on: the only head its own
        # checkpoint may replace.
        self._base = self.head()
        self._git("read-tree", self._base.id if self._base else "--empty")

    # -- git plumbing -------------------------------------------------------

    def _run(self, *arguments: str, input: bytes | None = None, bound: bool = True) -> bytes:
        environment = {
            "PATH": os.environ.get("PATH", os.defpath),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_AUTHOR_NAME": "polystore",
            "GIT_AUTHOR_EMAIL": "polystore@localhost",
            "GIT_COMMITTER_NAME": "polystore",
            "GIT_COMMITTER_EMAIL": "polystore@localhost",
        }
        command = ["git"]
        if bound:
            command.append(f"--git-dir={self.repository}")
            environment["GIT_INDEX_FILE"] = self._index
        completed = subprocess.run(
            [*command, *arguments], input=input, env=environment, capture_output=True, check=False,
        )
        if completed.returncode != 0:
            raise StorageResolutionError(
                f"git {' '.join(arguments)} failed: {completed.stderr.decode(errors='replace').strip()}"
            )
        return completed.stdout

    def _git(self, *arguments: str, input: bytes | None = None) -> str:
        return self._run(*arguments, input=input).decode().strip()

    @staticmethod
    def _path(identifier: str | Path, *, directory: bool = False) -> str:
        """The repository-relative POSIX path an identifier names; ``""`` is the root."""
        path = PurePosixPath(str(identifier))
        if path.is_absolute() or ".." in path.parts:
            raise StorageResolutionError(f"Git paths are relative to the repository root: {identifier}")
        text = "" if str(path) == "." else str(path)
        if not text and not directory:
            raise StorageResolutionError("A git file path cannot be the repository root")
        return text

    def _entries(self, ref: str | None = None) -> dict[str, str]:
        """Every file path and its blob id: the staged tree, or the tree at ``ref``."""
        if ref is None:
            listing = self._run("ls-files", "--stage", "-z").decode()
            rows = (row.split("\t", 1) for row in listing.split("\0") if row)
            return {path: meta.split()[1] for meta, path in rows}
        listing = self._run("ls-tree", "-r", "-z", ref).decode()
        rows = (row.split("\t", 1) for row in listing.split("\0") if row)
        return {path: meta.split()[2] for meta, path in rows}

    @staticmethod
    def _under(entries: dict[str, str], directory: str) -> dict[str, str]:
        prefix = f"{directory}/" if directory else ""
        return {path: blob for path, blob in entries.items() if path.startswith(prefix)}

    # -- DataSink -----------------------------------------------------------

    def accepts_payload(self, data: Any, identifier: str | Path) -> bool:
        del identifier
        return isinstance(data, (bytes, str))

    def save(self, data: Any, identifier: str | Path, **kwargs) -> None:
        """Stage ``data`` (bytes, or text as UTF-8) at ``identifier``, replacing what was there;
        the previous content stays in every checkpoint that recorded it."""
        self.save_batch([data], [identifier], **kwargs)

    def save_batch(self, data_list: list[Any], identifiers: list[str | Path], **kwargs) -> None:
        """Stage each of ``data_list`` at its identifier. Git is started twice whatever the
        number of files: once to store every blob, once to stage every path."""
        del kwargs
        if len(data_list) != len(identifiers):
            raise ValueError("data_list and identifiers must have the same length")
        payloads = [data.encode("utf-8") if isinstance(data, str) else data for data in data_list]
        for payload in payloads:
            if not isinstance(payload, bytes):
                raise TypeError(f"GitBackend stores bytes or text, not {type(payload).__name__}")
        paths = [self._path(identifier) for identifier in identifiers]
        if not paths:
            return
        with tempfile.TemporaryDirectory(dir=Path(self._index).parent) as folder:
            files = [Path(folder) / str(number) for number in range(len(payloads))]
            for file, payload in zip(files, payloads, strict=True):
                file.write_bytes(payload)
            blobs = self._git(
                "hash-object", "-w", "--stdin-paths",
                input="".join(f"{file}\n" for file in files).encode(),
            ).split()
        self._run(
            "update-index", "--add", "-z", "--index-info",
            input="".join(f"{_FILE_MODE} {blob}\t{path}\0"
                          for blob, path in zip(blobs, paths, strict=True)).encode(),
        )

    # -- DataSource ---------------------------------------------------------

    def load(self, file_path: str | Path, **kwargs) -> bytes:
        """The bytes at ``file_path``: staged, or as recorded at ``ref=``."""
        path = self._path(file_path)
        blob = self._entries(kwargs.get("ref")).get(path)
        if blob is None:
            raise FileNotFoundError(f"No such git file: {path}")
        return self._run("cat-file", "blob", blob)

    def load_text(self, file_path: str | Path, **kwargs) -> str:
        return self.load(file_path, **kwargs).decode("utf-8")

    def load_batch(self, file_paths: list[str | Path], **kwargs) -> list[bytes]:
        return [self.load(path, **kwargs) for path in file_paths]

    def list_files(
        self,
        directory: str | Path,
        pattern: str | None = None,
        extensions: set[str] | None = None,
        recursive: bool = False,
        **kwargs,
    ) -> list[str]:
        base = self._path(directory, directory=True)
        self.is_dir(base, **kwargs)
        prefix = f"{base}/" if base else ""
        files = []
        for path in self._under(self._entries(kwargs.get("ref")), base):
            relative = path[len(prefix):]
            if not recursive and "/" in relative:
                continue
            name = PurePosixPath(path).name
            if pattern is not None and not fnmatch.fnmatch(name, pattern):
                continue
            if extensions is not None and PurePosixPath(name).suffix.lower() not in extensions:
                continue
            files.append(path)
        return sorted(files)

    def is_file(self, path: str | Path, **kwargs) -> bool:
        entries = self._entries(kwargs.get("ref"))
        target = self._path(path, directory=True)
        if target in entries:
            return True
        if not target or self._under(entries, target):
            raise IsADirectoryError(f"Git path is a directory: {target or '.'}")
        raise FileNotFoundError(f"No such git path: {target}")

    def is_dir(self, path: str | Path, **kwargs) -> bool:
        entries = self._entries(kwargs.get("ref"))
        target = self._path(path, directory=True)
        if not target or self._under(entries, target):
            return True
        if target in entries:
            raise NotADirectoryError(f"Git path is a file: {target}")
        raise FileNotFoundError(f"No such git path: {target}")

    def list_dir(self, path: str | Path, **kwargs) -> list[str]:
        base = self._path(path, directory=True)
        self.is_dir(base, **kwargs)
        prefix = f"{base}/" if base else ""
        entries = self._under(self._entries(kwargs.get("ref")), base)
        return sorted({name[len(prefix):].split("/", 1)[0] for name in entries})

    # -- file operations ----------------------------------------------------

    @property
    def requires_filesystem_validation(self) -> bool:
        return False

    def ensure_directory(self, directory: str | Path) -> str | Path:
        """Git has no empty directories: one exists as soon as a file is saved under it."""
        self._path(directory, directory=True)
        return directory

    def _unstage(self, paths: list[str]) -> None:
        """Remove entries from the index. Mode 0 through --index-info is the removal that
        needs no work tree, which a bare repository does not have."""
        self._run(
            "update-index", "-z", "--index-info",
            input="".join(f"0 {_NO_OBJECT}\t{path}\0" for path in paths).encode(),
        )

    def delete(self, path: str | Path) -> None:
        self.is_file(path)
        self._unstage([self._path(path)])

    def delete_all(self, path: str | Path) -> None:
        target = self._path(path, directory=True)
        entries = self._entries()
        doomed = [target] if target in entries else list(self._under(entries, target))
        if not doomed:
            raise FileNotFoundError(f"No such git path: {target}")
        self._unstage(doomed)

    def copy(self, src: str | Path, dst: str | Path) -> None:
        """Stage ``src``'s content at ``dst``: a file, or every file under a directory."""
        source, destination = self._path(src), self._path(dst)
        entries = self._entries()
        moved = (
            {destination: entries[source]} if source in entries
            else {destination + path[len(source):]: blob
                  for path, blob in self._under(entries, source).items()}
        )
        if not moved:
            raise FileNotFoundError(f"No such git path: {source}")
        for path, blob in moved.items():
            self._git("update-index", "--add", "--cacheinfo", f"{_FILE_MODE},{blob},{path}")

    def move(self, src: str | Path, dst: str | Path) -> None:
        self.copy(src, dst)
        self.delete_all(src)

    # -- versions -----------------------------------------------------------

    def head(self) -> GitCommit | None:
        """The branch's latest checkpoint, or ``None`` before the first."""
        try:
            commit = self._git("rev-parse", "--quiet", "--verify", f"{self.ref}^{{commit}}")
        except StorageResolutionError:
            return None
        return self.history(limit=1, ref=commit)[0]

    def checkpoint(self, message: str) -> GitCommit:
        """Record the staged tree as a commit and move the branch to it. Durable on return.

        A tree identical to the head's records nothing and returns the head. The branch moves
        only from the head this instance saw: another writer's checkpoint in between is a
        conflict, raised rather than overwritten.
        """
        tree = self._git("write-tree")
        parent = self._base
        if parent is not None and parent.tree == tree:
            return parent
        lineage = ("-p", parent.id) if parent else ()
        commit = self._git("commit-tree", tree, *lineage, "-m", message)
        self._git("update-ref", self.ref, commit, parent.id if parent else _NO_OBJECT)
        self._base = self.history(limit=1, ref=commit)[0]
        return self._base

    def history(
        self, path: str | Path | None = None, *, limit: int | None = None, ref: str | None = None,
    ) -> tuple[GitCommit, ...]:
        """Checkpoints newest first; with ``path``, only those that changed it."""
        start = ref or self.ref
        try:
            self._git("rev-parse", "--quiet", "--verify", f"{start}^{{commit}}")
        except StorageResolutionError:
            return ()
        arguments = ["log", "--format=%H%x1f%T%x1f%ct%x1f%B%x1e"]
        if limit is not None:
            arguments.append(f"--max-count={limit}")
        arguments.append(start)
        if path is not None:
            arguments += ["--", self._path(path, directory=True)]
        records = (record.strip("\n") for record in self._run(*arguments).decode().split("\x1e"))
        return tuple(
            GitCommit(commit, tree, int(timestamp), message.rstrip("\n"))
            for commit, tree, timestamp, message in (
                record.split("\x1f", 3) for record in records if record
            )
        )

    def restore(self, ref: str) -> None:
        """Stage the tree recorded at ``ref``. The next checkpoint records the return as a
        new commit; history is never rewritten."""
        self._git("read-tree", ref)

    def fork(self, ref: str) -> GitBackend:
        """A new branch starting at this branch's head; staged changes are not carried."""
        head = self.head()
        if head is None:
            raise StorageResolutionError("A branch with no checkpoint cannot be forked")
        self._git("update-ref", ref, head.id, _NO_OBJECT)
        return type(self)(self.repository, ref)

    def blob_id(self, path: str | Path, **kwargs) -> str:
        """The content hash of one file: equal ids are equal bytes."""
        target = self._path(path)
        blob = self._entries(kwargs.get("ref")).get(target)
        if blob is None:
            raise FileNotFoundError(f"No such git file: {target}")
        return blob

    def tree_id(self, directory: str | Path = "") -> str:
        """The content hash of the staged files under ``directory``: equal ids are equal
        names and bytes, whatever their history."""
        base = self._path(directory, directory=True)
        self.is_dir(base)
        return self._git("write-tree", *([f"--prefix={base}/"] if base else []))

    def compact(self) -> None:
        """Pack loose objects with deltas."""
        self._git("gc", "--quiet")

    # -- PicklableBackend ---------------------------------------------------

    def get_connection_params(self) -> dict[str, Any]:
        return {"repository": str(self.repository), "ref": self.ref}

    def set_connection_params(self, params: dict[str, Any] | None) -> None:
        if params and (params["repository"], params["ref"]) != (str(self.repository), self.ref):
            raise StorageResolutionError("A git backend cannot be rebound to another branch")

    @classmethod
    def from_connection_params(cls, params: dict[str, Any] | None) -> GitBackend:
        if not params:
            raise StorageResolutionError("A git backend needs its repository and branch")
        return cls(params["repository"], params["ref"])


def _discard(index: str) -> None:
    for path in (index, f"{index}.lock"):
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
