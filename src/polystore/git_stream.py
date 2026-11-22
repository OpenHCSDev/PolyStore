"""A git branch written through one long-lived git process.

``GitBackend`` starts git for every operation: about a dozen processes to stage a few files and
checkpoint them. ``GitStreamBackend`` keeps its staged tree in memory, names every blob and tree
itself (a git id is a hash of the content), and writes checkpoints down one ``git fast-import``
that stays open for the repository. A checkpoint costs no process.

What that changes for a caller: a checkpoint is in git's hands when ``checkpoint`` returns and
on disk, where another process reads it, at ``flush``. Reading anything this instance has not
staged flushes first. The branch has this one writer: another writer's checkpoint in between
is not detected here.
"""

from __future__ import annotations

import atexit
import hashlib
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, ClassVar

from polystore.constants import Backend
from polystore.exceptions import StorageResolutionError
from polystore.git import _FILE_MODE, GitBackend, GitCommit

_TREE_MODE = "40000"


def blob_id(data: bytes) -> str:
    """The id git gives these bytes as a file."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()  # noqa: S324 - git's id


def tree_id(entries: dict[str, str]) -> str:
    """The id git gives the tree of ``entries``: each path, relative to the tree, and its
    blob id."""
    files: dict[str, str] = {}
    folders: dict[str, dict[str, str]] = {}
    for path, blob in entries.items():
        name, _, rest = path.partition("/")
        if rest:
            folders.setdefault(name, {})[rest] = blob
        else:
            files[name] = blob
    # Git orders a tree's entries by name, a directory's name ending in a slash.
    rows = sorted(
        [(name.encode(), _FILE_MODE, blob) for name, blob in files.items()]
        + [(f"{name}/".encode(), _TREE_MODE, tree_id(under)) for name, under in folders.items()]
    )
    body = b"".join(mode.encode() + b" " + name.rstrip(b"/") + b"\0" + bytes.fromhex(identity)
                    for name, mode, identity in rows)
    return hashlib.sha1(b"tree %d\0" % len(body) + body).hexdigest()  # noqa: S324 - git's id


def _quoted(path: str) -> str:
    """A path as fast-import reads it: as it is, or C-quoted when it could be misread."""
    if not path.startswith('"') and "\n" not in path:
        return path
    return '"' + path.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n") + '"'


class GitStream:
    """One ``git fast-import`` on a repository, shared by every stream branch of it in this
    process and open until the process ends."""

    open_streams: ClassVar[dict[Path, GitStream]] = {}
    opening: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, backend: GitStreamBackend) -> None:
        self.turn = threading.RLock()
        self.unflushed = False
        self.marks = 0
        #: Each branch's newest commit made here and the mark git knows it by: a commit git
        #: has only in the stream is named to it by mark, one on disk by its id.
        self.tips: dict[str, tuple[str, int]] = {}
        self.process = subprocess.Popen(  # noqa: S603 - git, with this backend's own arguments
            ["git", f"--git-dir={backend.repository}", "fast-import", "--quiet", "--done"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=backend._environment(bound=True),
        )
        atexit.register(self.close)

    @classmethod
    def of(cls, backend: GitStreamBackend) -> GitStream:
        repository = backend.repository.resolve()
        with cls.opening:
            if repository not in cls.open_streams:
                cls.open_streams[repository] = cls(backend)
            return cls.open_streams[repository]

    @classmethod
    def settle(cls, repository: Path) -> None:
        """Put on disk whatever this process has streamed to ``repository``."""
        stream = cls.open_streams.get(repository.resolve())
        if stream is not None:
            stream.flush()

    def told(self, text: bytes, answered: bool = False) -> str:
        """Write ``text`` to git and, for a command git answers, read its one line."""
        try:
            self.process.stdin.write(text)
            if not answered:
                return ""
            self.process.stdin.flush()
            answer = self.process.stdout.readline()
        except BrokenPipeError:
            answer = b""
        if answered and not answer:
            raise StorageResolutionError(
                "git fast-import stopped: "
                + self.process.stderr.read().decode(errors="replace").strip())
        return answer.decode().strip()

    def commit(self, ref: str, parent: str | None, message: str, fresh: dict[str, bytes],
               removed: list[str], staged: dict[str, str], timestamp: int) -> str:
        """One checkpoint: the blobs git has not seen, then the commit that removes and stages
        paths on top of ``parent``. Answers the commit's id."""
        with self.turn:
            self.marks += 1
            lines = [b"".join((b"blob\ndata %d\n" % len(data), data, b"\n")) for data in fresh.values()]
            said = message.encode()
            lines.append(
                f"commit {ref}\nmark :{self.marks}\n"
                f"committer polystore <polystore@localhost> {timestamp} +0000\n".encode()
                + b"data %d\n" % len(said) + said + b"\n")
            tip, mark = self.tips.get(ref, (None, 0))
            lines.append(b"" if parent is None
                         else f"from {f':{mark}' if parent == tip else parent}\n".encode())
            lines.extend(f"D {_quoted(path)}\n".encode() for path in removed)
            lines.extend(f"M {_FILE_MODE} {blob} {_quoted(path)}\n".encode()
                         for path, blob in staged.items())
            self.unflushed = True
            commit = self.told(b"".join(lines) + f"\nget-mark :{self.marks}\n".encode(), answered=True)
            self.tips[ref] = (commit, self.marks)
            return commit

    def flush(self) -> None:
        """Have git put every checkpoint so far on disk and move the branches to them."""
        with self.turn:
            if self.unflushed:
                self.told(b"checkpoint\nprogress flushed\n", answered=True)
                self.unflushed = False

    def close(self) -> None:
        with self.turn:
            if self.process.poll() is None:
                self.process.communicate(b"done\n")


class GitStreamBackend(GitBackend):
    """A branch whose checkpoints are written through the repository's one open git."""

    _backend_type = Backend.GIT_STREAM.value

    def _open_staging(self) -> None:
        """The staged tree is kept here: every path and its blob id, and the bytes of each
        blob git has not been given yet."""
        self._tree: dict[str, str] = {}
        self._recorded: dict[str, str] = {}
        self._fresh: dict[str, bytes] = {}
        self._stream = GitStream.of(self)

    def _staging_environment(self) -> dict[str, str]:
        return {}

    def _run(self, *arguments: str, input: bytes | None = None, bound: bool = True) -> bytes:
        """A git started beside the stream reads what is on disk, so the stream is put there
        first."""
        GitStream.settle(self.repository)
        return super()._run(*arguments, input=input, bound=bound)

    def _stage_tree_at(self, ref: str | None) -> None:
        self._tree = super()._entries(ref) if ref else {}
        if self._base is not None and ref == self._base.id:
            self._recorded = dict(self._tree)

    def _stage_blobs(self, staged: dict[str, str]) -> None:
        self._tree.update(staged)

    def _unstage(self, paths: list[str]) -> None:
        for path in paths:
            del self._tree[path]

    def _entries(self, ref: str | None = None) -> dict[str, str]:
        return dict(self._tree) if ref is None else super()._entries(ref)

    def save_batch(self, data_list: list[Any], identifiers: list[str | Path], **kwargs) -> None:
        """Stage each of ``data_list`` at its identifier. Nothing is started and nothing is
        written: git is given the bytes with the checkpoint that records them."""
        del kwargs
        if len(data_list) != len(identifiers):
            raise ValueError("data_list and identifiers must have the same length")
        payloads = [data.encode("utf-8") if isinstance(data, str) else data for data in data_list]
        for payload in payloads:
            if not isinstance(payload, bytes):
                raise TypeError(f"GitBackend stores bytes or text, not {type(payload).__name__}")
        for identifier, payload in zip(identifiers, payloads, strict=True):
            path, blob = self._path(identifier), blob_id(payload)
            if self._tree.get(path) != blob:
                self._tree[path] = blob
                self._fresh[blob] = payload

    def load(self, file_path: str | Path, **kwargs) -> bytes:
        path = self._path(file_path)
        blob = self._entries(kwargs.get("ref")).get(path)
        if blob is None:
            raise FileNotFoundError(f"No such git file: {path}")
        return self._fresh[blob] if blob in self._fresh else self._run("cat-file", "blob", blob)

    def checkpoint(self, message: str) -> GitCommit:
        """Record the staged tree as a commit on the branch, through the open git. A tree
        identical to the head's records nothing and returns the head. On disk at ``flush``."""
        tree = tree_id(self._tree)
        parent = self._base
        if parent is not None and parent.tree == tree:
            return parent
        staged = {path: blob for path, blob in self._tree.items() if self._recorded.get(path) != blob}
        timestamp = int(time.time())
        commit = self._stream.commit(
            self.ref, parent.id if parent else None, message,
            {blob: self._fresh[blob] for blob in set(staged.values()) if blob in self._fresh},
            [path for path in self._recorded if path not in self._tree], staged, timestamp)
        self._fresh.clear()
        self._recorded = dict(self._tree)
        self._base = GitCommit(commit, tree, timestamp, message)
        return self._base

    def flush(self) -> None:
        """Put this repository's streamed checkpoints on disk."""
        GitStream.settle(self.repository)

    def tree_id(self, directory: str | Path = "") -> str:
        base = self._path(directory, directory=True)
        self.is_dir(base)
        prefix = f"{base}/" if base else ""
        return tree_id({path[len(prefix):]: blob for path, blob in self._under(self._tree, base).items()})
