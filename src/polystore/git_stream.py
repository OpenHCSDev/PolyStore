"""A git branch written through one long-lived git process.

``GitBackend`` starts git for every operation: about a dozen processes to stage a few files and
checkpoint them. ``GitStreamBackend`` keeps its staged tree in memory, names every blob and tree
itself (a git id is a hash of the content), and writes checkpoints down one ``git fast-import``
that stays open while any branch of the repository is open here. A checkpoint starts no
process; a ``flush`` of fewer than a hundred objects starts one.

What the base promises when ``checkpoint`` returns, this member promises at ``flush``: the
checkpoint is on disk, and the branch moved only from the head this instance saw. A branch
another writer moved is a conflict raised by the checkpoint that can see it, or by the flush
that git refuses it at. Reading anything this instance has not staged flushes first. It holds
plain files only, and a ref that holds anything else is refused when it is opened.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
import threading
import time
import weakref
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
    """The id git gives the tree of ``entries``: each plain file's path, relative to the tree,
    and its blob id."""
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
    """One ``git fast-import`` on a repository, shared by every stream branch of it this
    process has open, and closed when the last of them goes."""

    #: The open stream of each repository, per process: a forked child opens its own.
    open_streams: ClassVar[dict[tuple[int, Path], GitStream]] = {}
    opening: ClassVar[threading.Lock] = threading.Lock()

    def __init__(self, backend: GitStreamBackend, key: tuple[int, Path]) -> None:
        self.key = key
        self.turn = threading.RLock()
        self.branches = 0
        self.unflushed = False
        self.marks = 0
        #: Each branch's newest commit made here and the mark git knows it by: a commit git
        #: has only in the stream is named to it by mark, one on disk by its id.
        self.tips: dict[str, tuple[str, int]] = {}
        environment = backend._environment(bound=True)
        self.committer = (f"{environment['GIT_COMMITTER_NAME']} "
                          f"<{environment['GIT_COMMITTER_EMAIL']}>")
        # What git says of a branch it would not move goes to a file read at every flush: a
        # pipe nobody drains is read by nobody, and blocks git once it fills.
        said = backend.repository / "polystore-stream"
        said.mkdir(exist_ok=True)
        handle, self.said = tempfile.mkstemp(dir=said, suffix=".err")
        self.heard = 0
        self.process = subprocess.Popen(  # noqa: S603 - git, with this backend's own arguments
            ["git", f"--git-dir={backend.repository}", "fast-import", "--quiet", "--done"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=handle, env=environment,
        )
        os.close(handle)

    @classmethod
    def joined(cls, backend: GitStreamBackend) -> GitStream:
        """The repository's open stream, with one more branch on it."""
        key = (os.getpid(), backend.repository.resolve())
        with cls.opening:
            if key not in cls.open_streams:
                cls.open_streams[key] = cls(backend, key)
            stream = cls.open_streams[key]
            stream.branches += 1
            return stream

    @classmethod
    def settle(cls, repository: Path) -> None:
        """Put on disk whatever this process has streamed to ``repository``."""
        stream = cls.open_streams.get((os.getpid(), repository.resolve()))
        if stream is not None:
            stream.flush()

    def left(self) -> None:
        """One branch fewer; the last one closes the stream."""
        with self.opening:
            self.branches -= 1
            if self.branches:
                return
        self.close()

    def refused(self) -> str:
        """What git has said since it was last asked: it is quiet unless something is wrong."""
        with open(self.said, "rb") as said:
            said.seek(self.heard)
            text = said.read()
        self.heard += len(text)
        return text.decode(errors="replace").strip()

    def told(self, text: bytes, answers: int) -> list[str]:
        """Write ``text`` to git and read the ``answers`` lines git answers it with."""
        try:
            self.process.stdin.write(text)
            self.process.stdin.flush()
            lines = [self.process.stdout.readline().decode().rstrip("\n") for _ in range(answers)]
        except BrokenPipeError:
            lines = [""]
        if not all(lines):
            self.process.wait()
            raise StorageResolutionError(f"git fast-import stopped: {self.ended()}")
        return lines

    def commit(self, ref: str, parent: str | None, tree: str, message: str,
               fresh: dict[str, bytes], removed: list[str], staged: dict[str, str],
               timestamp: int) -> str:
        """One checkpoint: the blobs git has not seen, then the commit that removes and stages
        paths on top of ``parent``. Answers the commit's id. A branch this stream has moved
        past ``parent`` is another writer's, and a tree git names otherwise than ``tree`` is
        a tree this member cannot name: both raise."""
        with self.turn:
            tip, mark = self.tips.get(ref, (parent, 0))
            if parent != tip:
                raise StorageResolutionError(
                    f"{ref} moved to {tip} since this branch was at {parent}: "
                    "another writer's checkpoint is not overwritten")
            self.marks += 1
            said = message.encode()
            lines = [b"".join((b"blob\ndata %d\n" % len(data), data, b"\n"))
                     for data in fresh.values()]
            lines.append(
                f"commit {ref}\nmark :{self.marks}\n"
                f"committer {self.committer} {timestamp} +0000\n".encode()
                + b"data %d\n" % len(said) + said + b"\n")
            lines.append(b"" if parent is None
                         else f"from {f':{mark}' if mark else parent}\n".encode())
            lines.extend(f"D {_quoted(path)}\n".encode() for path in removed)
            lines.extend(f"M {_FILE_MODE} {blob} {_quoted(path)}\n".encode()
                         for path, blob in staged.items())
            self.unflushed = True
            commit, root = self.told(
                b"".join(lines) + f'\nget-mark :{self.marks}\nls :{self.marks} ""\n'.encode(), 2)
            named = root.split("\t")[0].split()[-1]
            if named != tree:
                raise StorageResolutionError(
                    f"git names the tree of {commit} {named}; it was computed as {tree}")
            self.tips[ref] = (commit, self.marks)
            return commit

    def flush(self) -> None:
        """Have git put every checkpoint so far on disk and move the branches to them. A
        branch git would not move, because another writer moved it first, raises."""
        with self.turn:
            if self.unflushed:
                self.unflushed = False
                self.told(b"checkpoint\nprogress flushed\n", 1)
            refused = self.refused()
            if refused:
                raise StorageResolutionError(f"git did not take a checkpoint: {refused}")

    def ended(self) -> str:
        """This stream is over and no longer the repository's, so the next branch opens a new
        one. Answers git's last words."""
        with self.opening:
            if self.open_streams.get(self.key) is self:
                del self.open_streams[self.key]
        refused = self.refused()
        os.unlink(self.said)
        return refused

    def close(self) -> None:
        """Finish the stream: what it holds goes to disk, and git's last word is heard. A
        stream that already stopped has said its last."""
        with self.turn:
            if self.process.poll() is not None:
                return
            self.process.communicate(b"done\n")
            refused = self.ended()
            if self.process.returncode or refused:
                raise StorageResolutionError(
                    f"git fast-import ended with {self.process.returncode}: {refused}")


class GitStreamBackend(GitBackend):
    """A branch whose checkpoints are written through the repository's one open git."""

    _backend_type = Backend.GIT_STREAM.value

    def _open_staging(self) -> None:
        """The staged tree is kept here: every path and its blob id, and the bytes of each
        blob git has not been given yet."""
        self._tree: dict[str, str] = {}
        self._recorded: dict[str, str] = {}
        self._fresh: dict[str, bytes] = {}
        self._stream = GitStream.joined(self)
        self._finalizer = weakref.finalize(self, self._stream.left)

    def _staging_environment(self) -> dict[str, str]:
        return {}

    def _run(self, *arguments: str, input: bytes | None = None, bound: bool = True) -> bytes:
        """A git started beside the stream reads what is on disk, so the stream is put there
        first."""
        GitStream.settle(self.repository)
        return super()._run(*arguments, input=input, bound=bound)

    def _stage_tree_at(self, ref: str | None) -> None:
        rows = self._rows(ref) if ref else []
        other = sorted(path for mode, _blob, path in rows if mode != _FILE_MODE)
        if other:
            raise StorageResolutionError(
                f"{ref} holds more than plain files ({', '.join(other[:3])}): "
                "a stream branch names its own trees and knows only plain files")
        self._tree = {path: blob for _mode, blob, path in rows}
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
        identical to the head's records nothing and returns the head. The branch moves only
        from the head this instance saw, on disk at ``flush``."""
        tree = tree_id(self._tree)
        parent = self._base
        staged = {path: blob for path, blob in self._tree.items() if self._recorded.get(path) != blob}
        fresh = {blob: self._fresh[blob] for blob in set(staged.values()) if blob in self._fresh}
        self._fresh.clear()
        if parent is not None and parent.tree == tree:
            return parent
        timestamp = int(time.time())
        commit = self._stream.commit(
            self.ref, parent.id if parent else None, tree, message, fresh,
            [path for path in self._recorded if path not in self._tree], staged, timestamp)
        self._recorded = dict(self._tree)
        self._base = GitCommit(commit, tree, timestamp, message)
        return self._base

    def flush(self) -> None:
        """Put this repository's streamed checkpoints on disk."""
        self._stream.flush()

    def tree_id(self, directory: str | Path = "") -> str:
        base = self._path(directory, directory=True)
        self.is_dir(base)
        prefix = f"{base}/" if base else ""
        return tree_id({path[len(prefix):]: blob for path, blob in self._under(self._tree, base).items()})
