"""Small shared helpers: errors, a "missing" sentinel, and durable file writes."""

import os


class StratumError(Exception):
    """Base class for every error raised by Stratum."""


class CorruptionError(StratumError):
    """A file on disk failed a checksum or structural check."""


class _Missing:
    """Sentinel meaning "this layer knows nothing about the key".

    We cannot use ``None`` for that, because ``None`` already means
    "tombstone" (the key was deleted) inside the engine.
    """

    __slots__ = ()

    def __repr__(self):
        return "MISSING"

    def __bool__(self):
        return False


MISSING = _Missing()


def write_all(fd, data):
    """Write every byte of ``data`` to ``fd`` (os.write may write only part)."""
    written = os.write(fd, data)
    if written == len(data):
        return
    view = memoryview(data)[written:]
    while view:
        written = os.write(fd, view)
        view = view[written:]


def fsync_dir(path):
    """fsync a directory so a create/rename inside it survives power loss."""
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write(path, data):
    """Replace ``path`` with ``data`` so readers see the old or the new file,
    never a half-written one: write a temp file, fsync it, rename over the
    target, then fsync the directory."""
    tmp = path + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        write_all(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    fsync_dir(os.path.dirname(path) or ".")
