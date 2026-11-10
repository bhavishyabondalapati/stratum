"""Write-ahead log (WAL).

Every write is appended here *before* it goes into the memtable, so the
memtable can be rebuilt after a crash.

Record layout (little-endian):

    crc32   u32   checksum of the payload
    length  u32   payload length in bytes
    payload       op (u8: 1 = put, 2 = delete) | key_len (u32) | key | value

Recovery reads records until the first one that is incomplete or fails its
checksum. A crash can only tear the *last* record (appends are sequential),
so everything from that point on is cut off.
"""

import os
import struct
import zlib

from .util import fsync_dir, write_all

_HEADER = struct.Struct("<II")  # crc32, payload length
_PAYLOAD_HEAD = struct.Struct("<BI")  # op, key length
OP_PUT = 1
OP_DELETE = 2


def encode_record(key, value):
    """Encode one put (or a delete when ``value`` is None)."""
    if value is None:
        payload = _PAYLOAD_HEAD.pack(OP_DELETE, len(key)) + key
    else:
        payload = _PAYLOAD_HEAD.pack(OP_PUT, len(key)) + key + value
    return _HEADER.pack(zlib.crc32(payload), len(payload)) + payload


class WAL:
    """Append-only log writer.

    Each record goes straight to the OS with ``os.write`` (no Python-side
    buffering), so once ``append`` returns the record survives a process
    crash. With ``sync=True`` it is also fsync'd, to survive an OS crash or
    power loss.
    """

    def __init__(self, path, sync=True):
        self.path = path
        self._sync = sync
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        if sync:
            fsync_dir(os.path.dirname(path) or ".")

    def append(self, key, value):
        write_all(self._fd, encode_record(key, value))
        if self._sync:
            os.fsync(self._fd)

    def close(self):
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None


def read_records(path):
    """Read every valid record from the start of a WAL file.

    Returns ``(records, valid_bytes, file_bytes)``. ``records`` is a list of
    ``(key, value)`` with ``value=None`` for deletes; ``valid_bytes`` is
    where the valid prefix ends.
    """
    with open(path, "rb") as f:
        data = f.read()
    total = len(data)
    head = _PAYLOAD_HEAD.size
    records = []
    pos = 0
    while pos + _HEADER.size <= total:
        crc, length = _HEADER.unpack_from(data, pos)
        end = pos + _HEADER.size + length
        # length < head also rejects an all-zero region, whose "crc of an
        # empty payload" would otherwise look valid.
        if length < head or end > total:
            break
        payload = data[pos + _HEADER.size:end]
        if zlib.crc32(payload) != crc:
            break
        op, key_len = _PAYLOAD_HEAD.unpack_from(payload)
        key_end = head + key_len
        if op == OP_PUT and key_end <= length:
            records.append((payload[head:key_end], payload[key_end:]))
        elif op == OP_DELETE and key_end == length:
            records.append((payload[head:key_end], None))
        else:
            break
        pos = end
    return records, pos, total


def recover(path):
    """Replay a WAL and truncate any torn or corrupt tail. Returns records."""
    records, valid, total = read_records(path)
    if valid < total:
        os.truncate(path, valid)
    return records
