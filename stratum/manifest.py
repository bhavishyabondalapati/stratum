"""The MANIFEST: the single source of truth for which files are live.

It is a tiny JSON document followed by a crc32 line:

    {"log_number": 12, "next_file_number": 14, "tables": [13, 9, 4]}
    crc32:1a2b3c4d

* ``tables``            live SSTable file numbers, NEWEST FIRST
* ``log_number``        WAL files with a smaller number are obsolete
* ``next_file_number``  the next unused file number

It is replaced atomically (write temp file, fsync, rename, fsync dir), so
after a crash we see either the old or the new version, never a mix. Any
file in the directory that the manifest does not mention is garbage left
by an interrupted flush or compaction and is deleted on startup.
"""

import json
import os
import zlib

from .util import CorruptionError, atomic_write

MANIFEST_NAME = "MANIFEST"


class Manifest:
    def __init__(self, tables=(), log_number=0, next_file_number=1):
        self.tables = list(tables)
        self.log_number = log_number
        self.next_file_number = next_file_number

    def encode(self):
        body = json.dumps(
            {
                "log_number": self.log_number,
                "next_file_number": self.next_file_number,
                "tables": self.tables,
            },
            sort_keys=True,
        ).encode()
        return body + b"\ncrc32:%08x\n" % zlib.crc32(body)

    @classmethod
    def decode(cls, data):
        try:
            body, crc_line = data.rstrip(b"\n").rsplit(b"\n", 1)
            if not crc_line.startswith(b"crc32:"):
                raise ValueError("missing checksum line")
            if int(crc_line[6:], 16) != zlib.crc32(body):
                raise ValueError("checksum mismatch")
            doc = json.loads(body)
            return cls(
                [int(n) for n in doc["tables"]],
                int(doc["log_number"]),
                int(doc["next_file_number"]),
            )
        except (ValueError, KeyError, TypeError) as exc:
            raise CorruptionError(f"corrupt MANIFEST: {exc}") from exc

    def save(self, directory):
        atomic_write(os.path.join(directory, MANIFEST_NAME), self.encode())

    @classmethod
    def load(cls, directory):
        """Load the manifest, or return None if the database is brand new."""
        try:
            with open(os.path.join(directory, MANIFEST_NAME), "rb") as f:
                return cls.decode(f.read())
        except FileNotFoundError:
            return None
