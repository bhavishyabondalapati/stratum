"""The memtable: the in-memory, sorted buffer that absorbs recent writes.

It is a thin wrapper around the skip list that also tracks roughly how many
bytes it holds, so the engine knows when to flush it to an SSTable.
A value of ``None`` is a tombstone ("this key was deleted").
"""

from .skiplist import SkipList
from .util import MISSING

# Rough per-entry bookkeeping cost (node object, pointers, bytes headers).
ENTRY_OVERHEAD = 64


class Memtable:
    def __init__(self):
        self._list = SkipList()
        self.approx_bytes = 0

    def __len__(self):
        return len(self._list)

    def put(self, key, value):
        old = self._list.insert(key, value)
        new_len = len(value) if value is not None else 0
        if old is MISSING:
            self.approx_bytes += len(key) + new_len + ENTRY_OVERHEAD
        else:
            self.approx_bytes += new_len - (len(old) if old is not None else 0)

    def get(self, key):
        """Returns the value, None for a tombstone, or MISSING."""
        return self._list.get(key)

    def items(self, start=None):
        return self._list.items(start)
