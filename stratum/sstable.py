"""SSTable: an immutable, sorted file of key/value entries.

File layout:

    +--------------------+
    | data block 0       |  sorted entries, ~block_size bytes, + crc32
    | data block 1       |
    | ...                |
    +--------------------+
    | bloom filter block |  "is this key possibly in this file?"  + crc32
    +--------------------+
    | index block        |  first key + offset + size of every data block,
    |                    |  and the largest key in the file       + crc32
    +--------------------+
    | footer (52 bytes)  |  where the bloom and index blocks live, entry
    |                    |  count, crc32, magic number
    +--------------------+

Entry inside a data block:  key_len u32 | value_len u32 | key | value
A value_len of 0xFFFFFFFF marks a tombstone (deleted key, no value bytes).

The index is "sparse": one entry per block, not per key. To find a key we
binary-search the index for the one block that could hold it, read that
block, and binary-search inside it.
"""

import os
import struct
import threading
import zlib
from array import array
from bisect import bisect_left, bisect_right
from collections import OrderedDict

from .bloom import BloomFilter, hash_pair
from .util import MISSING, CorruptionError, fsync_dir

MAGIC = 0x314D555441525453  # the bytes "STRATUM1" read as a little-endian u64
TOMBSTONE_LEN = 0xFFFFFFFF

_ENTRY = struct.Struct("<II")  # key_len, value_len
_INDEX_ENTRY = struct.Struct("<IQI")  # first_key_len, block offset, block size
_U32 = struct.Struct("<I")
# bloom_offset, bloom_size, index_offset, index_size, num_entries, crc, magic
_FOOTER = struct.Struct("<QQQQQIQ")
_FOOTER_BODY = struct.Struct("<QQQQQ")


class BlockCache:
    """A small LRU cache of decoded data blocks, shared by all SSTables."""

    def __init__(self, capacity):
        self.capacity = capacity
        self._blocks = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key):
        with self._lock:
            block = self._blocks.get(key)
            if block is None:
                self.misses += 1
                return None
            self._blocks.move_to_end(key)
            self.hits += 1
            return block

    def put(self, key, block):
        if self.capacity <= 0:
            return
        with self._lock:
            self._blocks[key] = block
            self._blocks.move_to_end(key)
            if len(self._blocks) > self.capacity:
                self._blocks.popitem(last=False)

    def __len__(self):
        return len(self._blocks)


class SSTableWriter:
    """Builds one SSTable. Keys must be added in strictly increasing order."""

    def __init__(self, path, block_size=4096, bloom_bits_per_key=10):
        self.path = path
        self._block_size = block_size
        self._bits_per_key = bloom_bits_per_key
        self._file = open(path, "wb", buffering=1 << 20)
        self._block = bytearray()
        self._block_first_key = None
        self._index = []  # (first_key, offset, size) per data block
        self._hashes = array("Q")  # h1, h2 per key; bloom is built at the end
        self._offset = 0
        self._last_key = None
        self.num_entries = 0

    def add(self, key, value):
        if self._last_key is not None and key <= self._last_key:
            raise ValueError("keys must be added in strictly increasing order")
        block = self._block
        if not block:
            self._block_first_key = key
        if value is None:
            block += _ENTRY.pack(len(key), TOMBSTONE_LEN)
            block += key
        else:
            block += _ENTRY.pack(len(key), len(value))
            block += key
            block += value
        self._hashes.extend(hash_pair(key))
        self._last_key = key
        self.num_entries += 1
        if len(block) >= self._block_size:
            self._flush_block()

    def _write(self, payload):
        """Write payload + crc32; return (offset, size_including_crc)."""
        data = bytes(payload) + _U32.pack(zlib.crc32(payload))
        offset = self._offset
        self._file.write(data)
        self._offset += len(data)
        return offset, len(data)

    def _flush_block(self):
        if not self._block:
            return
        offset, size = self._write(self._block)
        self._index.append((self._block_first_key, offset, size))
        self._block = bytearray()

    def finish(self):
        """Write bloom filter, index and footer; fsync; close."""
        self._flush_block()

        bloom = BloomFilter.for_capacity(self.num_entries, self._bits_per_key)
        hashes = self._hashes
        for i in range(0, len(hashes), 2):
            bloom.add_hashes(hashes[i], hashes[i + 1])
        bloom_offset, bloom_size = self._write(bloom.to_bytes())

        parts = [_U32.pack(len(self._index))]
        for first_key, offset, size in self._index:
            parts.append(_INDEX_ENTRY.pack(len(first_key), offset, size))
            parts.append(first_key)
        last_key = self._last_key if self._last_key is not None else b""
        parts.append(_U32.pack(len(last_key)))
        parts.append(last_key)
        index_offset, index_size = self._write(b"".join(parts))

        body = _FOOTER_BODY.pack(
            bloom_offset, bloom_size, index_offset, index_size, self.num_entries
        )
        self._file.write(body + _U32.pack(zlib.crc32(body)) + struct.pack("<Q", MAGIC))
        self._file.flush()
        os.fsync(self._file.fileno())
        self._file.close()
        return self.num_entries

    def abandon(self):
        self._file.close()
        try:
            os.remove(self.path)
        except FileNotFoundError:
            pass


def build_table(path, items, block_size=4096, bloom_bits_per_key=10):
    """Write sorted ``(key, value)`` pairs to ``path`` crash-safely.

    The data goes to ``path + ".tmp"`` first and is renamed into place only
    once it is complete and fsync'd, so a half-written table is never seen
    under its real name. Returns the number of entries; if there were none,
    no file is created.
    """
    tmp = path + ".tmp"
    writer = SSTableWriter(tmp, block_size, bloom_bits_per_key)
    try:
        for key, value in items:
            writer.add(key, value)
    except BaseException:
        writer.abandon()
        raise
    if writer.num_entries == 0:
        writer.abandon()
        return 0
    count = writer.finish()
    os.replace(tmp, path)
    fsync_dir(os.path.dirname(path) or ".")
    return count


def _decode_block(raw, end):
    """Decode the entries in raw[:end] into parallel key and value lists."""
    keys = []
    values = []
    unpack = _ENTRY.unpack_from
    pos = 0
    try:
        while pos < end:
            key_len, value_len = unpack(raw, pos)
            pos += 8
            keys.append(raw[pos:pos + key_len])
            pos += key_len
            if value_len == TOMBSTONE_LEN:
                values.append(None)
            else:
                values.append(raw[pos:pos + value_len])
                pos += value_len
    except struct.error as exc:
        raise CorruptionError("malformed data block") from exc
    if pos != end:
        raise CorruptionError("malformed data block")
    return keys, values


class SSTable:
    """Read-only view of one SSTable file.

    The index and Bloom filter are loaded into memory when the table is
    opened; data blocks are read on demand with ``os.pread`` (which is safe
    to call from many threads at once because it does not move a shared
    file position).
    """

    def __init__(self, path, file_number=0, cache=None):
        self.path = path
        self.file_number = file_number
        self._cache = cache
        self._fd = os.open(path, os.O_RDONLY)
        # statistics, used by the benchmarks
        self.bloom_checks = 0
        self.bloom_rejects = 0
        self.false_positives = 0
        try:
            self._load_metadata()
        except BaseException:
            self.close()
            raise

    def _read_checked(self, offset, size, what):
        raw = os.pread(self._fd, size, offset)
        if len(raw) != size or size < 4:
            raise CorruptionError(f"{self.path}: truncated {what}")
        if zlib.crc32(raw[:-4]) != _U32.unpack_from(raw, size - 4)[0]:
            raise CorruptionError(f"{self.path}: checksum mismatch in {what}")
        return raw

    def _load_metadata(self):
        self.size = os.fstat(self._fd).st_size
        if self.size < _FOOTER.size:
            raise CorruptionError(f"{self.path}: file too small")
        footer = os.pread(self._fd, _FOOTER.size, self.size - _FOOTER.size)
        (bloom_offset, bloom_size, index_offset, index_size,
         self.num_entries, crc, magic) = _FOOTER.unpack(footer)
        if magic != MAGIC:
            raise CorruptionError(f"{self.path}: bad magic number")
        if zlib.crc32(footer[:_FOOTER_BODY.size]) != crc:
            raise CorruptionError(f"{self.path}: footer checksum mismatch")

        raw = self._read_checked(bloom_offset, bloom_size, "bloom filter")
        self._bloom = BloomFilter.from_bytes(raw[:-4])

        raw = self._read_checked(index_offset, index_size, "index")
        (count,) = _U32.unpack_from(raw, 0)
        pos = 4
        self._first_keys = []
        self._blocks = []  # (offset, size)
        for _ in range(count):
            key_len, offset, size = _INDEX_ENTRY.unpack_from(raw, pos)
            pos += _INDEX_ENTRY.size
            self._first_keys.append(raw[pos:pos + key_len])
            pos += key_len
            self._blocks.append((offset, size))
        (key_len,) = _U32.unpack_from(raw, pos)
        pos += 4
        self.max_key = raw[pos:pos + key_len]
        self.min_key = self._first_keys[0] if self._first_keys else b""

    def _load_block(self, index):
        offset, size = self._blocks[index]
        raw = self._read_checked(offset, size, f"data block {index}")
        return _decode_block(raw, size - 4)

    def _cached_block(self, index):
        cache = self._cache
        if cache is None:
            return self._load_block(index)
        cache_key = (self.file_number, index)
        block = cache.get(cache_key)
        if block is None:
            block = self._load_block(index)
            cache.put(cache_key, block)
        return block

    def get(self, key):
        """Returns the value, None for a tombstone, or MISSING."""
        if not self._blocks or key < self.min_key or key > self.max_key:
            return MISSING
        self.bloom_checks += 1
        if not self._bloom.might_contain(key):
            self.bloom_rejects += 1
            return MISSING
        keys, values = self._cached_block(bisect_right(self._first_keys, key) - 1)
        i = bisect_left(keys, key)
        if i < len(keys) and keys[i] == key:
            return values[i]
        self.false_positives += 1
        return MISSING

    def items(self, start=None):
        """Yield (key, value) in order from ``start``. Tombstones included.

        Sequential reads skip the block cache so that a big scan or a
        compaction does not evict the blocks that point lookups rely on.
        """
        if not self._blocks:
            return
        if start is None or start <= self.min_key:
            first_block = 0
        elif start > self.max_key:
            return
        else:
            first_block = bisect_right(self._first_keys, start) - 1
        for index in range(first_block, len(self._blocks)):
            keys, values = self._load_block(index)
            if index == first_block and start is not None:
                skip = bisect_left(keys, start)
                if skip:
                    keys = keys[skip:]
                    values = values[skip:]
            yield from zip(keys, values)

    def __iter__(self):
        return self.items()

    def close(self):
        fd, self._fd = self._fd, None
        if fd is not None:
            os.close(fd)

    def __del__(self):
        # A table that was compacted away is deleted from disk immediately,
        # but its file descriptor stays open until the last reader (e.g. a
        # scan in progress) lets go of this object.
        try:
            self.close()
        except Exception:
            pass
