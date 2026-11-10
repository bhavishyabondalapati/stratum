"""The Stratum storage engine: ties the WAL, memtable, SSTables, manifest
and background compaction together behind put / get / delete / scan.

Locks (always taken in this order, never the other way round):

    _write_lock       one writer at a time: WAL append + memtable insert,
                      and the memtable flush when it is full
    _compaction_lock  one compaction at a time
    _state_lock       guards changes to the set of live files (the
                      manifest) and the file-number counter

Readers take no lock at all. The current memtable and table list live in
one immutable ``_State`` tuple; flush and compaction build a new tuple and
swap it in with a single attribute assignment, so a reader always sees a
complete, consistent snapshot.
"""

import fcntl
import os
import threading
from collections import namedtuple

from .compaction import compact_tables, pick_compaction
from .iterators import merge_iterators
from .manifest import Manifest
from .memtable import Memtable
from .sstable import BlockCache, SSTable, build_table
from .util import MISSING, StratumError
from .wal import WAL, recover

# mem: the active Memtable. tables: tuple of SSTable, newest first.
_State = namedtuple("_State", "mem tables")

MAX_KEY_BYTES = 64 * 1024
MAX_VALUE_BYTES = 512 * 1024 * 1024


class _Aborted(Exception):
    """Raised inside a compaction to cancel it when the engine is closing."""


class Stratum:
    """A persistent, ordered key-value store. Keys and values are bytes.

    Parameters
    ----------
    path : directory holding the database (created if missing)
    memtable_bytes : flush the memtable to an SSTable at about this size
    sync : fsync the WAL on every write (survives power loss). With
        ``sync=False`` an acknowledged write still survives a process crash
        (it is already in the OS page cache) but not an OS crash.
    block_size : target size of an SSTable data block
    bloom_bits_per_key : Bloom filter size; 10 bits gives ~0.8% false positives
    block_cache_blocks : number of decoded blocks kept in the LRU cache
    background_compaction : run size-tiered compaction on a background thread
    compaction_min_tables / compaction_max_tables / compaction_size_ratio :
        see ``compaction.pick_compaction``
    """

    def __init__(self, path, *, memtable_bytes=4 * 1024 * 1024, sync=True,
                 block_size=4096, bloom_bits_per_key=10,
                 block_cache_blocks=4096, background_compaction=True,
                 compaction_min_tables=4, compaction_max_tables=12,
                 compaction_size_ratio=1.5):
        self.path = os.fspath(path)
        self._memtable_bytes = memtable_bytes
        self._sync = sync
        self._block_size = block_size
        self._bits_per_key = bloom_bits_per_key
        self._min_tables = compaction_min_tables
        self._max_tables = compaction_max_tables
        self._size_ratio = compaction_size_ratio
        self._cache = BlockCache(block_cache_blocks)

        self._write_lock = threading.Lock()
        self._compaction_lock = threading.Lock()
        self._state_lock = threading.Lock()
        self._wake = threading.Event()
        self._closing = False
        self._closed = False
        self._bg_error = None
        self._bg_thread = None
        self._wal = None
        self._old_wal_paths = []
        self.flush_count = 0
        self.compaction_count = 0

        os.makedirs(self.path, exist_ok=True)
        self._lock_fd = os.open(
            os.path.join(self.path, "LOCK"), os.O_RDWR | os.O_CREAT, 0o644
        )
        try:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise StratumError(
                    f"{self.path} is already open in another process or engine"
                ) from exc
            self._recover()
        except BaseException:
            self._release_files()
            raise

        if background_compaction:
            self._bg_thread = threading.Thread(
                target=self._background_loop, name="stratum-compaction", daemon=True
            )
            self._bg_thread.start()
            self._wake.set()

    # ------------------------------------------------------------------
    # file naming
    # ------------------------------------------------------------------

    def _sst_path(self, number):
        return os.path.join(self.path, "%06d.sst" % number)

    def _wal_path(self, number):
        return os.path.join(self.path, "%06d.wal" % number)

    def _allocate_file_number(self):
        """Caller must hold _state_lock."""
        number = self._next_file_number
        self._next_file_number += 1
        return number

    def _save_manifest(self, tables):
        """Caller must hold _state_lock."""
        Manifest(
            [t.file_number for t in tables], self._log_number, self._next_file_number
        ).save(self.path)

    # ------------------------------------------------------------------
    # startup / crash recovery
    # ------------------------------------------------------------------

    def _recover(self):
        manifest = Manifest.load(self.path) or Manifest()
        live_tables = set(manifest.tables)

        # Anything the manifest does not know about is debris from a flush
        # or compaction that was interrupted by a crash.
        wal_numbers = []
        highest = 0
        for name in os.listdir(self.path):
            full = os.path.join(self.path, name)
            stem, _, ext = name.partition(".")
            if name.endswith(".tmp"):
                os.remove(full)
            elif ext in ("sst", "wal") and stem.isdigit():
                number = int(stem)
                if ext == "sst" and number not in live_tables:
                    os.remove(full)
                elif ext == "wal" and number < manifest.log_number:
                    os.remove(full)
                else:
                    highest = max(highest, number)
                    if ext == "wal":
                        wal_numbers.append(number)

        tables = []
        try:
            for number in manifest.tables:
                tables.append(SSTable(self._sst_path(number), number, self._cache))
        except FileNotFoundError as exc:
            for table in tables:
                table.close()
            raise StratumError(f"MANIFEST lists a missing table: {exc}") from exc

        self._next_file_number = max(manifest.next_file_number, highest + 1)
        self._log_number = manifest.log_number

        # Rebuild the memtable from the surviving WAL files, oldest first.
        mem = Memtable()
        for number in sorted(wal_numbers):
            wal_path = self._wal_path(number)
            for key, value in recover(wal_path):
                mem.put(key, value)
            self._old_wal_paths.append(wal_path)

        self._state = _State(mem, tuple(tables))
        # Persist what we recovered as an SSTable and start a fresh WAL.
        self._rotate()

    # ------------------------------------------------------------------
    # write path
    # ------------------------------------------------------------------

    @staticmethod
    def _check_key(key):
        if type(key) is not bytes:
            raise TypeError("keys must be bytes")
        if len(key) > MAX_KEY_BYTES:
            raise ValueError("key is too large")

    def _check_usable(self):
        if self._closed:
            raise StratumError("the database is closed")

    def put(self, key, value):
        """Store ``value`` under ``key``, replacing any existing value."""
        self._check_key(key)
        if type(value) is not bytes:
            raise TypeError("values must be bytes")
        if len(value) > MAX_VALUE_BYTES:
            raise ValueError("value is too large")
        self._write(key, value)

    def delete(self, key):
        """Delete ``key`` (a no-op if it does not exist)."""
        self._check_key(key)
        self._write(key, None)  # None = tombstone

    def _write(self, key, value):
        with self._write_lock:
            self._check_usable()
            if self._bg_error is not None:
                raise StratumError("background compaction failed") from self._bg_error
            self._wal.append(key, value)  # 1. make it durable
            mem = self._state.mem
            mem.put(key, value)  # 2. make it visible
            if mem.approx_bytes >= self._memtable_bytes:
                self._rotate()  # 3. spill to disk when full

    def flush(self):
        """Write the memtable to a new SSTable now (no-op if it is empty)."""
        with self._write_lock:
            self._check_usable()
            if len(self._state.mem):
                self._rotate()

    def _rotate(self):
        """Flush the memtable to an SSTable and switch to a fresh WAL.

        Caller must hold _write_lock (or be the constructor), so no write
        can touch the memtable while it is being flushed. Readers keep
        using the old memtable until the new state is swapped in.

        Crash safety comes from the order of the steps:
          1. write the SSTable (temp file, fsync, rename)
          2. create the new, empty WAL
          3. atomically replace the MANIFEST: it adds the table and
             declares every older WAL obsolete
          4. delete the old WAL files
        A crash before step 3 leaves the old WAL in charge (the orphan
        table is deleted on restart). A crash after step 3 leaves the table
        in charge (the stale WAL is deleted on restart).
        """
        mem = self._state.mem
        with self._state_lock:
            table_number = self._allocate_file_number() if len(mem) else None
            wal_number = self._allocate_file_number()

        table = None
        if table_number is not None:
            path = self._sst_path(table_number)
            build_table(path, mem.items(), self._block_size, self._bits_per_key)
            table = SSTable(path, table_number, self._cache)

        new_wal = WAL(self._wal_path(wal_number), sync=self._sync)

        with self._state_lock:
            tables = self._state.tables  # re-read: a compaction may have run
            if table is not None:
                tables = (table,) + tables
            self._log_number = wal_number
            self._save_manifest(tables)
            if table is not None:
                self._state = _State(Memtable(), tables)
                self.flush_count += 1

        old_wal, self._wal = self._wal, new_wal
        if old_wal is not None:
            old_wal.close()
            self._old_wal_paths.append(old_wal.path)
        for path in self._old_wal_paths:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass
        self._old_wal_paths = []
        self._wake.set()  # there may be something to compact now

    # ------------------------------------------------------------------
    # read path
    # ------------------------------------------------------------------

    def get(self, key, default=None):
        """Return the value for ``key``, or ``default`` if it is absent."""
        self._check_key(key)
        self._check_usable()
        mem, tables = self._state
        value = mem.get(key)
        if value is MISSING:
            for table in tables:  # newest first; the first answer wins
                value = table.get(key)
                if value is not MISSING:
                    break
            else:
                return default
        return default if value is None else value  # None = tombstone

    def __contains__(self, key):
        return self.get(key, MISSING) is not MISSING

    def scan(self, start=None, end=None, limit=None):
        """Iterate ``(key, value)`` pairs with ``start <= key < end`` in key
        order. ``None`` for a bound means "unbounded".

        The set of SSTables is fixed when ``scan`` is called, so flushes and
        compactions that happen mid-scan do not disturb it. The memtable is
        read live, so writes made *during* the scan may or may not appear.
        """
        for bound in (start, end):
            if bound is not None:
                self._check_key(bound)
        self._check_usable()
        mem, tables = self._state  # snapshot now, not at first next()
        if limit is not None and limit <= 0:
            return iter(())
        return self._scan(mem, tables, start, end, limit)

    @staticmethod
    def _scan(mem, tables, start, end, limit):
        sources = [mem.items(start)]
        for table in tables:
            if end is not None and table.min_key >= end:
                continue  # table lies entirely after the range
            sources.append(table.items(start))
        produced = 0
        for key, value in merge_iterators(sources):
            if end is not None and key >= end:
                return
            if value is None:
                continue  # tombstone: the key is deleted
            yield key, value
            produced += 1
            if limit is not None and produced >= limit:
                return

    # ------------------------------------------------------------------
    # compaction
    # ------------------------------------------------------------------

    def _background_loop(self):
        while True:
            self._wake.wait()
            self._wake.clear()
            if self._closing:
                return
            try:
                while self._compact_once(full=False):
                    pass
            except _Aborted:
                return
            except Exception as exc:  # surfaced to the next writer
                self._bg_error = exc
                return

    def _check_abort(self):
        if self._closing:
            raise _Aborted()

    def _compact_once(self, full):
        """Run one compaction. Returns True if tables were merged."""
        with self._compaction_lock:
            with self._state_lock:
                tables = self._state.tables
                if full:
                    picked = (0, len(tables)) if tables else None
                else:
                    picked = pick_compaction(
                        [t.size for t in tables],
                        self._min_tables, self._max_tables, self._size_ratio,
                    )
                if picked is None:
                    return False
                start, end = picked
                inputs = tables[start:end]
                # Tombstones can go only if nothing older remains below.
                drop_tombstones = end == len(tables)
                number = self._allocate_file_number()

            # The slow part runs without any lock that readers or writers need.
            path = self._sst_path(number)
            count = compact_tables(
                inputs, path, drop_tombstones, self._block_size,
                self._bits_per_key, None if full else self._check_abort,
            )
            output = SSTable(path, number, self._cache) if count else None

            with self._state_lock:
                current = self._state.tables  # may have new tables in front
                at = current.index(inputs[0])
                replaced = current[:at] + ((output,) if output else ()) \
                    + current[at + len(inputs):]
                self._save_manifest(replaced)  # commit point
                self._state = _State(self._state.mem, replaced)
                self.compaction_count += 1

            # Only now is it safe to delete the inputs. Readers that still
            # hold these tables keep working: the open file descriptors stay
            # valid until the SSTable objects are garbage collected.
            for table in inputs:
                try:
                    os.remove(table.path)
                except FileNotFoundError:
                    pass
            return True

    def compact(self):
        """Flush, then merge every SSTable into one, dropping all tombstones
        and overwritten values."""
        self.flush()
        self._check_usable()
        self._compact_once(full=True)

    # ------------------------------------------------------------------
    # housekeeping
    # ------------------------------------------------------------------

    def stats(self):
        mem, tables = self._state
        return {
            "memtable_entries": len(mem),
            "memtable_bytes": mem.approx_bytes,
            "tables": [
                {"file": t.file_number, "bytes": t.size, "entries": t.num_entries}
                for t in tables
            ],
            "table_count": len(tables),
            "disk_bytes": sum(t.size for t in tables),
            "flushes": self.flush_count,
            "compactions": self.compaction_count,
            "block_cache_hits": self._cache.hits,
            "block_cache_misses": self._cache.misses,
            "bloom_checks": sum(t.bloom_checks for t in tables),
            "bloom_rejects": sum(t.bloom_rejects for t in tables),
            "bloom_false_positives": sum(t.false_positives for t in tables),
        }

    def _release_files(self):
        if self._wal is not None:
            self._wal.close()
            self._wal = None
        if self._lock_fd is not None:
            os.close(self._lock_fd)  # also releases the flock
            self._lock_fd = None

    def close(self):
        """Stop background work and release all files. Safe to call twice.

        The memtable is not flushed: its contents are already in the WAL
        and are replayed on the next open.
        """
        with self._write_lock:
            if self._closed:
                return
            self._closing = True
            self._wake.set()
            if self._bg_thread is not None:
                self._bg_thread.join()
            self._closed = True
            for table in self._state.tables:
                table.close()
            self._state = _State(Memtable(), ())
            self._release_files()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
