# Design decisions

Design choices and why I made them. Each one lists the
alternative I did not take, so the trade-off is visible.

## Scope and environment

1. **Standard library only, Python 3.10+.** No pip installs. Developed and
   tested on Python 3.12 (conda env `stratum`), macOS on Apple Silicon.
   `requirements.txt` exists but is intentionally empty.
2. **`unittest`, not pytest.** The project is standard library only, and
   pytest would be a dependency. (`pytest` can still run the
   suite unchanged if you have it.)
3. **POSIX only.** The engine uses `os.pread`, `fcntl.flock`, directory
   `fsync` and "an unlinked file stays readable while it is open". None of
   that works the same way on Windows.
4. **Keys and values are `bytes`.** Keys up to 64 KiB, values up to
   512 MiB. Empty keys and empty values are allowed; an empty value is
   *not* a delete. The client and CLI accept `str` and encode it as UTF-8.

## Write path and durability

5. **WAL records go to the OS unbuffered (`os.write`), one per write.**
   When `put` returns, the bytes are in the kernel, so the write survives
   a process crash (`kill -9`) even with `sync=False`. A Python-side
   buffer would be faster but could lose acknowledged writes.
6. **`sync=True` is the default** and calls `os.fsync` after every write.
   `sync=False` skips it: still safe against process crashes, not against
   OS crashes or power loss.
   *Not done:* group commit (batching several writers into one fsync), and
   macOS `F_FULLFSYNC`. On macOS plain `fsync` does not force the drive to
   flush its cache, so true power-loss safety would need `F_FULLFSYNC`.
7. **SSTables and the MANIFEST are always fsync'd**, whatever `sync` says.
   They are written rarely, and the WAL is deleted once they exist, so
   they must really be on disk.
8. **Torn-record handling: stop at the first bad record and truncate.**
   A record is bad if it is incomplete, fails its CRC32, or has an
   impossible structure. Everything from that point is cut off the file.
   Since the log is append-only, a crash can only damage the tail, so this
   never discards an acknowledged write. If a record in the *middle* were
   corrupted by a disk fault, the records after it would also be dropped;
   a production engine would report that instead of silently truncating.
   A zero-filled tail is also rejected (the CRC of an empty payload is 0,
   so a minimum payload length is enforced).
9. **One WAL file per memtable.** A flush switches to a new WAL and
   deletes the old one. There is never a need to truncate a log that is in
   use.
10. **The memtable is flushed by the writing thread, not in the
    background.** When the memtable is full, the `put` that filled it
    writes the SSTable before returning, and other writers wait. Readers
    are not blocked (they keep reading the old memtable).
    *Alternative:* an immutable memtable flushed by a background thread
    (LevelDB/RocksDB). Under CPython's GIL only one thread runs Python at a
    time, so that would add locking complexity without adding throughput.
    The cost is an occasional slow write (one flush, tens of milliseconds).
11. **Recovered WAL data is flushed to an SSTable on startup** and a fresh
    WAL is started. This keeps recovery uniform (replay, flush, delete old
    logs) and bounded. The cost is a small SSTable per restart, which
    compaction merges later. A clean `close()` does not flush; it relies
    on the same path at the next open.

## SSTables

12. **Format:** 4 KiB data blocks (each with a CRC32), then the Bloom
    filter, then a sparse index (first key + offset of every block, plus
    the largest key), then a fixed 52-byte footer with a magic number.
    Every block is checksummed and verified on every read.
    *Not done:* compression and key-prefix compression. `zlib` is in the
    standard library, so block compression would be an easy extension.
13. **The index and Bloom filter are loaded into memory when a table is
    opened.** Costs RAM (about 10 bits per key for the filter, plus one
    key per block) but makes a lookup at most one block read.
14. **Bloom filter: double hashing over one BLAKE2b digest.** Two 64-bit
    hashes `h1`, `h2` come from `hashlib.blake2b`; the k probe positions
    are `h1 + i*h2 mod m`. The filter logic is written by hand; only the
    hash primitive comes from the standard library. A hand-written Python
    hash would be far slower. The filter is sized after the table is
    written, from the exact key count. Measured false-positive rates match
    theory (see BENCHMARKS.md).
15. **A shared LRU block cache** (default 4096 decoded blocks) serves point
    lookups. Scans and compaction bypass it so they do not evict the
    blocks that lookups need.
16. **Deleted-but-still-open tables.** Compaction unlinks its input files
    immediately. Readers that already hold those tables (for example a
    scan in progress) keep working through their open file descriptors,
    which are closed when the table object is garbage collected. Simpler
    than reference counting; relies on CPython's prompt refcounting.

## Compaction

17. **Size-tiered, restricted to tables that are adjacent in age order**
    (the same idea as RocksDB's "universal" compaction). Tables are kept
    newest-first. A run of at least 4 neighbouring tables of similar size
    (each no bigger than 1.5x the total of the newer ones in the run) is
    merged into one table that takes their place in the order.
    *Why adjacent only:* it preserves the rule "earlier in the list =
    strictly newer data", so a lookup can stop at the first table that
    knows the key and no per-entry sequence numbers are needed. Classic
    size-tiered compaction (merge any similar-sized tables) would need
    sequence numbers on every entry to stay correct.
18. **Tombstones are dropped only when the merge includes the oldest
    table.** Otherwise an older table could still hold a value that the
    tombstone must keep hidden. Overwritten values are always dropped.
19. **One background compaction thread**, woken after every flush. It does
    its merging without holding any lock that readers or writers need,
    and takes a short lock only to swap the result into the table list and
    rewrite the MANIFEST. `close()` interrupts a running compaction and
    removes its partial output.
20. **If background compaction fails** (for example, disk full), the error
    is remembered and raised on the next write, instead of being lost in a
    thread nobody watches.

## Concurrency

21. **Readers take no locks.** The memtable and the table list live in one
    immutable tuple that is replaced with a single assignment (atomic
    under the GIL). The skip list links a new node bottom-up after filling
    in its own pointers, so a concurrent reader always sees a valid list.
    This relies on the GIL; a free-threaded Python build would need real
    synchronisation.
22. **Scans are snapshot-consistent for SSTables and "live" for the
    memtable.** The set of tables is fixed when `scan()` is called. Writes
    made while a scan is running may or may not show up in it. Copying the
    memtable per scan would give full snapshots but make short scans slow.
23. **Three locks, fixed order:** write lock (one writer at a time) →
    compaction lock (one compaction at a time) → state lock (file list +
    MANIFEST). The MANIFEST is rewritten while holding the state lock,
    which is simple and safe; nothing on the read path takes that lock.
24. **A `LOCK` file with `flock`** stops two processes (or two engine
    objects) from opening the same directory.

## MANIFEST

25. **A small JSON document plus a CRC32 line, rewritten whole** each time
    (temp file → fsync → rename → fsync directory). Human-readable, which
    suits a learning project. LevelDB appends edit records to a manifest
    log instead, which is cheaper with thousands of tables.
26. **The MANIFEST is the only source of truth.** On startup, any
    `.sst`, `.wal` or `.tmp` file it does not account for is deleted: it
    can only be left over from a flush or compaction that a crash
    interrupted. File numbers are never reused.

## Server, client, CLI

27. **`SCAN start end [LIMIT n]` is a sorted range scan**, not Redis's
    cursor-based `SCAN`. It returns a flat array `[k1, v1, k2, v2, ...]`
    for `start <= key < end`; an empty string means "no bound"; the
    default limit is 1000 and the maximum 100,000. `RANGE` is an alias.
    The client's `scan_iter` pages through large ranges.
28. **Extra commands beyond the core set:** `EXISTS`, `ECHO`, `INFO`, `QUIT`,
    and a stub `COMMAND` (so `redis-cli` can connect). Inline commands
    (plain text lines) are accepted so `nc`/`telnet` work.
29. **`DEL` returns how many keys existed**, like Redis. That needs a read
    before each delete, and the pair is not atomic across connections.
30. **Engine calls run in the default thread pool** so a slow write never
    freezes the event loop. This costs some per-request latency. Default
    port is 6380 (not Redis's 6379, to avoid clashing with a real Redis).
    There is no authentication or TLS; the server binds to 127.0.0.1 by
    default.

## Testing and measurement

31. **Crash test design.** The worker's operations are a deterministic
    function of `(seed, index)` and it prints the index after each
    acknowledged write. After `SIGKILL`, the recovered database must equal
    the model after exactly N or N+1 operations, where N is the last
    acknowledged index (one more write may have been in flight). This
    checks both "no acknowledged write lost" and "nothing half-applied".
    100 kills per run by default (`STRATUM_CRASH_ROUNDS` overrides).
    `SIGKILL` tests process crashes; it cannot simulate power loss.
32. **I checked that the tests can fail.** I temporarily injected three
    bugs (always drop tombstones; delete the WAL before the MANIFEST
    commit; merge preferring the oldest value). The model test caught the
    first and third, the crash test caught the second.
33. **Coverage uses a small `sys.settrace` hook** rather than
    `trace.Trace`. The `trace` module caches its ignore decision by bare
    module name, so `stratum/__init__.py` was mistaken for the standard
    library's `__init__.py` files and reported as 0% covered. Code that
    runs only in child processes is not counted.
34. **Benchmarks run with WAL fsync off** for the main numbers, with a
    separate fsync-per-write measurement, because per-write fsync mostly
    measures the disk rather than the engine.
