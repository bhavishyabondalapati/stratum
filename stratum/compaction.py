"""Size-tiered compaction: choosing which SSTables to merge, and merging them.

Every memtable flush adds one more SSTable. Left alone, reads would have to
check more and more files, and deleted or overwritten data would never be
reclaimed. Compaction fixes both by merging several tables into one.

"Size-tiered" means we merge tables of *similar size*: four small tables
become one medium table, four medium tables become one large one, and so
on. Each key is therefore rewritten only O(log n) times.

Stratum keeps its tables in age order (newest first) and only ever merges
tables that are *next to each other* in that order. That keeps a simple
invariant: a table earlier in the list is always newer than every table
after it, so a read can stop at the first table that knows the key. (This
is the same idea as RocksDB's "universal" compaction.)
"""

from .iterators import merge_iterators
from .sstable import build_table


def pick_compaction(sizes, min_tables=4, max_tables=12, size_ratio=1.5):
    """Choose a run of adjacent tables to merge.

    ``sizes`` are table sizes in bytes, newest first. Starting from each
    position we grow a run for as long as the next (older) table is no
    bigger than ``size_ratio`` times everything gathered so far - i.e. it
    belongs to roughly the same size tier. The first run with at least
    ``min_tables`` members wins.

    Returns ``(start, end)`` (a half-open slice of the list) or None.
    """
    count = len(sizes)
    for start in range(count - min_tables + 1):
        total = sizes[start]
        end = start + 1
        while (
            end < count
            and end - start < max_tables
            and sizes[end] <= total * size_ratio
        ):
            total += sizes[end]
            end += 1
        if end - start >= min_tables:
            return start, end
    return None


def merged_entries(tables, drop_tombstones):
    """Merge ``tables`` (newest first) into one sorted stream.

    Shadowed (older) versions of a key are always dropped. Tombstones are
    dropped only when ``drop_tombstones`` is true, which is safe only if no
    older table exists outside this merge - otherwise the tombstone is
    still needed to hide an older value living in that table.
    """
    merged = merge_iterators([table.items() for table in tables])
    if drop_tombstones:
        return ((key, value) for key, value in merged if value is not None)
    return merged


def compact_tables(tables, out_path, drop_tombstones, block_size=4096,
                   bloom_bits_per_key=10, check_abort=None):
    """Merge ``tables`` into a new SSTable at ``out_path``.

    Returns the number of entries written (0 means no file was created).
    ``check_abort`` is called periodically and may raise to cancel.
    """
    entries = merged_entries(tables, drop_tombstones)
    if check_abort is not None:
        entries = _with_abort_checks(entries, check_abort)
    return build_table(out_path, entries, block_size, bloom_bits_per_key)


def _with_abort_checks(entries, check_abort, every=1024):
    for n, entry in enumerate(entries):
        if n % every == 0:
            check_abort()
        yield entry
