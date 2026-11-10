import os
import shutil
import tempfile
import unittest

from stratum import Stratum, StratumError
from stratum.manifest import Manifest
from stratum.util import MISSING, write_all
from stratum.wal import WAL


class EngineTestCase(unittest.TestCase):
    """Base class: a temp directory and a helper that opens engines in it."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-engine-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def open(self, **kwargs):
        kwargs.setdefault("sync", False)
        db = Stratum(self.dir, **kwargs)
        self.addCleanup(db.close)
        return db

    def files(self, ext):
        return sorted(f for f in os.listdir(self.dir) if f.endswith(ext))


class BasicOperationsTest(EngineTestCase):
    def test_put_get_delete(self):
        db = self.open()
        self.assertIsNone(db.get(b"k"))
        db.put(b"k", b"v1")
        self.assertEqual(db.get(b"k"), b"v1")
        db.put(b"k", b"v2")
        self.assertEqual(db.get(b"k"), b"v2")
        self.assertIn(b"k", db)
        db.delete(b"k")
        self.assertIsNone(db.get(b"k"))
        self.assertNotIn(b"k", db)
        self.assertEqual(db.get(b"k", b"fallback"), b"fallback")
        db.delete(b"never-existed")  # no error

    def test_empty_value_is_not_a_delete(self):
        db = self.open()
        db.put(b"k", b"")
        self.assertEqual(db.get(b"k"), b"")
        self.assertIn(b"k", db)
        db.flush()
        self.assertEqual(db.get(b"k"), b"")
        self.assertEqual(list(db.scan()), [(b"k", b"")])

    def test_type_and_size_checks(self):
        db = self.open()
        with self.assertRaises(TypeError):
            db.put("text", b"v")
        with self.assertRaises(TypeError):
            db.put(b"k", "text")
        with self.assertRaises(TypeError):
            db.put(b"k", None)
        with self.assertRaises(TypeError):
            db.get("text")
        with self.assertRaises(TypeError):
            db.delete(1)
        with self.assertRaises(TypeError):
            db.scan(start="a")
        with self.assertRaises(ValueError):
            db.put(b"k" * 70_000, b"v")

    def test_newest_layer_wins(self):
        """The same key in an old table, a new table and the memtable."""
        db = self.open(background_compaction=False)
        db.put(b"k", b"old")
        db.put(b"gone", b"x")
        db.flush()
        db.put(b"k", b"mid")
        db.delete(b"gone")
        db.flush()
        self.assertEqual(db.get(b"k"), b"mid")
        self.assertIsNone(db.get(b"gone"))  # tombstone in newer table
        db.put(b"k", b"new")
        self.assertEqual(db.get(b"k"), b"new")
        self.assertEqual(list(db.scan()), [(b"k", b"new")])
        db.delete(b"k")
        self.assertIsNone(db.get(b"k"))  # tombstone in memtable
        self.assertEqual(list(db.scan()), [])
        self.assertEqual(db.stats()["table_count"], 2)


class ScanTest(EngineTestCase):
    def setUp(self):
        super().setUp()
        self.db = self.open(background_compaction=False)
        # spread keys over two tables and the memtable
        for i in range(0, 30, 3):
            self.db.put(b"k%02d" % i, b"t1")
        self.db.flush()
        for i in range(1, 30, 3):
            self.db.put(b"k%02d" % i, b"t2")
        self.db.flush()
        for i in range(2, 30, 3):
            self.db.put(b"k%02d" % i, b"mem")
        self.all = [(b"k%02d" % i, (b"t1", b"t2", b"mem")[i % 3]) for i in range(30)]

    def test_full_scan_is_sorted_and_complete(self):
        self.assertEqual(list(self.db.scan()), self.all)

    def test_bounds_are_start_inclusive_end_exclusive(self):
        got = list(self.db.scan(b"k05", b"k10"))
        self.assertEqual([k for k, _ in got], [b"k05", b"k06", b"k07", b"k08", b"k09"])
        self.assertEqual(list(self.db.scan(b"k10", b"k10")), [])
        self.assertEqual(list(self.db.scan(b"k20", b"k10")), [])
        self.assertEqual(list(self.db.scan(start=b"k28")), self.all[28:])
        self.assertEqual(list(self.db.scan(end=b"k02")), self.all[:2])
        self.assertEqual(list(self.db.scan(b"k055", b"k07")), self.all[6:7])
        self.assertEqual(list(self.db.scan(b"zzz")), [])

    def test_limit(self):
        self.assertEqual(list(self.db.scan(limit=4)), self.all[:4])
        self.assertEqual(list(self.db.scan(b"k10", limit=1)), self.all[10:11])
        self.assertEqual(list(self.db.scan(limit=0)), [])
        self.assertEqual(list(self.db.scan(limit=1000)), self.all)

    def test_limit_counts_only_live_keys(self):
        self.db.delete(b"k00")
        self.db.delete(b"k01")
        self.assertEqual(list(self.db.scan(limit=2)), self.all[2:4])

    def test_scan_survives_flush_and_compaction_midway(self):
        it = self.db.scan()
        first = [next(it) for _ in range(5)]
        self.db.put(b"zzz", b"late")
        self.db.compact()  # deletes the files the scan is reading
        self.assertEqual(self.db.stats()["table_count"], 1)
        rest = [kv for kv in it if kv[0] != b"zzz"]
        self.assertEqual(first + rest, self.all)


class PersistenceTest(EngineTestCase):
    def test_data_survives_reopen_from_wal_only(self):
        db = self.open()
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        db.delete(b"a")
        self.assertEqual(db.stats()["table_count"], 0)  # nothing flushed yet
        db.close()

        db = self.open()
        self.assertIsNone(db.get(b"a"))
        self.assertEqual(db.get(b"b"), b"2")
        # recovery turned the replayed WAL into a table
        self.assertEqual(db.stats()["table_count"], 1)

    def test_data_survives_reopen_with_tables_and_wal(self):
        db = self.open(memtable_bytes=2000, background_compaction=False)
        expected = {}
        for i in range(500):
            key, value = b"key%04d" % (i % 200), b"value-%d" % i
            db.put(key, value)
            expected[key] = value
        self.assertGreater(db.stats()["flushes"], 3)
        db.close()
        db = self.open()
        self.assertEqual(dict(db.scan()), expected)

    def test_sync_mode_works(self):
        db = self.open(sync=True, memtable_bytes=500)
        for i in range(20):
            db.put(b"k%d" % i, b"v" * 50)
        db.close()
        db = self.open(sync=True)
        self.assertEqual(len(list(db.scan())), 20)

    def test_memtable_flushes_at_threshold_and_wal_rotates(self):
        db = self.open(memtable_bytes=1000, background_compaction=False)
        self.assertEqual(len(self.files(".wal")), 1)
        first_wal = self.files(".wal")
        for i in range(50):
            db.put(b"key%03d" % i, b"x" * 30)
        self.assertGreaterEqual(len(self.files(".sst")), 2)
        self.assertEqual(len(self.files(".wal")), 1)  # old WALs are deleted
        self.assertNotEqual(self.files(".wal"), first_wal)
        self.assertEqual(self.files(".tmp"), [])
        self.assertLess(db.stats()["memtable_bytes"], 1000)

    def test_flush_of_empty_memtable_is_a_noop(self):
        db = self.open()
        db.flush()
        self.assertEqual(db.stats()["table_count"], 0)

    def test_manifest_matches_files_on_disk(self):
        db = self.open(memtable_bytes=1000, background_compaction=False)
        for i in range(100):
            db.put(b"key%03d" % i, b"x" * 30)
        manifest = Manifest.load(self.dir)
        self.assertEqual(
            sorted("%06d.sst" % n for n in manifest.tables), self.files(".sst")
        )
        self.assertEqual(manifest.tables, sorted(manifest.tables, reverse=True))
        self.assertEqual(["%06d.wal" % manifest.log_number], self.files(".wal"))


class RecoveryTest(EngineTestCase):
    def test_orphan_files_are_removed_on_open(self):
        db = self.open()
        db.put(b"a", b"1")
        db.flush()
        db.close()
        for name in ("000999.sst", "000998.sst.tmp", "MANIFEST.tmp", "000000.wal"):
            with open(os.path.join(self.dir, name), "wb") as f:
                f.write(b"leftover junk from a crashed flush")
        db = self.open()
        self.assertEqual(db.get(b"a"), b"1")
        names = os.listdir(self.dir)
        for name in ("000999.sst", "000998.sst.tmp", "MANIFEST.tmp", "000000.wal"):
            self.assertNotIn(name, names)

    def test_torn_wal_tail_is_ignored(self):
        db = self.open()
        db.put(b"a", b"1")
        db.put(b"b", b"2")
        db.close()
        (wal_name,) = self.files(".wal")
        wal_path = os.path.join(self.dir, wal_name)
        os.truncate(wal_path, os.path.getsize(wal_path) - 3)
        db = self.open()
        self.assertEqual(dict(db.scan()), {b"a": b"1"})

    def test_two_wals_are_replayed_in_order(self):
        """Simulates a crash after the new WAL was created but before the
        manifest switched over: both logs must be replayed, oldest first."""
        db = self.open()
        db.put(b"k", b"old")
        db.put(b"only-old", b"1")
        db.close()
        (wal_name,) = self.files(".wal")
        newer = os.path.join(self.dir, "%06d.wal" % (int(wal_name[:6]) + 1))
        wal = WAL(newer, sync=False)
        wal.append(b"k", b"new")
        wal.close()
        db = self.open()
        self.assertEqual(dict(db.scan()), {b"k": b"new", b"only-old": b"1"})

    def test_missing_table_is_an_error(self):
        db = self.open()
        db.put(b"a", b"1")
        db.flush()
        db.put(b"b", b"1")
        db.flush()
        db.close()
        os.remove(os.path.join(self.dir, self.files(".sst")[0]))
        with self.assertRaisesRegex(StratumError, "missing table"):
            Stratum(self.dir)
        # and the failed open released the directory lock
        with self.assertRaisesRegex(StratumError, "missing table"):
            Stratum(self.dir)

    def test_file_numbers_are_never_reused(self):
        seen = set()
        for round_number in range(5):
            db = self.open()
            db.put(b"k%d" % round_number, b"v")
            db.flush()
            current = set(self.files(".sst")) | set(self.files(".wal"))
            db.close()
            new = {f[:6] for f in current} - seen
            self.assertTrue(new)
            seen |= {f[:6] for f in current}
        db = self.open()
        self.assertEqual(len(list(db.scan())), 5)


class LifecycleTest(EngineTestCase):
    def test_directory_lock_blocks_second_open(self):
        self.open()
        with self.assertRaises(StratumError):
            Stratum(self.dir)

    def test_lock_released_on_close(self):
        self.open().close()
        self.open().close()

    def test_use_after_close_raises(self):
        db = self.open()
        db.put(b"a", b"1")
        db.close()
        db.close()  # closing twice is fine
        for call in (
            lambda: db.put(b"a", b"2"),
            lambda: db.get(b"a"),
            lambda: db.delete(b"a"),
            lambda: db.scan(),
            lambda: db.flush(),
            lambda: db.compact(),
        ):
            with self.assertRaises(StratumError):
                call()

    def test_context_manager(self):
        with Stratum(self.dir, sync=False) as db:
            db.put(b"a", b"1")
        with Stratum(self.dir, sync=False) as db:
            self.assertEqual(db.get(b"a"), b"1")

    def test_stats(self):
        db = self.open(background_compaction=False)
        db.put(b"a", b"1")
        stats = db.stats()
        self.assertEqual(stats["memtable_entries"], 1)
        self.assertEqual(stats["table_count"], 0)
        db.flush()
        db.get(b"a")
        db.get(b"zzz")
        stats = db.stats()
        self.assertEqual(stats["memtable_entries"], 0)
        self.assertEqual(stats["tables"][0]["entries"], 1)
        self.assertEqual(stats["disk_bytes"], stats["tables"][0]["bytes"])
        self.assertEqual(stats["flushes"], 1)


class BackgroundFailureTest(EngineTestCase):
    def test_background_error_is_reported_to_writers(self):
        db = self.open(memtable_bytes=1000, compaction_min_tables=2)

        def broken(full):
            raise OSError("disk on fire")

        db._compact_once = broken
        with self.assertRaisesRegex(StratumError, "background compaction failed"):
            for i in range(500):
                db.put(b"key%03d" % i, b"x" * 50)
        # reads still work, and nothing that was acknowledged is lost
        self.assertEqual(db.get(b"key000"), b"x" * 50)


class UtilTest(unittest.TestCase):
    def test_missing_sentinel(self):
        self.assertEqual(repr(MISSING), "MISSING")
        self.assertFalse(MISSING)

    def test_write_all_handles_partial_writes(self):
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        real_write = os.write
        calls = []

        def stingy_write(fd, data):  # writes at most 3 bytes per call
            calls.append(len(data))
            return real_write(fd, bytes(data[:3]))

        os.write = stingy_write
        try:
            write_all(write_fd, b"0123456789")
        finally:
            os.write = real_write
            os.close(write_fd)
        self.assertEqual(os.read(read_fd, 100), b"0123456789")
        self.assertEqual(calls, [10, 7, 4, 1])


if __name__ == "__main__":
    unittest.main()
