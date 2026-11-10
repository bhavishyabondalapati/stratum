import os
import random
import shutil
import tempfile
import threading
import time
import unittest

from stratum import Stratum
from stratum.compaction import compact_tables, merged_entries, pick_compaction
from stratum.iterators import merge_iterators
from stratum.sstable import SSTable, build_table


class MergeIteratorTest(unittest.TestCase):
    def test_newest_source_wins(self):
        newest = [(b"a", b"new"), (b"c", None)]
        middle = [(b"a", b"mid"), (b"b", b"mid")]
        oldest = [(b"a", b"old"), (b"c", b"old"), (b"d", b"old")]
        self.assertEqual(
            list(merge_iterators([newest, middle, oldest])),
            [(b"a", b"new"), (b"b", b"mid"), (b"c", None), (b"d", b"old")],
        )

    def test_empty_and_single_sources(self):
        self.assertEqual(list(merge_iterators([])), [])
        self.assertEqual(list(merge_iterators([[], []])), [])
        self.assertEqual(list(merge_iterators([[], [(b"a", b"1")]])), [(b"a", b"1")])

    def test_random_merge_matches_dict_layering(self):
        rng = random.Random(11)
        sources = []
        for _ in range(6):
            keys = sorted(rng.sample(range(300), rng.randrange(0, 120)))
            sources.append([(b"%04d" % k, b"%d" % rng.randrange(10**6)) for k in keys])
        expected = {}
        for source in reversed(sources):  # apply oldest first
            expected.update(source)
        self.assertEqual(list(merge_iterators(sources)), sorted(expected.items()))


class PickCompactionTest(unittest.TestCase):
    def test_too_few_tables(self):
        self.assertIsNone(pick_compaction([]))
        self.assertIsNone(pick_compaction([100, 100, 100]))

    def test_similar_sizes_are_merged(self):
        self.assertEqual(pick_compaction([100, 100, 100, 100]), (0, 4))
        self.assertEqual(pick_compaction([90, 110, 100, 95, 105]), (0, 5))

    def test_much_larger_old_table_is_left_alone(self):
        # four fresh flushes sitting on top of one big, already-merged table
        self.assertEqual(pick_compaction([100, 100, 100, 100, 5000]), (0, 4))
        self.assertIsNone(pick_compaction([100, 100, 100, 5000]))

    def test_run_can_start_in_the_middle(self):
        # one small new table, then a tier of four large ones
        self.assertEqual(pick_compaction([10, 5000, 5000, 5000, 5000]), (1, 5))

    def test_max_tables_caps_the_run(self):
        self.assertEqual(pick_compaction([100] * 30, max_tables=8), (0, 8))

    def test_tiers_grow_geometrically(self):
        """Simulate many flushes; table count must stay small (log-like)."""
        sizes = []
        worst = 0
        for _ in range(2000):
            sizes.insert(0, 100)  # a flush adds a new small table in front
            while True:
                picked = pick_compaction(sizes)
                if picked is None:
                    break
                start, end = picked
                sizes[start:end] = [sum(sizes[start:end])]
            worst = max(worst, len(sizes))
        self.assertEqual(sum(sizes), 200_000)  # nothing lost
        self.assertLess(worst, 20)


class CompactTablesTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-compact-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.count = 0

    def table(self, items):
        self.count += 1
        path = os.path.join(self.dir, "%06d.sst" % self.count)
        build_table(path, items)
        table = SSTable(path, self.count)
        self.addCleanup(table.close)
        return table

    def fixtures(self):
        newest = self.table([(b"a", b"a2"), (b"b", None), (b"e", b"e1")])
        oldest = self.table([(b"a", b"a1"), (b"b", b"b1"), (b"c", b"c1"), (b"d", None)])
        return [newest, oldest]

    def test_shadowed_values_dropped_tombstones_kept(self):
        out = os.path.join(self.dir, "out.sst")
        count = compact_tables(self.fixtures(), out, drop_tombstones=False)
        merged = SSTable(out)
        self.addCleanup(merged.close)
        self.assertEqual(
            list(merged),
            [(b"a", b"a2"), (b"b", None), (b"c", b"c1"), (b"d", None), (b"e", b"e1")],
        )
        self.assertEqual(count, 5)

    def test_tombstones_dropped_when_requested(self):
        out = os.path.join(self.dir, "out.sst")
        compact_tables(self.fixtures(), out, drop_tombstones=True)
        merged = SSTable(out)
        self.addCleanup(merged.close)
        self.assertEqual(list(merged), [(b"a", b"a2"), (b"c", b"c1"), (b"e", b"e1")])

    def test_everything_deleted_produces_no_file(self):
        tables = [self.table([(b"a", None)]), self.table([(b"a", b"1")])]
        out = os.path.join(self.dir, "out.sst")
        self.assertEqual(compact_tables(tables, out, drop_tombstones=True), 0)
        self.assertFalse(os.path.exists(out))
        self.assertFalse(os.path.exists(out + ".tmp"))

    def test_abort_cleans_up(self):
        class Stop(Exception):
            pass

        def abort():
            raise Stop()

        out = os.path.join(self.dir, "out.sst")
        with self.assertRaises(Stop):
            compact_tables(self.fixtures(), out, False, check_abort=abort)
        self.assertFalse(os.path.exists(out))
        self.assertFalse(os.path.exists(out + ".tmp"))

    def test_merged_entries_helper(self):
        self.assertEqual(len(list(merged_entries(self.fixtures(), False))), 5)
        self.assertEqual(len(list(merged_entries(self.fixtures(), True))), 3)


class EngineCompactionTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-compact-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def open(self, **kwargs):
        kwargs.setdefault("sync", False)
        db = Stratum(self.dir, **kwargs)
        self.addCleanup(db.close)
        return db

    def sst_files(self):
        return sorted(f for f in os.listdir(self.dir) if f.endswith(".sst"))

    def wait_for(self, condition, timeout=10.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return True
            time.sleep(0.01)
        return False

    def test_manual_compact_merges_everything_and_drops_garbage(self):
        db = self.open(background_compaction=False)
        for round_number in range(5):
            for i in range(100):
                db.put(b"key%03d" % i, b"round-%d" % round_number)
            db.flush()
        for i in range(0, 100, 2):
            db.delete(b"key%03d" % i)
        self.assertEqual(db.stats()["table_count"], 5)
        size_before = db.stats()["disk_bytes"]

        db.compact()

        stats = db.stats()
        self.assertEqual(stats["table_count"], 1)
        self.assertEqual(stats["tables"][0]["entries"], 50)  # no tombstones left
        self.assertLess(stats["disk_bytes"], size_before / 5)
        self.assertEqual(len(self.sst_files()), 1)  # inputs were deleted
        expected = [(b"key%03d" % i, b"round-4") for i in range(1, 100, 2)]
        self.assertEqual(list(db.scan()), expected)
        self.assertIsNone(db.get(b"key000"))

    def test_compacting_everything_away_leaves_no_tables(self):
        db = self.open(background_compaction=False)
        db.put(b"a", b"1")
        db.flush()
        db.delete(b"a")
        db.compact()
        self.assertEqual(db.stats()["table_count"], 0)
        self.assertEqual(self.sst_files(), [])
        self.assertIsNone(db.get(b"a"))
        db.compact()  # compacting an empty database is fine

    def test_partial_compaction_keeps_tombstones_that_still_matter(self):
        """Merging only the NEWER tables must not drop a tombstone, or the
        old value underneath would come back to life."""
        db = self.open(background_compaction=False, compaction_min_tables=2)
        db.put(b"victim", b"old value " * 2000)  # a big, old table
        db.flush()
        db.delete(b"victim")
        db.flush()
        db.put(b"other", b"x")
        db.flush()
        # sizes newest-first are [tiny, tiny, big]: only the two tiny ones merge
        self.assertTrue(db._compact_once(full=False))
        self.assertEqual(db.stats()["table_count"], 2)
        self.assertIsNone(db.get(b"victim"))
        self.assertEqual(list(db.scan()), [(b"other", b"x")])
        self.assertFalse(db._compact_once(full=False))

    def test_background_thread_compacts_automatically(self):
        db = self.open(memtable_bytes=2000)
        expected = {}
        rng = random.Random(4)
        for i in range(4000):
            key = b"key%04d" % rng.randrange(500)
            if rng.random() < 0.2:
                db.delete(key)
                expected.pop(key, None)
            else:
                expected[key] = b"value-%d" % i
                db.put(key, expected[key])
        self.assertGreater(db.stats()["flushes"], 20)
        self.assertTrue(self.wait_for(lambda: db.stats()["compactions"] > 0))
        self.assertTrue(self.wait_for(lambda: db.stats()["table_count"] < 12))
        self.assertEqual(dict(db.scan()), expected)
        for key in (b"key%04d" % i for i in range(500)):
            self.assertEqual(db.get(key), expected.get(key))
        # every table file on disk is one the engine knows about
        self.assertTrue(
            self.wait_for(lambda: len(self.sst_files()) == db.stats()["table_count"])
        )

    def test_readers_and_writer_run_during_background_compaction(self):
        db = self.open(memtable_bytes=3000)
        stop = threading.Event()
        errors = []
        committed = {}  # key -> highest version the writer has finished writing

        def reader(seed):
            rng = random.Random(seed)
            try:
                while not stop.is_set():
                    # Yield so spinning readers do not starve the writer
                    # of the GIL (which would only make the test slow).
                    time.sleep(0.0005)
                    key_id = rng.randrange(200)
                    floor = committed.get(key_id, 0)  # read BEFORE the get
                    value = db.get(b"key%04d" % key_id)
                    seen = int(value) if value is not None else 0
                    if seen < floor:
                        errors.append(f"key {key_id}: saw {seen}, expected >= {floor}")
                        return
                    if rng.random() < 0.05:
                        keys = [k for k, _ in db.scan(limit=50)]
                        if keys != sorted(set(keys)):
                            errors.append("scan returned unsorted or duplicate keys")
                            return
            except Exception as exc:  # pragma: no cover
                errors.append(repr(exc))

        threads = [threading.Thread(target=reader, args=(s,)) for s in range(3)]
        for thread in threads:
            thread.start()
        rng = random.Random(9)
        for version in range(1, 3001):
            key_id = rng.randrange(200)
            db.put(b"key%04d" % key_id, b"%d" % version)
            committed[key_id] = version
        stop.set()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        self.assertGreater(db.stats()["compactions"], 0)
        for key_id, version in committed.items():
            self.assertEqual(db.get(b"key%04d" % key_id), b"%d" % version)

    def test_close_during_background_compaction_is_clean(self):
        for _ in range(3):
            db = Stratum(self.dir, sync=False, memtable_bytes=2000)
            for i in range(1500):
                db.put(b"key%05d" % i, b"v" * 40)
            db.close()  # may interrupt a compaction that is in flight
            leftovers = [f for f in os.listdir(self.dir) if f.endswith(".tmp")]
            self.assertEqual(leftovers, [])
        db = self.open()
        self.assertEqual(len(list(db.scan())), 1500)


if __name__ == "__main__":
    unittest.main()
