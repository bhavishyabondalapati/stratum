import random
import threading
import unittest

from stratum.memtable import ENTRY_OVERHEAD, Memtable
from stratum.skiplist import SkipList
from stratum.util import MISSING


class SkipListTest(unittest.TestCase):
    def test_empty(self):
        sl = SkipList()
        self.assertEqual(len(sl), 0)
        self.assertIs(sl.get(b"a"), MISSING)
        self.assertEqual(list(sl), [])
        self.assertNotIn(b"a", sl)

    def test_insert_get_overwrite(self):
        sl = SkipList()
        self.assertIs(sl.insert(b"b", b"1"), MISSING)
        self.assertIs(sl.insert(b"a", b"2"), MISSING)
        self.assertEqual(sl.insert(b"b", b"3"), b"1")  # returns old value
        self.assertEqual(len(sl), 2)
        self.assertEqual(sl.get(b"b"), b"3")
        self.assertEqual(sl.get(b"a"), b"2")
        self.assertEqual(sl.get(b"zz", "default"), "default")
        self.assertIn(b"a", sl)

    def test_none_value_is_stored(self):
        sl = SkipList()
        sl.insert(b"k", None)
        self.assertIsNone(sl.get(b"k"))
        self.assertIn(b"k", sl)

    def test_sorted_iteration_matches_dict(self):
        rng = random.Random(1)
        sl = SkipList(seed=1)
        model = {}
        for _ in range(5000):
            key = b"%06d" % rng.randrange(2000)
            value = b"%d" % rng.randrange(10**9)
            sl.insert(key, value)
            model[key] = value
        self.assertEqual(len(sl), len(model))
        self.assertEqual(list(sl.items()), sorted(model.items()))
        for key, value in model.items():
            self.assertEqual(sl.get(key), value)

    def test_items_from_start(self):
        sl = SkipList()
        for i in range(0, 100, 2):
            sl.insert(b"%03d" % i, i)
        # start on an existing key, between keys, before all, after all
        self.assertEqual(next(sl.items(b"010"))[0], b"010")
        self.assertEqual(next(sl.items(b"011"))[0], b"012")
        self.assertEqual(next(sl.items(b""))[0], b"000")
        self.assertEqual(list(sl.items(b"099")), [])
        self.assertEqual(len(list(sl.items(b"050"))), 25)

    def test_uses_multiple_levels(self):
        sl = SkipList(seed=7)
        for i in range(2000):
            sl.insert(i, i)
        self.assertGreater(sl._level, 3)

    def test_reader_sees_consistent_list_during_writes(self):
        sl = SkipList()
        stop = threading.Event()
        errors = []

        def reader():
            while not stop.is_set():
                keys = [k for k, _ in sl.items()]
                if keys != sorted(keys):
                    errors.append("unsorted iteration")
                    return

        thread = threading.Thread(target=reader)
        thread.start()
        rng = random.Random(3)
        for _ in range(20000):
            sl.insert(rng.randrange(10**6), 0)
        stop.set()
        thread.join()
        self.assertEqual(errors, [])


class MemtableTest(unittest.TestCase):
    def test_size_accounting(self):
        mem = Memtable()
        mem.put(b"key", b"12345")
        self.assertEqual(mem.approx_bytes, 3 + 5 + ENTRY_OVERHEAD)
        mem.put(b"key", b"1")  # overwrite with a shorter value
        self.assertEqual(mem.approx_bytes, 3 + 1 + ENTRY_OVERHEAD)
        mem.put(b"key", None)  # tombstone
        self.assertEqual(mem.approx_bytes, 3 + ENTRY_OVERHEAD)
        mem.put(b"key", b"abc")
        self.assertEqual(mem.approx_bytes, 3 + 3 + ENTRY_OVERHEAD)
        self.assertEqual(len(mem), 1)

    def test_get_distinguishes_tombstone_from_missing(self):
        mem = Memtable()
        mem.put(b"dead", None)
        self.assertIsNone(mem.get(b"dead"))
        self.assertIs(mem.get(b"never"), MISSING)
        self.assertEqual(list(mem.items()), [(b"dead", None)])


if __name__ == "__main__":
    unittest.main()
