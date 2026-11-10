import os
import random
import shutil
import tempfile
import unittest

from stratum.sstable import BlockCache, SSTable, SSTableWriter, build_table
from stratum.util import MISSING, CorruptionError


class SSTableTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-sst-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.path = os.path.join(self.dir, "000001.sst")

    def build(self, items, **kwargs):
        count = build_table(self.path, items, **kwargs)
        table = SSTable(self.path, 1)
        self.addCleanup(table.close)
        self.assertEqual(count, table.num_entries)
        return table

    def sample(self, n=2000):
        # every 10th key is a tombstone, every 7th has an empty value
        items = []
        for i in range(n):
            key = b"key%06d" % (i * 2)
            if i % 10 == 0:
                items.append((key, None))
            elif i % 7 == 0:
                items.append((key, b""))
            else:
                items.append((key, b"value-%d" % i * (1 + i % 5)))
        return items

    def test_roundtrip_all_entries(self):
        items = self.sample()
        table = self.build(items, block_size=512)
        self.assertEqual(table.num_entries, len(items))
        self.assertEqual(table.min_key, items[0][0])
        self.assertEqual(table.max_key, items[-1][0])
        self.assertGreater(len(table._blocks), 10)  # really is multi-block
        self.assertEqual(list(table), items)
        for key, value in items:
            self.assertEqual(table.get(key), value)

    def test_get_absent_keys(self):
        table = self.build(self.sample(), block_size=512)
        self.assertIs(table.get(b"a"), MISSING)  # before min_key
        self.assertIs(table.get(b"z"), MISSING)  # after max_key
        for i in range(500):  # odd keys fall between stored keys
            self.assertIs(table.get(b"key%06d" % (i * 2 + 1)), MISSING)
        self.assertGreater(table.bloom_rejects, 400)  # bloom did the work
        self.assertEqual(
            table.bloom_checks, table.bloom_rejects + table.false_positives
        )

    def test_items_from_start(self):
        items = self.sample(500)
        table = self.build(items, block_size=256)
        keys = [k for k, _ in items]
        rng = random.Random(5)
        probes = [None, b"", b"a", b"z", keys[0], keys[-1]]
        probes += [b"key%06d" % rng.randrange(1100) for _ in range(200)]
        for start in probes:
            expected = [kv for kv in items if start is None or kv[0] >= start]
            self.assertEqual(list(table.items(start)), expected, start)

    def test_value_larger_than_block(self):
        items = [(b"a", b"x" * 50_000), (b"b", b"y"), (b"c", b"z" * 20_000)]
        table = self.build(items, block_size=1024)
        self.assertEqual(list(table), items)
        self.assertEqual(table.get(b"c"), b"z" * 20_000)

    def test_single_entry_and_binary_keys(self):
        table = self.build([(b"\x00\xff\x00", b"\xff" * 3)])
        self.assertEqual(table.get(b"\x00\xff\x00"), b"\xff" * 3)
        self.assertEqual(table.min_key, table.max_key)

    def test_empty_input_creates_no_file(self):
        self.assertEqual(build_table(self.path, []), 0)
        self.assertEqual(os.listdir(self.dir), [])

    def test_writer_rejects_unsorted_and_duplicate_keys(self):
        for second in (b"a", b"b"):
            writer = SSTableWriter(self.path)
            writer.add(b"b", b"1")
            with self.assertRaises(ValueError):
                writer.add(second, b"2")
            writer.abandon()
        self.assertEqual(os.listdir(self.dir), [])

    def test_failed_build_leaves_no_files(self):
        def exploding():
            yield b"a", b"1"
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            build_table(self.path, exploding())
        self.assertEqual(os.listdir(self.dir), [])

    def test_block_cache_is_used_for_point_lookups(self):
        cache = BlockCache(8)
        build_table(self.path, self.sample(), block_size=512)
        table = SSTable(self.path, 1, cache)
        self.addCleanup(table.close)
        table.get(b"key000002")
        self.assertEqual((cache.hits, cache.misses), (0, 1))
        table.get(b"key000002")
        self.assertEqual((cache.hits, cache.misses), (1, 1))
        for i in range(1, 2000, 3):
            table.get(b"key%06d" % (i * 2))
        self.assertLessEqual(len(cache), 8)  # LRU eviction keeps it bounded
        list(table)  # scans bypass the cache
        self.assertLessEqual(len(cache), 8)

    def test_zero_capacity_cache(self):
        cache = BlockCache(0)
        cache.put("k", "v")
        self.assertIsNone(cache.get("k"))

    def corrupt(self, offset, table_items=None):
        build_table(self.path, table_items or self.sample(300), block_size=512)
        with open(self.path, "r+b") as f:
            if offset < 0:
                f.seek(offset, os.SEEK_END)
            else:
                f.seek(offset)
            byte = f.read(1)
            f.seek(-1, os.SEEK_CUR)
            f.write(bytes([byte[0] ^ 0xFF]))

    def test_corrupt_data_block_detected_on_read(self):
        self.corrupt(20)
        table = SSTable(self.path)  # metadata is still fine
        self.addCleanup(table.close)
        with self.assertRaises(CorruptionError):
            list(table)
        with self.assertRaises(CorruptionError):
            table.get(b"key000002")

    def test_corrupt_footer_magic_and_checksum_detected_on_open(self):
        for offset in (-1, -10, -30):  # magic, footer crc, footer body
            self.corrupt(offset)
            with self.assertRaises(CorruptionError):
                SSTable(self.path)

    def test_corrupt_index_or_bloom_detected_on_open(self):
        self.corrupt(-60)  # just before the footer: inside the index block
        with self.assertRaises(CorruptionError):
            SSTable(self.path)

    def test_truncated_file_detected(self):
        build_table(self.path, self.sample(300))
        size = os.path.getsize(self.path)
        for new_size in (0, 10, size // 2, size - 1):
            os.truncate(self.path, new_size)
            with self.assertRaises(CorruptionError):
                SSTable(self.path)


if __name__ == "__main__":
    unittest.main()
