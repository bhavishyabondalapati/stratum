"""Randomized model-based test.

Run thousands of random operations against Stratum and against a plain
Python dict (the "model"). After every read, both must agree exactly. The
memtable is tiny, so the run constantly flushes, compacts and reopens the
database - any bug in layering, tombstones, merging or recovery shows up as
a mismatch. A failure message includes the seed, so it can be replayed.
"""

import random
import shutil
import tempfile
import unittest

from stratum import Stratum

from . import QUICK

OPS = 1200 if QUICK else 15000


class ModelBasedTest(unittest.TestCase):
    def run_model(self, seed, ops=OPS, **options):
        directory = tempfile.mkdtemp(prefix="stratum-model-")
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        options.setdefault("sync", False)
        options.setdefault("memtable_bytes", 4096)
        options.setdefault("block_size", 256)

        rng = random.Random(seed)
        keys = [b"key%04d" % i for i in range(400)]
        db = Stratum(directory, **options)
        model = {}
        counts = {"put": 0, "delete": 0, "get": 0, "scan": 0, "reopen": 0}

        def random_bound():
            roll = rng.random()
            if roll < 0.2:
                return None
            if roll < 0.9:
                return rng.choice(keys)
            return b"key%04d~" % rng.randrange(400)  # falls between two keys

        try:
            for step in range(ops):
                where = f"seed={seed} step={step}"
                roll = rng.random()
                if roll < 0.45:
                    key = rng.choice(keys)
                    value = rng.randbytes(rng.choice((0, 1, 8, 50, 300)))
                    db.put(key, value)
                    model[key] = value
                    counts["put"] += 1
                elif roll < 0.60:
                    key = rng.choice(keys)
                    db.delete(key)
                    model.pop(key, None)
                    counts["delete"] += 1
                elif roll < 0.85:
                    key = rng.choice(keys)
                    self.assertEqual(db.get(key), model.get(key), where)
                    self.assertEqual(key in db, key in model, where)
                    counts["get"] += 1
                elif roll < 0.95:
                    start, end = random_bound(), random_bound()
                    limit = rng.choice((None, None, 1, 5, 50))
                    expected = [
                        (k, model[k])
                        for k in sorted(model)
                        if (start is None or k >= start) and (end is None or k < end)
                    ]
                    if limit is not None:
                        expected = expected[:limit]
                    self.assertEqual(list(db.scan(start, end, limit)), expected, where)
                    counts["scan"] += 1
                elif roll < 0.97:
                    db.close()
                    db = Stratum(directory, **options)
                    counts["reopen"] += 1
                elif roll < 0.98:
                    db.compact()
                else:
                    db.flush()

            # final, exhaustive comparison
            self.assertEqual(list(db.scan()), sorted(model.items()), f"seed={seed}")
            for key in keys:
                self.assertEqual(db.get(key), model.get(key), f"seed={seed}")
            db.close()
            db = Stratum(directory, **options)
            self.assertEqual(list(db.scan()), sorted(model.items()), f"seed={seed}")
        finally:
            db.close()
        return counts

    def test_with_background_compaction(self):
        for seed in (1, 2, 3):
            counts = self.run_model(seed)
            self.assertGreater(counts["scan"], 50)
            self.assertGreater(counts["reopen"], 5)

    def test_without_background_compaction(self):
        self.run_model(4, background_compaction=False)

    def test_aggressive_compaction(self):
        # merge as soon as two tables exist: maximum compaction churn
        self.run_model(5, compaction_min_tables=2, memtable_bytes=1024)

    def test_fsync_on_every_write(self):
        self.run_model(6, ops=OPS // 4, sync=True)

    def test_no_block_cache_and_tiny_bloom(self):
        # 1 bit per key = lots of Bloom false positives; results must not change
        self.run_model(7, block_cache_blocks=0, bloom_bits_per_key=1)

    def test_everything_in_memtable(self):
        self.run_model(8, ops=OPS // 4, memtable_bytes=64 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
