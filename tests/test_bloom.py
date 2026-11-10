import unittest

from stratum.bloom import (
    BloomFilter,
    hash_pair,
    optimal_num_hashes,
    theoretical_fpr,
)


class BloomFilterTest(unittest.TestCase):
    def test_no_false_negatives(self):
        keys = [b"key-%d" % i for i in range(5000)]
        bf = BloomFilter.for_capacity(len(keys), bits_per_key=10)
        for key in keys:
            bf.add(key)
        for key in keys:
            self.assertTrue(bf.might_contain(key))
            self.assertIn(key, bf)

    def test_empty_filter_contains_nothing(self):
        bf = BloomFilter.for_capacity(100)
        self.assertFalse(bf.might_contain(b"anything"))

    def test_false_positive_rate_close_to_theory(self):
        n = 20000
        bf = BloomFilter.for_capacity(n, bits_per_key=10)
        for i in range(n):
            bf.add(b"present-%d" % i)
        probes = 50000
        false_positives = sum(
            bf.might_contain(b"absent-%d" % i) for i in range(probes)
        )
        measured = false_positives / probes
        theory = theoretical_fpr(n, bf.num_bits, bf.num_hashes)
        self.assertAlmostEqual(theory, 0.0082, places=3)
        # allow generous sampling noise around ~0.8%
        self.assertLess(measured, theory * 1.5)
        self.assertGreater(measured, theory * 0.5)

    def test_serialization_roundtrip(self):
        bf = BloomFilter.for_capacity(1000, bits_per_key=8)
        for i in range(1000):
            bf.add(b"%d" % i)
        clone = BloomFilter.from_bytes(bf.to_bytes())
        self.assertEqual(clone.num_bits, bf.num_bits)
        self.assertEqual(clone.num_hashes, bf.num_hashes)
        for i in range(1000):
            self.assertTrue(clone.might_contain(b"%d" % i))
        self.assertEqual(clone.to_bytes(), bf.to_bytes())

    def test_parameters(self):
        self.assertEqual(optimal_num_hashes(10), 7)
        self.assertEqual(optimal_num_hashes(0.5), 1)
        self.assertEqual(optimal_num_hashes(1000), 30)
        self.assertEqual(theoretical_fpr(0, 100, 3), 0.0)
        self.assertEqual(BloomFilter.for_capacity(0).num_bits, 64)
        with self.assertRaises(ValueError):
            BloomFilter(0, 1)
        with self.assertRaises(ValueError):
            BloomFilter(64, 3, bits=b"too short")

    def test_hash_pair_is_deterministic_and_h2_odd(self):
        self.assertEqual(hash_pair(b"abc"), hash_pair(b"abc"))
        self.assertNotEqual(hash_pair(b"abc"), hash_pair(b"abd"))
        for i in range(100):
            self.assertEqual(hash_pair(b"%d" % i)[1] & 1, 1)

    def test_add_hashes_matches_add(self):
        a = BloomFilter(1024, 5)
        b = BloomFilter(1024, 5)
        for i in range(50):
            a.add(b"%d" % i)
            b.add_hashes(*hash_pair(b"%d" % i))
        self.assertEqual(a.to_bytes(), b.to_bytes())


if __name__ == "__main__":
    unittest.main()
