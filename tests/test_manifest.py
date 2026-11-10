import os
import shutil
import tempfile
import unittest

from stratum.manifest import MANIFEST_NAME, Manifest
from stratum.util import CorruptionError, atomic_write


class ManifestTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-manifest-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def test_missing_manifest_means_new_database(self):
        self.assertIsNone(Manifest.load(self.dir))

    def test_save_and_load(self):
        Manifest([9, 4, 2], log_number=10, next_file_number=11).save(self.dir)
        loaded = Manifest.load(self.dir)
        self.assertEqual(loaded.tables, [9, 4, 2])  # order is preserved
        self.assertEqual(loaded.log_number, 10)
        self.assertEqual(loaded.next_file_number, 11)
        self.assertEqual(os.listdir(self.dir), [MANIFEST_NAME])  # no temp left

    def test_save_replaces_previous_version(self):
        Manifest([1]).save(self.dir)
        Manifest([2, 1]).save(self.dir)
        self.assertEqual(Manifest.load(self.dir).tables, [2, 1])

    def test_leftover_temp_file_does_not_affect_load(self):
        """A crash mid-update leaves MANIFEST.tmp behind; the real MANIFEST
        must still be the old, complete version."""
        Manifest([1], 2, 3).save(self.dir)
        with open(os.path.join(self.dir, MANIFEST_NAME + ".tmp"), "wb") as f:
            f.write(b'{"tables": [1, 2, half-writ')
        self.assertEqual(Manifest.load(self.dir).tables, [1])

    def test_corruption_is_detected(self):
        path = os.path.join(self.dir, MANIFEST_NAME)
        good = Manifest([3, 1], 4, 5).encode()
        bad_versions = [
            b"",
            b"garbage",
            good.replace(b"[3, 1]", b"[3, 2]"),  # body no longer matches crc
            good[: len(good) // 2],  # truncated
            b'{"tables": []}\ncrc32:zzzzzzzz\n',
            b'{"tables": []}\nnot-a-crc-line\n',
        ]
        for data in bad_versions:
            with open(path, "wb") as f:
                f.write(data)
            with self.assertRaises(CorruptionError):
                Manifest.load(self.dir)

    def test_missing_field_is_corruption(self):
        import zlib

        body = b'{"tables": [1]}'
        with self.assertRaises(CorruptionError):
            Manifest.decode(body + b"\ncrc32:%08x\n" % zlib.crc32(body))

    def test_atomic_write_overwrites(self):
        path = os.path.join(self.dir, "file")
        atomic_write(path, b"one")
        atomic_write(path, b"two")
        with open(path, "rb") as f:
            self.assertEqual(f.read(), b"two")
        self.assertEqual(os.listdir(self.dir), ["file"])


if __name__ == "__main__":
    unittest.main()
