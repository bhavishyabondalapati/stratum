import os
import shutil
import tempfile
import unittest

from stratum.wal import WAL, encode_record, read_records, recover


class WALTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-wal-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.path = os.path.join(self.dir, "000001.wal")

    def write(self, records, sync=False):
        wal = WAL(self.path, sync=sync)
        for key, value in records:
            wal.append(key, value)
        wal.close()

    def test_roundtrip_puts_and_deletes(self):
        records = [
            (b"a", b"1"),
            (b"b", b""),  # empty value is NOT a delete
            (b"a", None),  # delete
            (b"", b"empty key"),
            (b"big", b"x" * 100_000),
            (b"\x00\xff", b"\x00binary\xff"),
        ]
        self.write(records, sync=True)
        got, valid, total = read_records(self.path)
        self.assertEqual(got, records)
        self.assertEqual(valid, total)

    def test_append_after_reopen(self):
        self.write([(b"a", b"1")])
        self.write([(b"b", b"2")])
        self.assertEqual(recover(self.path), [(b"a", b"1"), (b"b", b"2")])

    def test_empty_file(self):
        self.write([])
        self.assertEqual(read_records(self.path), ([], 0, 0))

    def test_torn_tail_at_every_byte_offset(self):
        """Cut the file at every possible length: recovery must return
        exactly the records that fit completely, and truncate the rest."""
        records = [(b"key%d" % i, b"v" * i if i % 3 else None) for i in range(12)]
        self.write(records)
        with open(self.path, "rb") as f:
            full = f.read()
        boundaries = [0]
        for key, value in records:
            boundaries.append(boundaries[-1] + len(encode_record(key, value)))
        self.assertEqual(boundaries[-1], len(full))

        for cut in range(len(full) + 1):
            with open(self.path, "wb") as f:
                f.write(full[:cut])
            complete = max(i for i, b in enumerate(boundaries) if b <= cut)
            self.assertEqual(recover(self.path), records[:complete], cut)
            self.assertEqual(os.path.getsize(self.path), boundaries[complete])

    def test_writes_continue_cleanly_after_torn_tail(self):
        self.write([(b"a", b"1"), (b"b", b"2")])
        size = os.path.getsize(self.path)
        os.truncate(self.path, size - 1)  # tear the last record
        self.assertEqual(recover(self.path), [(b"a", b"1")])
        self.write([(b"c", b"3")])
        self.assertEqual(recover(self.path), [(b"a", b"1"), (b"c", b"3")])

    def test_bit_flip_stops_replay_at_bad_record(self):
        records = [(b"k%d" % i, b"value%d" % i) for i in range(5)]
        self.write(records)
        with open(self.path, "rb") as f:
            data = bytearray(f.read())
        offset = len(encode_record(*records[0])) + len(encode_record(*records[1]))
        data[offset + 10] ^= 0x01  # corrupt the third record's payload
        with open(self.path, "wb") as f:
            f.write(data)
        self.assertEqual(recover(self.path), records[:2])
        self.assertEqual(os.path.getsize(self.path), offset)

    def test_zero_filled_tail_is_not_a_record(self):
        self.write([(b"a", b"1")])
        with open(self.path, "ab") as f:
            f.write(b"\x00" * 64)
        self.assertEqual(recover(self.path), [(b"a", b"1")])

    def test_garbage_with_valid_crc_but_bad_structure(self):
        import struct
        import zlib

        for payload in (
            struct.pack("<BI", 9, 1) + b"k",  # unknown op
            struct.pack("<BI", 1, 99) + b"k",  # key length past the end
            struct.pack("<BI", 2, 1) + b"k" + b"extra",  # delete with a value
        ):
            with open(self.path, "wb") as f:
                f.write(struct.pack("<II", zlib.crc32(payload), len(payload)))
                f.write(payload)
            self.assertEqual(recover(self.path), [])
            self.assertEqual(os.path.getsize(self.path), 0)


if __name__ == "__main__":
    unittest.main()
