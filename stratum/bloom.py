"""A Bloom filter: a compact "definitely not here / maybe here" set.

It is a bit array of m bits plus k hash functions. Adding a key sets k bits.
A lookup checks the same k bits: if any is 0 the key was never added (no
false negatives); if all are 1 the key is *probably* present.

Instead of k independent hash functions we use "double hashing"
(Kirsch-Mitzenmacher): take two 64-bit hashes h1, h2 and use
h1 + i*h2 for i = 0..k-1. This is as good in practice and much cheaper.
The two hashes come from one BLAKE2b digest (hashlib, standard library).
"""

import hashlib
import math
import struct

_HEADER = struct.Struct("<QI")  # num_bits, num_hashes


def hash_pair(key):
    """Two 64-bit hashes of ``key``. h2 is forced odd so it is never 0."""
    digest = hashlib.blake2b(key, digest_size=16).digest()
    h1 = int.from_bytes(digest[:8], "little")
    h2 = int.from_bytes(digest[8:], "little") | 1
    return h1, h2


def optimal_num_hashes(bits_per_key):
    """k = (m/n) * ln 2 minimises the false-positive rate."""
    return max(1, min(30, round(bits_per_key * math.log(2))))


def theoretical_fpr(num_keys, num_bits, num_hashes):
    """Expected false-positive rate: (1 - e^(-k*n/m)) ^ k."""
    if num_keys == 0:
        return 0.0
    return (1.0 - math.exp(-num_hashes * num_keys / num_bits)) ** num_hashes


class BloomFilter:
    def __init__(self, num_bits, num_hashes, bits=None):
        if num_bits < 1 or num_hashes < 1:
            raise ValueError("num_bits and num_hashes must be >= 1")
        self.num_bits = num_bits
        self.num_hashes = num_hashes
        nbytes = (num_bits + 7) // 8
        if bits is None:
            self._bits = bytearray(nbytes)
        else:
            if len(bits) != nbytes:
                raise ValueError("bit array has the wrong length")
            self._bits = bytearray(bits)

    @classmethod
    def for_capacity(cls, num_keys, bits_per_key=10):
        """Size a filter for ``num_keys`` keys at ``bits_per_key`` bits each."""
        num_bits = max(64, int(num_keys * bits_per_key))
        return cls(num_bits, optimal_num_hashes(bits_per_key))

    def add(self, key):
        self.add_hashes(*hash_pair(key))

    def add_hashes(self, h1, h2):
        m = self.num_bits
        bits = self._bits
        for _ in range(self.num_hashes):
            pos = h1 % m
            bits[pos >> 3] |= 1 << (pos & 7)
            h1 += h2

    def might_contain(self, key):
        h1, h2 = hash_pair(key)
        m = self.num_bits
        bits = self._bits
        for _ in range(self.num_hashes):
            pos = h1 % m
            if not bits[pos >> 3] & (1 << (pos & 7)):
                return False
            h1 += h2
        return True

    __contains__ = might_contain

    def to_bytes(self):
        return _HEADER.pack(self.num_bits, self.num_hashes) + bytes(self._bits)

    @classmethod
    def from_bytes(cls, data):
        num_bits, num_hashes = _HEADER.unpack_from(data)
        return cls(num_bits, num_hashes, data[_HEADER.size:])
