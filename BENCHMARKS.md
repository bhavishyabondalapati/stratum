# Stratum benchmarks

Measured on 2026-11-10 by `python bench.py`. Every number below was produced by that run - nothing is estimated.

**Machine:** Apple M4 Pro, 14 cores, Darwin 25.6.0 (arm64), Python 3.12.16

**Workload:** 16-byte keys, 100-byte values, inserted in random order. Default engine settings (4 MiB memtable, 4 KiB blocks, 10 Bloom bits/key, 4096-block cache, background compaction on). Single client thread.

## Write throughput

| Keys | Time | Writes/s | MB/s (key+value) | Flushes | Compactions | Tables at end | On disk |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 0.6 s | 175,590 | 20.4 | 4 | 1 | 1 | 11.8 MB |
| 1,000,000 | 8.4 s | 119,638 | 13.9 | 42 | 12 | 2 | 123.6 MB |

WAL fsync is **off** in the table above (each write still reaches the OS before it is acknowledged, so it survives a process crash). The time includes memtable flushes and sharing the CPU with the compaction thread.

With `sync=True` (fsync after every write): **51,039 writes/s** (5,000 writes). Note that on macOS `fsync` does not force the drive to flush its own cache, so this is faster than a true power-loss-safe sync would be.

## Random-read latency (point lookups)

20,000 lookups each, timed individually, right after loading (several SSTables on disk).

| Keys | Tables | Existing key p50 | p99 | mean | Missing key p50 | p99 | mean | Block-cache hit rate |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100,000 | 1 | 2.7 µs | 16.7 µs | 4.3 µs | 1.5 µs | 2.3 µs | 1.6 µs | 85% |
| 1,000,000 | 2 | 10.4 µs | 24.9 µs | 10.5 µs | 2.5 µs | 11.3 µs | 2.8 µs | 13% |

After a manual full compaction (a single SSTable):

| Keys | Existing key p50 | p99 | Compaction time | On disk |
|---:|---:|---:|---:|---:|
| 100,000 | 1.8 µs | 9.2 µs | 0.2 s | 12.6 MB |
| 1,000,000 | 8.3 µs | 11.4 µs | 1.6 s | 126.3 MB |

## Range scans

| Keys | Full scan | Keys/s | Keys/s after full compaction | 100-key scan | Keys/s (short scans) |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 0.03 s | 3,533,543 | 3,634,535 | 0.04 ms | 2,853,203 |
| 1,000,000 | 0.31 s | 3,178,320 | 3,654,339 | 0.05 ms | 2,046,434 |

## Bloom filter: false-positive rate vs. theory

Stand-alone filter holding 100,000 keys, probed with 200,000 keys that were never added. Theory: `(1 - e^(-k*n/m))^k`.

| Bits per key | Hash functions (k) | Measured FPR | Theoretical FPR | Measured / theory |
|---:|---:|---:|---:|---:|
| 4 | 3 | 14.5690% | 14.6892% | 0.99 |
| 6 | 4 | 5.6315% | 5.6057% | 1.00 |
| 8 | 6 | 2.1420% | 2.1577% | 0.99 |
| 10 | 7 | 0.8245% | 0.8194% | 1.01 |
| 12 | 8 | 0.3195% | 0.3142% | 1.02 |
| 16 | 11 | 0.0415% | 0.0459% | 0.90 |

Inside the engine (10 bits/key), during the missing-key lookups above:

| Keys | Bloom filter checks | False positives (wasted block reads) | Rate |
|---:|---:|---:|---:|
| 100,000 | 19,999 | 149 | 0.745% |
| 1,000,000 | 40,000 | 295 | 0.737% |

## Other

| Keys | Background compaction settle time after load | Reopen time (after full compaction) |
|---:|---:|---:|
| 100,000 | 0.0 s | 3.7 ms |
| 1,000,000 | 0.0 s | 12.0 ms |

## Reading these numbers

- Stratum is pure Python, so every operation pays interpreter overhead. Compare the *shape* of these numbers (how they change with data size) rather than the absolute values against engines written in C or C++.
- Missing-key lookups are cheaper than hits: the Bloom filters answer "not here" without reading a data block, except for the ~0.8% false positives.
- Existing-key latency depends mostly on the block cache. When the data fits in the cache, most lookups are a few in-memory steps; when it does not, a typical lookup pays for a `pread` and for decoding one 4 KiB block. The block-cache hit rate column shows which case applies.
- The files were just written, so they are in the operating system's page cache. These are warm-cache numbers; a cold disk read would add to them.
- Write throughput drops as the dataset grows because more time goes to flushes and to the compaction thread, which shares the interpreter (GIL).
- Bloom rows with very low rates (16 bits/key) rest on fewer than 100 false positives, so their measured/theory ratio is noisier.
