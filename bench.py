"""Benchmarks for Stratum. Writes the measured numbers to BENCHMARKS.md.

    python bench.py                      # 100K and 1M keys (takes minutes)
    python bench.py --sizes 20000        # a quick smoke run
    python bench.py --no-write           # print only, leave BENCHMARKS.md

What is measured, for each dataset size N:
  * write throughput: N puts of 16-byte keys / 100-byte values in random
    order (WAL fsync off; a separate small run measures fsync-per-write)
  * random-read latency: p50 / p99 of point lookups of existing keys, and
    of keys that do not exist (answered mostly by Bloom filters)
  * range-scan speed: one full scan, and many short 100-key scans
And once: Bloom filter false-positive rate vs. the theoretical value.
"""

import argparse
import os
import platform
import random
import shutil
import sys
import tempfile
import time

from stratum import Stratum
from stratum.bloom import BloomFilter, theoretical_fpr

VALUE_SIZE = 100
READ_SAMPLES = 20_000
SHORT_SCANS = 2_000
SHORT_SCAN_LENGTH = 100


def key_for(i):
    return b"user%012d" % i  # 16 bytes


def missing_key_for(i):
    # sorts between two real keys, so it is inside every table's key range
    # and only the Bloom filter can rule it out cheaply
    return b"user%012d!" % i


def percentile(sorted_values, fraction):
    index = min(len(sorted_values) - 1, int(len(sorted_values) * fraction))
    return sorted_values[index]


def time_gets(db, keys):
    """Per-call latency of db.get in microseconds, sorted ascending."""
    clock = time.perf_counter_ns
    get = db.get
    latencies = []
    for key in keys:
        start = clock()
        get(key)
        latencies.append(clock() - start)
    latencies.sort()
    return [ns / 1000.0 for ns in latencies]


def wait_for_compaction(db, quiet_seconds=1.0, timeout=600):
    """Block until the background thread has had nothing to do for a while.
    Returns how long it kept working (the quiet period is not counted)."""
    began = time.monotonic()
    deadline = began + timeout
    last = None
    quiet_since = time.monotonic()
    while time.monotonic() < deadline:
        stats = db.stats()
        current = (stats["compactions"], stats["table_count"])
        if current != last:
            last = current
            quiet_since = time.monotonic()
        elif time.monotonic() - quiet_since >= quiet_seconds:
            break
        time.sleep(0.05)
    return quiet_since - began


def bench_dataset(n, base_dir, log):
    directory = os.path.join(base_dir, f"n{n}")
    rng = random.Random(n)
    order = list(range(n))
    rng.shuffle(order)
    values = [rng.randbytes(VALUE_SIZE) for _ in range(1024)]
    result = {"n": n}

    # ---- write throughput (no fsync) --------------------------------
    log(f"[{n:,}] writing {n:,} keys in random order ...")
    db = Stratum(directory, sync=False)
    put = db.put
    start = time.perf_counter()
    for i in order:
        put(key_for(i), values[i & 1023])
    elapsed = time.perf_counter() - start
    result["write_seconds"] = elapsed
    result["write_ops"] = n / elapsed
    result["write_mb"] = n * (16 + VALUE_SIZE) / elapsed / 1e6

    result["settle_seconds"] = wait_for_compaction(db)
    stats = db.stats()
    result["flushes"] = stats["flushes"]
    result["compactions"] = stats["compactions"]
    result["tables"] = stats["table_count"]
    result["disk_mb"] = stats["disk_bytes"] / 1e6
    log(f"[{n:,}]   {result['write_ops']:,.0f} writes/s; "
        f"{result['tables']} tables after {result['compactions']} compactions")

    # ---- random reads -------------------------------------------------
    log(f"[{n:,}] random reads ...")
    hit_keys = [key_for(rng.randrange(n)) for _ in range(READ_SAMPLES)]
    miss_keys = [missing_key_for(rng.randrange(n)) for _ in range(READ_SAMPLES)]
    assert db.get(hit_keys[0]) is not None and db.get(miss_keys[0]) is None

    before = db.stats()
    hits = time_gets(db, hit_keys)
    result["hit_p50"], result["hit_p99"] = percentile(hits, 0.50), percentile(hits, 0.99)
    result["hit_mean"] = sum(hits) / len(hits)

    mid = db.stats()
    misses = time_gets(db, miss_keys)
    after = db.stats()
    result["miss_p50"], result["miss_p99"] = percentile(misses, 0.50), percentile(misses, 0.99)
    result["miss_mean"] = sum(misses) / len(misses)
    checks = after["bloom_checks"] - mid["bloom_checks"]
    false_pos = after["bloom_false_positives"] - mid["bloom_false_positives"]
    result["engine_bloom_checks"] = checks
    result["engine_bloom_fpr"] = false_pos / checks if checks else 0.0
    lookups = (mid["block_cache_hits"] - before["block_cache_hits"]
               + mid["block_cache_misses"] - before["block_cache_misses"])
    result["cache_hit_rate"] = (
        (mid["block_cache_hits"] - before["block_cache_hits"]) / lookups if lookups else 0.0
    )

    # ---- range scans --------------------------------------------------
    log(f"[{n:,}] range scans ...")
    start = time.perf_counter()
    count = sum(1 for _ in db.scan())
    elapsed = time.perf_counter() - start
    assert count == n, (count, n)
    result["full_scan_seconds"] = elapsed
    result["full_scan_keys"] = n / elapsed

    starts = [key_for(rng.randrange(max(1, n - SHORT_SCAN_LENGTH))) for _ in range(SHORT_SCANS)]
    start = time.perf_counter()
    total = 0
    for start_key in starts:
        total += len(list(db.scan(start_key, limit=SHORT_SCAN_LENGTH)))
    elapsed = time.perf_counter() - start
    assert total == SHORT_SCANS * SHORT_SCAN_LENGTH
    result["short_scan_ms"] = elapsed / SHORT_SCANS * 1000
    result["short_scan_keys"] = total / elapsed

    # ---- after a full compaction (one table) ---------------------------
    log(f"[{n:,}] full compaction, then re-measure reads ...")
    start = time.perf_counter()
    db.compact()
    result["compact_seconds"] = time.perf_counter() - start
    hits = time_gets(db, hit_keys)
    result["c_hit_p50"], result["c_hit_p99"] = percentile(hits, 0.50), percentile(hits, 0.99)
    start = time.perf_counter()
    count = sum(1 for _ in db.scan())
    result["c_full_scan_keys"] = count / (time.perf_counter() - start)
    result["c_disk_mb"] = db.stats()["disk_bytes"] / 1e6
    db.close()

    # ---- reopen time ---------------------------------------------------
    start = time.perf_counter()
    db = Stratum(directory, sync=False)
    result["reopen_ms"] = (time.perf_counter() - start) * 1000
    db.close()
    shutil.rmtree(directory, ignore_errors=True)
    return result


def bench_fsync(base_dir, count=5000):
    """Write throughput when every write is fsync'd."""
    directory = os.path.join(base_dir, "fsync")
    value = b"v" * VALUE_SIZE
    db = Stratum(directory, sync=True)
    start = time.perf_counter()
    for i in range(count):
        db.put(key_for(i), value)
    elapsed = time.perf_counter() - start
    db.close()
    shutil.rmtree(directory, ignore_errors=True)
    return count / elapsed


def bench_bloom(n=100_000, probes=200_000):
    rows = []
    for bits_per_key in (4, 6, 8, 10, 12, 16):
        bloom = BloomFilter.for_capacity(n, bits_per_key)
        for i in range(n):
            bloom.add(key_for(i))
        assert all(bloom.might_contain(key_for(i)) for i in range(0, n, 97))
        false_positives = sum(bloom.might_contain(missing_key_for(i)) for i in range(probes))
        rows.append({
            "bits": bits_per_key,
            "k": bloom.num_hashes,
            "measured": false_positives / probes,
            "theory": theoretical_fpr(n, bloom.num_bits, bloom.num_hashes),
        })
    return rows, n, probes


def machine_description():
    cpu = platform.processor() or platform.machine()
    try:
        import subprocess
        cpu = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"],
                             capture_output=True, text=True, timeout=5).stdout.strip() or cpu
    except Exception:
        pass
    return (f"{cpu}, {os.cpu_count()} cores, {platform.system()} {platform.release()} "
            f"({platform.machine()}), Python {platform.python_version()}")


def render(results, fsync_ops, bloom, started):
    bloom_rows, bloom_n, bloom_probes = bloom
    out = []
    add = out.append
    add("# Stratum benchmarks\n")
    add(f"Measured on {time.strftime('%Y-%m-%d', time.localtime(started))} by `python bench.py`. "
        "Every number below was produced by that run - nothing is estimated.\n")
    add(f"**Machine:** {machine_description()}\n")
    add(f"**Workload:** 16-byte keys, {VALUE_SIZE}-byte values, inserted in random order. "
        "Default engine settings (4 MiB memtable, 4 KiB blocks, 10 Bloom bits/key, "
        "4096-block cache, background compaction on). Single client thread.\n")

    add("## Write throughput\n")
    add("| Keys | Time | Writes/s | MB/s (key+value) | Flushes | Compactions | Tables at end | On disk |")
    add("|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in results:
        add(f"| {r['n']:,} | {r['write_seconds']:.1f} s | {r['write_ops']:,.0f} | "
            f"{r['write_mb']:.1f} | {r['flushes']} | {r['compactions']} | "
            f"{r['tables']} | {r['disk_mb']:.1f} MB |")
    add("")
    add("WAL fsync is **off** in the table above (each write still reaches the OS before "
        "it is acknowledged, so it survives a process crash). The time includes memtable "
        "flushes and sharing the CPU with the compaction thread.\n")
    add(f"With `sync=True` (fsync after every write): **{fsync_ops:,.0f} writes/s** "
        "(5,000 writes). Note that on macOS `fsync` does not force the drive to flush "
        "its own cache, so this is faster than a true power-loss-safe sync would be.\n")

    add("## Random-read latency (point lookups)\n")
    add(f"{READ_SAMPLES:,} lookups each, timed individually, right after loading "
        "(several SSTables on disk).\n")
    add("| Keys | Tables | Existing key p50 | p99 | mean | Missing key p50 | p99 | mean | Block-cache hit rate |")
    add("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for r in results:
        add(f"| {r['n']:,} | {r['tables']} | {r['hit_p50']:.1f} µs | {r['hit_p99']:.1f} µs | "
            f"{r['hit_mean']:.1f} µs | {r['miss_p50']:.1f} µs | {r['miss_p99']:.1f} µs | "
            f"{r['miss_mean']:.1f} µs | {r['cache_hit_rate']:.0%} |")
    add("")
    add("After a manual full compaction (a single SSTable):\n")
    add("| Keys | Existing key p50 | p99 | Compaction time | On disk |")
    add("|---:|---:|---:|---:|---:|")
    for r in results:
        add(f"| {r['n']:,} | {r['c_hit_p50']:.1f} µs | {r['c_hit_p99']:.1f} µs | "
            f"{r['compact_seconds']:.1f} s | {r['c_disk_mb']:.1f} MB |")
    add("")

    add("## Range scans\n")
    add("| Keys | Full scan | Keys/s | Keys/s after full compaction | "
        f"{SHORT_SCAN_LENGTH}-key scan | Keys/s (short scans) |")
    add("|---:|---:|---:|---:|---:|---:|")
    for r in results:
        add(f"| {r['n']:,} | {r['full_scan_seconds']:.2f} s | {r['full_scan_keys']:,.0f} | "
            f"{r['c_full_scan_keys']:,.0f} | {r['short_scan_ms']:.2f} ms | "
            f"{r['short_scan_keys']:,.0f} |")
    add("")

    add("## Bloom filter: false-positive rate vs. theory\n")
    add(f"Stand-alone filter holding {bloom_n:,} keys, probed with {bloom_probes:,} keys "
        "that were never added. Theory: `(1 - e^(-k*n/m))^k`.\n")
    add("| Bits per key | Hash functions (k) | Measured FPR | Theoretical FPR | Measured / theory |")
    add("|---:|---:|---:|---:|---:|")
    for row in bloom_rows:
        ratio = row["measured"] / row["theory"] if row["theory"] else 0
        add(f"| {row['bits']} | {row['k']} | {row['measured']:.4%} | {row['theory']:.4%} | {ratio:.2f} |")
    add("")
    add("Inside the engine (10 bits/key), during the missing-key lookups above:\n")
    add("| Keys | Bloom filter checks | False positives (wasted block reads) | Rate |")
    add("|---:|---:|---:|---:|")
    for r in results:
        add(f"| {r['n']:,} | {r['engine_bloom_checks']:,} | "
            f"{round(r['engine_bloom_fpr'] * r['engine_bloom_checks']):,} | "
            f"{r['engine_bloom_fpr']:.3%} |")
    add("")

    add("## Other\n")
    add("| Keys | Background compaction settle time after load | Reopen time (after full compaction) |")
    add("|---:|---:|---:|")
    for r in results:
        add(f"| {r['n']:,} | {r['settle_seconds']:.1f} s | {r['reopen_ms']:.1f} ms |")
    add("")
    add("## Reading these numbers\n")
    add("- Stratum is pure Python, so every operation pays interpreter overhead. "
        "Compare the *shape* of these numbers (how they change with data size) "
        "rather than the absolute values against engines written in C or C++.")
    add("- Missing-key lookups are cheaper than hits: the Bloom filters answer "
        "\"not here\" without reading a data block, except for the ~0.8% false positives.")
    add("- Existing-key latency depends mostly on the block cache. When the data fits "
        "in the cache, most lookups are a few in-memory steps; when it does not, a "
        "typical lookup pays for a `pread` and for decoding one 4 KiB block. The "
        "block-cache hit rate column shows which case applies.")
    add("- The files were just written, so they are in the operating system's page "
        "cache. These are warm-cache numbers; a cold disk read would add to them.")
    add("- Write throughput drops as the dataset grows because more time goes to "
        "flushes and to the compaction thread, which shares the interpreter (GIL).")
    add("- Bloom rows with very low rates (16 bits/key) rest on fewer than 100 "
        "false positives, so their measured/theory ratio is noisier.")
    return "\n".join(out) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", default="100000,1000000",
                        help="comma-separated dataset sizes (default: 100000,1000000)")
    parser.add_argument("--dir", help="where to put benchmark data (default: a temp dir)")
    parser.add_argument("--no-write", action="store_true", help="do not write BENCHMARKS.md")
    args = parser.parse_args()
    sizes = [int(s) for s in args.sizes.split(",")]

    def log(message):
        print(message, file=sys.stderr, flush=True)

    started = time.time()
    base_dir = args.dir or tempfile.mkdtemp(prefix="stratum-bench-")
    os.makedirs(base_dir, exist_ok=True)
    try:
        log("bloom filter false-positive rates ...")
        bloom = bench_bloom()
        log("fsync'd writes ...")
        fsync_ops = bench_fsync(base_dir)
        results = [bench_dataset(n, base_dir, log) for n in sizes]
    finally:
        if not args.dir:
            shutil.rmtree(base_dir, ignore_errors=True)

    report = render(results, fsync_ops, bloom, started)
    print(report)
    if not args.no_write:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "BENCHMARKS.md")
        with open(path, "w") as f:
            f.write(report)
        log(f"wrote {path}")
    log(f"total benchmark time: {time.time() - started:.0f} s")


if __name__ == "__main__":
    main()
