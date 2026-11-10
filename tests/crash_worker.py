"""Child process for the crash-recovery test. It is meant to be SIGKILLed.

Usage: python crash_worker.py <db_dir> <seed> <first_op_index> <sync 0|1>

It applies an endless, deterministic stream of operations. Operation number
``i`` depends only on ``(seed, i)``, so the parent test can recompute
exactly what the worker did. After each operation returns (= the write is
acknowledged) the worker writes the operation's index to stdout.
"""

import os
import random
import sys

KEY_SPACE = 300


def op_for(seed, index):
    """The index-th operation: ``(key, value)``; value None means delete."""
    rng = random.Random(seed * 1_000_003 + index)
    key = b"key%04d" % rng.randrange(KEY_SPACE)
    if rng.random() < 0.2:
        return key, None
    # the index makes every written value unique
    return key, b"%d:" % index + rng.randbytes(rng.randrange(0, 150))


def apply_op(target, key, value):
    """Apply an op to a dict (the model) or to a Stratum engine."""
    if isinstance(target, dict):
        if value is None:
            target.pop(key, None)
        else:
            target[key] = value
    elif value is None:
        target.delete(key)
    else:
        target.put(key, value)


def main():
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from stratum import Stratum

    directory, seed, index, sync = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4] == "1"
    # Tiny memtable and eager compaction, so that a kill is likely to land
    # in the middle of a flush, a compaction or a manifest update.
    db = Stratum(directory, sync=sync, memtable_bytes=6000,
                 block_size=512, compaction_min_tables=3)
    while True:
        key, value = op_for(seed, index)
        apply_op(db, key, value)
        os.write(1, b"%d\n" % index)  # unbuffered: the acknowledgement
        index += 1


if __name__ == "__main__":
    main()
