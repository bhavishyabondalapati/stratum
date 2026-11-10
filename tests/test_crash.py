"""Crash-recovery test: kill -9 a writing process again and again.

Each round starts ``crash_worker.py``, lets it write for a random while,
kills it with SIGKILL (no cleanup code runs), then reopens the database in
this process and checks it.

What must hold after every crash. Let N be the last operation the worker
acknowledged. The worker is single-threaded, so at most one more operation
(N+1) can have been in flight when it died. Therefore the database must
equal the model after exactly N operations, or after exactly N+1:

  * no acknowledged write is lost        (state is not "older" than N)
  * nothing is corrupted or half-applied (state is one of two exact states)

The next round continues from whatever state was found, so damage would
also accumulate and be caught by later rounds.
"""

import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from stratum import Stratum
from stratum.manifest import Manifest

from . import QUICK
from .crash_worker import KEY_SPACE, apply_op, op_for

WORKER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crash_worker.py")
ROUNDS = int(os.environ.get("STRATUM_CRASH_ROUNDS", "10" if QUICK else "100"))


class CrashRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="stratum-crash-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def run_worker_and_kill(self, seed, first_index, sync, rng):
        """Run the worker, SIGKILL it, and return the index of the last
        operation it acknowledged (``first_index - 1`` if none)."""
        proc = subprocess.Popen(
            [sys.executable, WORKER, self.dir, str(seed), str(first_index),
             "1" if sync else "0"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        last_acked = first_index - 1
        try:
            if rng.random() < 0.3:
                # Kill after a short random time: often lands during
                # interpreter startup or during crash recovery itself.
                time.sleep(rng.uniform(0.0, 0.15))
            else:
                # Kill after a random number of acknowledged writes.
                for _ in range(rng.randrange(1, 3000)):
                    line = proc.stdout.readline()
                    if not line:
                        break  # worker died on its own; reported below
                    last_acked = int(line)
        finally:
            proc.kill()  # SIGKILL: the worker gets no chance to clean up
        # Acks the worker wrote before dying are still sitting in the pipe.
        rest = proc.stdout.read()
        err = proc.stderr.read()
        proc.stdout.close()
        proc.stderr.close()
        proc.wait()
        if proc.returncode != -signal.SIGKILL:
            self.fail(f"worker exited by itself ({proc.returncode}):\n{err.decode()}")
        lines = rest.split(b"\n")
        self.assertEqual(lines[-1], b"", "an ack line was cut in half")
        if len(lines) > 1:
            last_acked = int(lines[-2])
        return last_acked

    def test_sigkill_never_loses_acknowledged_writes(self):
        seed = int(os.environ.get("STRATUM_CRASH_SEED", "20240607"))
        rng = random.Random(seed)
        model = {}
        next_index = 0
        in_flight_applied = 0
        killed_before_first_ack = 0

        for round_number in range(ROUNDS):
            sync = round_number % 4 == 0  # most rounds skip fsync to go faster
            proc_first = next_index
            last_acked = self.run_worker_and_kill(seed, proc_first, sync, rng)
            where = f"seed={seed} round={round_number} first_index={proc_first}"

            db = Stratum(self.dir, sync=False)
            try:
                actual = dict(db.scan())
                self.assertGreaterEqual(last_acked, proc_first - 1, where)

                # Advance the model to N, then consider N+1.
                for index in range(next_index, last_acked + 1):
                    apply_op(model, *op_for(seed, index))
                with_in_flight = dict(model)
                apply_op(with_in_flight, *op_for(seed, last_acked + 1))

                if actual == model:
                    next_index = last_acked + 1
                elif actual == with_in_flight:
                    model = with_in_flight
                    next_index = last_acked + 2
                    in_flight_applied += 1
                else:
                    self.fail(self.describe_mismatch(actual, model, with_in_flight, where))

                # Point lookups must agree with the scan, for every key.
                for i in range(KEY_SPACE):
                    key = b"key%04d" % i
                    self.assertEqual(db.get(key), model.get(key), where)
                if last_acked == proc_first - 1:
                    killed_before_first_ack += 1
            finally:
                db.close()

            # The directory holds exactly the files the MANIFEST accounts for.
            manifest = Manifest.load(self.dir)
            on_disk = sorted(f for f in os.listdir(self.dir) if f.endswith(".sst"))
            self.assertEqual(on_disk, sorted("%06d.sst" % n for n in manifest.tables), where)
            self.assertEqual([f for f in os.listdir(self.dir) if f.endswith(".tmp")], [], where)

        self.assertGreater(next_index, ROUNDS * 50, "the workers barely wrote anything")
        if os.environ.get("STRATUM_VERBOSE"):
            print(
                f"\ncrash test: {ROUNDS} SIGKILLs, {next_index} operations verified, "
                f"{in_flight_applied} in-flight ops found applied, "
                f"{killed_before_first_ack} kills before the first ack"
            )

    def describe_mismatch(self, actual, model, with_in_flight, where):
        lost = {k for k in model if k not in actual}
        extra = {k for k in actual if k not in model}
        differ = {k for k in actual if k in model and actual[k] != model[k]}
        return (
            f"{where}: recovered state matches neither N nor N+1 operations. "
            f"missing={sorted(lost)[:5]} unexpected={sorted(extra)[:5]} "
            f"different={sorted(differ)[:5]}"
        )


if __name__ == "__main__":
    unittest.main()
