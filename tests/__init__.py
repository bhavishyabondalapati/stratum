"""Stratum test suite (unittest). Run with: python -m unittest"""

import os

# Set STRATUM_QUICK=1 to shrink the randomized tests (used for the coverage
# run, where line tracing makes everything ~10x slower).
QUICK = os.environ.get("STRATUM_QUICK") == "1"
