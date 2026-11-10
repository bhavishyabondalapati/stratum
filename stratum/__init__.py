"""Stratum: a persistent key-value storage engine built on an LSM tree."""

from .engine import Stratum
from .util import CorruptionError, StratumError

__all__ = ["Stratum", "StratumError", "CorruptionError"]
__version__ = "1.0.0"
