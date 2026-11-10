"""A skip list: a sorted map with O(log n) expected insert and lookup.

Picture a sorted linked list (level 0) with extra "express lanes" stacked on
top. Each node is promoted to the next lane with probability P, so higher
lanes hold fewer nodes and let a search skip over long stretches.

Concurrency: one writer plus any number of readers may use the list at the
same time under CPython's GIL. A new node has all of its own ``next``
pointers filled in *before* it is linked into the list, bottom lane first,
so a reader always sees a consistent chain.
"""

import random

from .util import MISSING

MAX_LEVEL = 20  # enough lanes for millions of keys at P = 0.25
P = 0.25


class _Node:
    __slots__ = ("key", "value", "next")

    def __init__(self, key, value, level):
        self.key = key
        self.value = value
        self.next = [None] * level


class SkipList:
    def __init__(self, seed=None):
        self._head = _Node(None, None, MAX_LEVEL)
        self._level = 1  # number of lanes currently in use
        self._len = 0
        self._random = random.Random(seed).random

    def __len__(self):
        return self._len

    def _random_level(self):
        level = 1
        rand = self._random
        while level < MAX_LEVEL and rand() < P:
            level += 1
        return level

    def insert(self, key, value):
        """Insert or overwrite ``key``. Returns the old value, or MISSING."""
        update = [None] * MAX_LEVEL  # last node before `key` in each lane
        node = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = node.next[i]
            while nxt is not None and nxt.key < key:
                node = nxt
                nxt = node.next[i]
            update[i] = node

        nxt = node.next[0]
        if nxt is not None and nxt.key == key:
            old = nxt.value
            nxt.value = value
            return old

        level = self._random_level()
        if level > self._level:
            for i in range(self._level, level):
                update[i] = self._head
            self._level = level

        new = _Node(key, value, level)
        for i in range(level):  # 1) point the new node forward
            new.next[i] = update[i].next[i]
        for i in range(level):  # 2) then publish it, bottom lane first
            update[i].next[i] = new
        self._len += 1
        return MISSING

    def _find_ge(self, key):
        """First node whose key is >= ``key`` (or None)."""
        node = self._head
        for i in range(self._level - 1, -1, -1):
            nxt = node.next[i]
            while nxt is not None and nxt.key < key:
                node = nxt
                nxt = node.next[i]
        return node.next[0]

    def get(self, key, default=MISSING):
        node = self._find_ge(key)
        if node is not None and node.key == key:
            return node.value
        return default

    def __contains__(self, key):
        return self.get(key) is not MISSING

    def items(self, start=None):
        """Yield (key, value) in sorted order, beginning at ``start``."""
        node = self._head.next[0] if start is None else self._find_ge(start)
        while node is not None:
            yield node.key, node.value
            node = node.next[0]

    def __iter__(self):
        return self.items()
