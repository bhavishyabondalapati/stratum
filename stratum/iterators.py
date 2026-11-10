"""Merging several sorted streams into one, with "newest wins"."""

import heapq


def merge_iterators(sources):
    """Merge sorted ``(key, value)`` iterables into one sorted stream.

    ``sources`` must be ordered newest first. When the same key appears in
    several sources, only the entry from the newest one is yielded.
    Tombstones (value None) are passed through; the caller decides whether
    to hide or drop them.

    A min-heap holds one "current" entry per source. Heap items are
    ``(key, source_index, value, iterator)``; on equal keys the lower
    source_index (the newer source) comes out first.
    """
    heap = []
    for index, source in enumerate(sources):
        it = iter(source)
        for key, value in it:
            heap.append((key, index, value, it))
            break
    heapq.heapify(heap)

    heappop = heapq.heappop
    heapreplace = heapq.heapreplace
    first = True
    last_key = None
    while heap:
        key, index, value, it = heap[0]
        for next_key, next_value in it:
            heapreplace(heap, (next_key, index, next_value, it))
            break
        else:
            heappop(heap)
        if first or key != last_key:
            first = False
            last_key = key
            yield key, value
