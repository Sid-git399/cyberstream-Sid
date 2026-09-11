"""
Duplicate Handling (Section 62).

Strategy (documented per the spec's requirement):
  A duplicate is defined as an event sharing the SAME event_id as one
  already seen within the current dedup horizon. event_id is generated
  by the producer (SecurityEvent.new_id()) and is expected to be
  globally unique per real emission — so a repeated event_id can only
  mean the same physical event was re-delivered (Kafka at-least-once
  redelivery, a retried producer send, or a replayed micro-batch after
  a checkpoint rollback).

  We deliberately do NOT try to fuzzy-match "logically identical"
  events (same source/host/action/timestamp but different event_id) as
  duplicates — that is normal, expected traffic (e.g. two genuinely
  separate failed logins one second apart look identical on those
  fields) and collapsing them would hide real signal from the brute
  force detector.

Implementation:
  A bounded LRU-style set of recently seen event_ids. Bounded so memory
  use doesn't grow unboundedly across a long-running stream — this
  mirrors how Spark Structured Streaming's own `dropDuplicates` with a
  watermark works (only dedups within a bounded recent horizon), and is
  a deliberate, documented trade-off: an event_id that reappears after
  falling out of the window is treated as new.
"""

from __future__ import annotations
from collections import OrderedDict
from typing import Any, Dict, List, Tuple


class DuplicateFilter:
    """Bounded recent-event_id filter. See module docstring for rationale."""

    def __init__(self, max_tracked: int = 500_000):
        self.max_tracked = max_tracked
        self._seen: "OrderedDict[str, None]" = OrderedDict()
        self.duplicate_count = 0
        self.unique_count = 0

    def is_duplicate(self, event_id: str) -> bool:
        if event_id in self._seen:
            self.duplicate_count += 1
            # refresh recency
            self._seen.move_to_end(event_id)
            return True
        self._seen[event_id] = None
        self.unique_count += 1
        if len(self._seen) > self.max_tracked:
            self._seen.popitem(last=False)  # evict oldest
        return False

    def stats(self) -> Dict[str, int]:
        return {"unique": self.unique_count, "duplicates": self.duplicate_count,
                "tracked": len(self._seen)}


def deduplicate_events(events: List[Dict[str, Any]],
                        dup_filter: "DuplicateFilter | None" = None
                        ) -> Tuple[List[Dict[str, Any]], int]:
    """
    Split a batch into (unique_events, duplicate_count) using event_id.
    A fresh, batch-local DuplicateFilter is used if none is passed —
    callers that need cross-batch dedup (the streaming job) should keep
    one DuplicateFilter alive across calls.
    """
    dup_filter = dup_filter or DuplicateFilter()
    unique = []
    dup_count = 0
    for e in events:
        eid = e.get("event_id")
        if eid and dup_filter.is_duplicate(eid):
            dup_count += 1
            continue
        unique.append(e)
    return unique, dup_count
