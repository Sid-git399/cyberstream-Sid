"""
Data Quality (Section 61).

Tracks the five metrics the spec explicitly asks for: invalid events,
missing fields, duplicate events, late events, and processing errors.
This is a plain in-process counter object — the streaming job holds one
instance across its lifetime (module-level singleton, see
streaming/spark_job.py) and the backend exposes its current values via
GET /api/pipeline/data-quality. Values reset when the Spark job restarts,
which is disclosed in the API response via `since`.
"""

from __future__ import annotations
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List


class DataQualityTracker:
    def __init__(self):
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.invalid_events = 0
        self.missing_field_counts: Counter = Counter()
        self.duplicate_events = 0
        self.late_events = 0
        self.processing_errors = 0
        self.total_seen = 0

    def record_invalid(self, missing_fields: List[str] | None = None):
        self.invalid_events += 1
        for f in (missing_fields or []):
            self.missing_field_counts[f] += 1

    def record_duplicate(self, count: int = 1):
        self.duplicate_events += count

    def record_late(self, count: int = 1):
        self.late_events += count

    def record_error(self):
        self.processing_errors += 1

    def record_seen(self, count: int = 1):
        self.total_seen += count

    def snapshot(self) -> Dict[str, Any]:
        total = max(self.total_seen, 1)
        return {
            "since": self.started_at,
            "total_events_seen": self.total_seen,
            "invalid_events": self.invalid_events,
            "invalid_event_pct": round(self.invalid_events / total * 100, 3),
            "missing_field_breakdown": dict(self.missing_field_counts.most_common(10)),
            "duplicate_events": self.duplicate_events,
            "late_events": self.late_events,
            "processing_errors": self.processing_errors,
        }


# Module-level singleton used by the streaming job / local runner so the
# backend and the pipeline can share one live counter within a process.
GLOBAL_TRACKER = DataQualityTracker()
