import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.dedup import DuplicateFilter, deduplicate_events
from shared.data_quality import DataQualityTracker


def test_duplicate_filter_flags_repeats():
    f = DuplicateFilter()
    assert f.is_duplicate("evt-1") is False
    assert f.is_duplicate("evt-2") is False
    assert f.is_duplicate("evt-1") is True
    assert f.stats()["duplicates"] == 1
    assert f.stats()["unique"] == 2


def test_duplicate_filter_evicts_oldest_beyond_capacity():
    f = DuplicateFilter(max_tracked=3)
    for i in range(5):
        f.is_duplicate(f"evt-{i}")
    # evt-0 and evt-1 should have been evicted; reappearing looks "new"
    assert f.is_duplicate("evt-0") is False
    assert len(f._seen) <= 3 + 1  # +1 because the re-add above just happened


def test_deduplicate_events_splits_batch_correctly():
    events = [{"event_id": "a"}, {"event_id": "b"}, {"event_id": "a"}, {"event_id": "c"}]
    unique, dup_count = deduplicate_events(events)
    assert dup_count == 1
    assert [e["event_id"] for e in unique] == ["a", "b", "c"]


def test_data_quality_tracker_snapshot():
    dq = DataQualityTracker()
    dq.record_seen(100)
    dq.record_invalid(["hostname"])
    dq.record_invalid(["hostname"])
    dq.record_duplicate(3)
    dq.record_late(2)
    dq.record_error()

    snap = dq.snapshot()
    assert snap["total_events_seen"] == 100
    assert snap["invalid_events"] == 2
    assert snap["missing_field_breakdown"]["hostname"] == 2
    assert snap["duplicate_events"] == 3
    assert snap["late_events"] == 2
    assert snap["processing_errors"] == 1
