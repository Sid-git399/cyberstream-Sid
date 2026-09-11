import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timedelta, timezone
from shared.windowing import bucket_events, run_windowed_detector, WINDOW_SECONDS
from shared.detection import detect_brute_force, detect_privilege_escalation


def ts(offset_seconds=0):
    # Anchored to a fixed, safely mid-window timestamp so tests aren't
    # flaky around real wall-clock minute boundaries.
    anchor = datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
    return (anchor + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def evt(**kw):
    base = {"event_id": "e", "timestamp": ts(), "event_type": "authentication",
            "action": "login_failed", "status": "failure", "source_ip": "1.2.3.4",
            "username": "bob", "hostname": "WS-01"}
    base.update(kw)
    return base


def test_bucket_events_groups_by_window_and_key():
    events = [evt(timestamp=ts(i), hostname="WS-01") for i in range(5)]
    events += [evt(timestamp=ts(i), hostname="WS-02") for i in range(5)]
    buckets = bucket_events(events, window_seconds=60)
    # both hosts fall in the same 1-min bucket but are distinct groups
    assert len(buckets) == 2


def test_bucket_events_separates_distant_windows():
    events = [evt(timestamp=ts(0), hostname="WS-01"),
              evt(timestamp=ts(600), hostname="WS-01")]  # 10 min apart
    buckets = bucket_events(events, window_seconds=60)
    assert len(buckets) == 2  # different 1-minute windows


def test_run_windowed_detector_respects_window_size():
    # 10 failed logins spread across 3 minutes should NOT trigger brute
    # force in a single 1-minute window, even though the raw count
    # crosses the threshold across the whole batch.
    events = [evt(timestamp=ts(i * 20)) for i in range(10)]  # 20s apart -> spans 200s
    results = run_windowed_detector(detect_brute_force, "brute_force", events)
    assert results == []  # each 1-min bucket only has ~3 events


def test_run_windowed_detector_triggers_within_single_window():
    events = [evt(timestamp=ts(i)) for i in range(10)]  # all within 10s -> same window
    results = run_windowed_detector(detect_brute_force, "brute_force", events)
    assert len(results) == 1
    assert results[0]["window_size_seconds"] == WINDOW_SECONDS["brute_force"]


def test_instantaneous_rules_skip_bucketing():
    events = [evt(event_type="privilege", action="privilege_escalation",
                    status="success", hostname="WS-01")]
    results = run_windowed_detector(detect_privilege_escalation, "privilege_escalation", events)
    assert len(results) == 1
    assert "window_start" not in results[0]
