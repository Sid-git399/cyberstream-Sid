import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datetime import datetime, timedelta, timezone
from shared.detection import (
    detect_brute_force, detect_password_spraying, detect_port_scan,
    detect_data_transfer_anomaly, run_detectors,
)


def ts(offset_seconds=0):
    # Anchored to a fixed, safely mid-window timestamp so tests aren't
    # flaky around real wall-clock minute/5-minute boundaries (the
    # detectors are now bucketed by window — see shared/windowing.py).
    anchor = datetime(2026, 1, 1, 12, 0, 10, tzinfo=timezone.utc)
    return (anchor + timedelta(seconds=offset_seconds)).isoformat().replace("+00:00", "Z")


def evt(**kw):
    base = {"event_id": "e", "timestamp": ts(), "event_type": "authentication",
            "action": "login_failed", "status": "failure", "source_ip": "1.2.3.4",
            "destination_ip": "10.0.0.1", "username": "bob", "hostname": "WS-01",
            "destination_port": None, "bytes": 0, "destination_internal": True}
    base.update(kw)
    return base


def test_brute_force_triggers_above_threshold():
    events = [evt(timestamp=ts(i)) for i in range(10)]
    result = detect_brute_force(events)
    assert result is not None
    assert result["detection_rule"] == "brute_force"
    assert result["evidence"]["event_count"] == 10


def test_brute_force_does_not_trigger_below_threshold():
    events = [evt(timestamp=ts(i)) for i in range(3)]
    assert detect_brute_force(events) is None


def test_password_spraying_needs_distinct_accounts():
    events = [evt(username=f"user{i}") for i in range(8)]
    result = detect_password_spraying(events)
    assert result is not None
    assert result["evidence"]["distinct_username"] >= 6


def test_password_spraying_ignores_single_account_bruteforce():
    events = [evt(username="bob") for _ in range(20)]
    # all same account -> this is brute force territory, not spraying
    assert detect_password_spraying(events) is None


def test_port_scan_triggers_on_many_ports():
    events = [evt(event_type="network", action="connection", destination_port=p)
              for p in range(20)]
    result = detect_port_scan(events)
    assert result is not None
    assert result["detection_rule"] == "port_scan"


def test_data_transfer_anomaly():
    events = [evt(event_type="network", action="connection", destination_internal=False,
                    bytes=60_000_000, hostname="WS-77")]
    result = detect_data_transfer_anomaly(events)
    assert result is not None
    assert result["evidence"]["total_bytes"] >= 50_000_000


def test_data_transfer_anomaly_ignores_internal_traffic():
    events = [evt(event_type="network", action="connection", destination_internal=True,
                    bytes=90_000_000, hostname="WS-77")]
    assert detect_data_transfer_anomaly(events) is None


def test_run_detectors_returns_multiple_when_applicable():
    events = [evt(timestamp=ts(i)) for i in range(10)]
    results = run_detectors(events)
    rules = {r["detection_rule"] for r in results}
    assert "brute_force" in rules


def test_suspicious_authentication_flags_offhours_privileged_login():
    from shared.detection import detect_suspicious_authentication
    e = evt(event_type="authentication", action="login_success", status="success",
             username="administrator", is_off_hours=True, is_privileged_account=True,
             source_internal=True)
    result = detect_suspicious_authentication([e])
    assert result is not None
    assert result["detection_rule"] == "suspicious_authentication"


def test_suspicious_authentication_flags_external_source_login():
    from shared.detection import detect_suspicious_authentication
    e = evt(event_type="authentication", action="login_success", status="success",
             username="bob", is_off_hours=False, is_privileged_account=False,
             source_internal=False)
    result = detect_suspicious_authentication([e])
    assert result is not None


def test_suspicious_authentication_ignores_normal_internal_login():
    from shared.detection import detect_suspicious_authentication
    e = evt(event_type="authentication", action="login_success", status="success",
             username="bob", is_off_hours=False, is_privileged_account=False,
             source_internal=True)
    assert detect_suspicious_authentication([e]) is None
