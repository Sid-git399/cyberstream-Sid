import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.correlation import dedupe_alerts, correlate_incidents
from shared.scoring import score_detection
from generator.generator import generate, HostPool
from shared.schema import validate_event


def _alert(rule, source_ip="1.2.3.4", hostname="WS-01"):
    d = {"detection_rule": rule, "title": rule, "source_ip": source_ip,
         "hostname": hostname, "username": None,
         "evidence": {"event_count": 12}}
    scored = score_detection(d)
    return {**d, **scored}


def test_dedup_collapses_identical_alerts():
    alerts = [_alert("brute_force") for _ in range(50)]
    deduped = dedupe_alerts(alerts)
    assert len(deduped) == 1
    assert deduped[0]["occurrences"] == 50
    assert deduped[0]["total_underlying_events"] == 50 * 12


def test_dedup_keeps_distinct_sources_separate():
    alerts = [_alert("brute_force", source_ip="1.1.1.1"),
              _alert("brute_force", source_ip="2.2.2.2")]
    deduped = dedupe_alerts(alerts)
    assert len(deduped) == 2


def test_correlate_builds_incident_from_kill_chain():
    alerts = [
        _alert("brute_force", hostname="WS-99"),
        _alert("privilege_escalation", hostname="WS-99"),
        _alert("suspicious_process", hostname="WS-99"),
    ]
    incidents = correlate_incidents(alerts)
    assert len(incidents) == 1
    assert incidents[0]["stage_count"] == 3
    rules_in_order = [s["detection_rule"] for s in incidents[0]["stages"]]
    assert rules_in_order == ["brute_force", "privilege_escalation", "suspicious_process"]


def test_correlate_ignores_single_isolated_alert():
    alerts = [_alert("brute_force", hostname="WS-01")]
    assert correlate_incidents(alerts) == []


def test_generator_produces_schema_valid_events():
    events = list(generate(events=200, rate=0, n_hosts=20, n_users=50,
                             scenario="normal", attack_pct=0.0))
    assert len(events) == 200
    for e in events[:50]:
        validate_event(e)  # every generated event must be structurally valid


def test_generator_brute_force_scenario_produces_burst():
    events = list(generate(events=500, rate=0, n_hosts=20, n_users=50,
                             scenario="brute_force", attack_pct=1.0))
    failed_logins = [e for e in events if e["action"] == "login_failed"
                       and e.get("is_synthetic_attack")]
    assert len(failed_logins) >= 10


def test_generator_never_marks_real_attack_execution():
    events = list(generate(events=300, rate=0, n_hosts=20, n_users=50,
                             scenario="multi_stage", attack_pct=1.0))
    # every event is a data record; none carry any execution/action-on-real-system fields
    for e in events:
        assert "exploit" not in e
        assert "payload" not in e
        assert "command" not in e


def test_multi_stage_bruteforce_burst_survives_every_window_alignment():
    """
    Regression test for a real bug: the multi_stage scenario's brute-force
    sub-burst (12 failed logins, 5s apart, 55s span) is close enough to
    the 60s brute-force window that it used to silently split across a
    window boundary depending on wall-clock luck, and brute_force would
    never fire. generator._snap_to_window_start fixes this by anchoring
    the burst to the start of its window — verify it holds at every
    possible second-of-minute start time, not just the one this process
    happens to start at.
    """
    from generator.generator import HostPool, _scenario_events
    from shared.detection import run_detectors
    from datetime import datetime, timezone

    pool = HostPool(50, 100, seed=7)
    for second in range(0, 60, 5):
        start = datetime(2026, 1, 1, 12, 0, second, tzinfo=timezone.utc)
        events = _scenario_events("multi_stage", pool, start)
        failed_logins = [e for e in events if e["action"] == "login_failed"]
        rules_fired = {d["detection_rule"] for d in run_detectors(failed_logins)}
        assert "brute_force" in rules_fired, (
            f"brute_force failed to fire when the burst started at second={second}"
        )
