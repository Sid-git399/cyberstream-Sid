import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "backend"))

from app.routers.settings import get_settings, get_detection_rules, bigdata_concepts
from app.routers.alerts import _recommended_steps
from app import metrics


def test_get_settings_reads_env_defaults():
    s = get_settings()
    assert s["kafka"]["topic"] == "security-events"
    assert "raw" in s["retention_days"]
    assert isinstance(s["retention_days"]["raw"], int)


def test_get_detection_rules_covers_every_detector():
    rules = get_detection_rules()
    rule_names = {r["rule"] for r in rules["rules"]}
    expected = {"brute_force", "password_spraying", "port_scan", "privilege_escalation",
                 "suspicious_process", "data_transfer_anomaly", "dns_anomaly",
                 "suspicious_authentication"}
    assert expected.issubset(rule_names)


def test_get_detection_rules_thresholds_match_shared_module():
    from shared.detection import THRESHOLDS
    rules = {r["rule"]: r for r in get_detection_rules()["rules"]}
    assert rules["brute_force"]["threshold"] == THRESHOLDS["brute_force_failed_logins"]
    assert rules["port_scan"]["threshold"] == THRESHOLDS["port_scan_distinct_ports"]


def test_bigdata_concepts_has_volume_velocity_variety():
    c = bigdata_concepts()
    assert set(c["volume_velocity_variety"].keys()) == {"volume", "velocity", "variety"}


def test_recommended_steps_has_fallback_for_unknown_rule():
    steps = _recommended_steps("some_future_rule_not_yet_defined")
    assert len(steps) >= 1


def test_recommended_steps_known_rule():
    steps = _recommended_steps("brute_force")
    assert any("account" in s.lower() for s in steps)


# --- Section 67 contract: never fabricate metrics -------------------------
# These run with no Kafka/Spark reachable in this environment, and verify
# the API degrades to an explicit OFFLINE/unavailable status rather than
# inventing a plausible-looking number.

def test_kafka_status_reports_offline_when_unreachable():
    result = metrics.kafka_status()
    assert result["status"] in ("OFFLINE", "DEGRADED")
    assert "reason" in result or "topics" in result


def test_spark_status_reports_offline_when_unreachable():
    result = metrics.spark_status()
    assert result["status"] == "OFFLINE"
    assert "reason" in result


def test_throughput_snapshot_never_fabricates_numbers_when_offline():
    snap = metrics.throughput_snapshot()
    # When Spark/Kafka are unreachable, these must be the literal string
    # "unavailable" — never a guessed float.
    assert snap["processing_eps"] == "unavailable"
    assert snap["latency_ms"] == "unavailable"
