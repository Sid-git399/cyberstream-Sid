import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.scoring import score_detection, severity_for, BaselineStore


def test_severity_bands():
    assert severity_for(0) == "INFO"
    assert severity_for(24) == "INFO"
    assert severity_for(25) == "LOW"
    assert severity_for(49) == "LOW"
    assert severity_for(50) == "MEDIUM"
    assert severity_for(75) == "HIGH"
    assert severity_for(90) == "CRITICAL"
    assert severity_for(100) == "CRITICAL"


def test_score_detection_breakdown_sums_correctly():
    detection = {"detection_rule": "brute_force"}
    result = score_detection(detection, is_privileged=True, cross_host=True)
    expected = 25 + 15 + 5
    assert result["risk_score"] == expected
    assert result["severity"] == severity_for(expected)
    assert len(result["score_breakdown"]) == 3


def test_score_detection_caps_at_100():
    detection = {"detection_rule": "brute_force"}
    result = score_detection(detection, is_privileged=True, cross_host=True)
    assert result["risk_score"] <= 100


def test_baseline_no_anomaly_with_insufficient_history():
    store = BaselineStore()
    assert store.evaluate("host-1", 100) is None
    assert store.evaluate("host-1", 105) is None


def test_baseline_flags_real_deviation():
    store = BaselineStore()
    # Build a stable baseline around ~100 events/minute
    for v in [98, 102, 99, 101, 100, 97, 103, 100]:
        store.evaluate("host-1", v)
    # Then a genuine spike
    result = store.evaluate("host-1", 850)
    assert result is not None
    assert result["observed"] == 850
    assert result["deviation_pct"] > 100


def test_baseline_does_not_flag_normal_fluctuation():
    store = BaselineStore()
    for v in [98, 102, 99, 101, 100, 97, 103, 100]:
        store.evaluate("host-1", v)
    result = store.evaluate("host-1", 101)
    assert result is None
