import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from shared.schema import validate_event, ValidationError, SecurityEvent
from shared.normalize import normalize_event, enrich_event


def make_raw(**overrides):
    base = {
        "event_id": SecurityEvent.new_id(),
        "timestamp": SecurityEvent.now_iso(),
        "event_type": "authentication",
        "action": "login_failed",
        "status": "failure",
        "source_ip": "203.0.113.5",
        "destination_ip": "10.20.1.12",
        "destination_port": 3389,
        "username": "administrator",
        "hostname": "WS-042",
        "bytes": 0,
        "country": "DZ",
    }
    base.update(overrides)
    return base


def test_valid_event_passes():
    validate_event(make_raw())  # should not raise


def test_missing_required_field_rejected():
    raw = make_raw()
    del raw["event_type"]
    with pytest.raises(ValidationError):
        validate_event(raw)


def test_unknown_event_type_rejected():
    with pytest.raises(ValidationError):
        validate_event(make_raw(event_type="not_a_real_type"))


def test_action_must_match_event_type():
    with pytest.raises(ValidationError):
        validate_event(make_raw(event_type="network", action="login_failed"))


def test_normalize_repairs_missing_optional_fields():
    raw = make_raw()
    del raw["bytes"]
    n = normalize_event(raw)
    assert n["bytes"] == 0
    assert n["metadata"] == {}


def test_normalize_rejects_unrecoverable_timestamp():
    from shared.schema import ValidationError
    raw = make_raw(timestamp="not-a-timestamp")
    with pytest.raises(ValidationError):
        normalize_event(raw)


def test_enrichment_flags_privileged_account():
    n = normalize_event(make_raw(username="administrator"))
    e = enrich_event(n)
    assert e["is_privileged_account"] is True


def test_enrichment_flags_suspicious_port():
    n = normalize_event(make_raw(destination_port=4444))
    e = enrich_event(n)
    assert e["is_suspicious_port"] is True


def test_enrichment_known_service_port():
    n = normalize_event(make_raw(destination_port=443))
    e = enrich_event(n)
    assert e["known_service"] == "https"
