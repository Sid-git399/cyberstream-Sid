"""
Normalization (Section 15) and Enrichment (Section 16).

Both functions are pure: dict -> dict. That purity is what lets the exact
same code run inside a Spark `mapPartitions`/pandas UDF AND inside a plain
Python loop (local dev fallback / unit tests) with identical behaviour.
"""

from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .schema import (
    EVENT_TYPES, KNOWN_SERVICE_PORTS, SUSPICIOUS_PORTS,
    PRIVILEGED_ACCOUNTS, ValidationError, validate_event,
)


def normalize_event(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Repair/standardize an event. Raises ValidationError only when the
    event is unusable even after repair attempts (routed to DLQ upstream).
    """
    evt = dict(raw)

    # --- timestamp repair -------------------------------------------------
    ts = evt.get("timestamp")
    if isinstance(ts, str):
        cleaned = ts[:-1] + "+00:00" if ts.endswith("Z") else ts
        try:
            datetime.fromisoformat(cleaned)
        except Exception:
            evt["timestamp"] = None
    else:
        evt["timestamp"] = None

    if evt["timestamp"] is None:
        raise ValidationError(f"unrecoverable timestamp: {ts!r}")

    # --- defaulting missing optional fields --------------------------------
    evt.setdefault("source_ip", None)
    evt.setdefault("destination_ip", None)
    evt.setdefault("source_port", None)
    evt.setdefault("destination_port", None)
    evt.setdefault("protocol", "TCP")
    evt.setdefault("username", None)
    evt.setdefault("hostname", None)
    evt.setdefault("bytes", 0)
    evt.setdefault("country", "ZZ")
    evt.setdefault("metadata", {})

    if evt.get("bytes") is None or not isinstance(evt.get("bytes"), (int, float)):
        evt["bytes"] = 0

    # final structural check (raises ValidationError -> caller sends to DLQ)
    validate_event(evt)
    return evt


def enrich_event(evt: Dict[str, Any]) -> Dict[str, Any]:
    """Add derived fields used by detection + dashboards (Section 16)."""
    out = dict(evt)

    ts = evt["timestamp"]
    dt = datetime.fromisoformat(ts[:-1] + "+00:00" if ts.endswith("Z") else ts)

    out["hour_of_day"] = dt.hour
    out["day_of_week"] = dt.strftime("%A")
    out["is_off_hours"] = dt.hour < 6 or dt.hour >= 22

    src = evt.get("source_ip") or ""
    dst = evt.get("destination_ip") or ""
    out["source_internal"] = src.startswith(("10.", "172.16.", "192.168."))
    out["destination_internal"] = dst.startswith(("10.", "172.16.", "192.168."))

    user = (evt.get("username") or "").lower()
    out["is_privileged_account"] = user in PRIVILEGED_ACCOUNTS

    dport = evt.get("destination_port")
    out["known_service"] = KNOWN_SERVICE_PORTS.get(dport) if dport else None
    out["is_suspicious_port"] = bool(dport) and dport in SUSPICIOUS_PORTS

    # Synthetic geo/asset-criticality mapping — deliberately local & fake-safe.
    out["asset_criticality"] = _asset_criticality(evt.get("hostname"))

    return out


def _asset_criticality(hostname: Optional[str]) -> str:
    if not hostname:
        return "unknown"
    h = hostname.upper()
    if h.startswith(("DC-", "SQL-", "DOMAIN")):
        return "critical"
    if h.startswith(("SRV-", "APP-")):
        return "high"
    if h.startswith("WS-"):
        return "standard"
    return "standard"
