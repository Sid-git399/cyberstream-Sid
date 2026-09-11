"""
CyberStream — Normalized Security Event Schema
================================================
Single source of truth for the event contract shared by:
  - generator/          (produces raw events onto Kafka)
  - streaming/           (Spark Structured Streaming consumes + normalizes)
  - backend/             (serves normalized/alert data to the frontend)

Why a shared schema module?
Every event type (auth, network, endpoint, privilege, cloud, web) must
collapse into ONE normalized shape before it hits detection logic.
Keeping the schema in one importable module means the generator can
validate what it emits, and the streaming layer can validate what it
consumes, without the two ever drifting apart.
"""

from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional, Dict, Any
import uuid

# ---------------------------------------------------------------------------
# Event type taxonomy (Section 8 of the spec)
# ---------------------------------------------------------------------------

EVENT_TYPES = {
    "authentication": [
        "login_success", "login_failed", "logout",
        "password_change", "account_locked",
    ],
    "network": [
        "connection", "dns_query", "firewall_allow",
        "firewall_deny", "port_scan",
    ],
    "endpoint": [
        "process_created", "process_terminated",
        "file_created", "file_modified",
    ],
    "privilege": [
        "privilege_escalation", "group_modified", "account_created",
    ],
    "cloud": [
        "cloud_login", "api_call", "permission_change",
    ],
    "web": [
        "http_request", "http_error", "suspicious_request",
    ],
}

ALL_ACTIONS = {a for actions in EVENT_TYPES.values() for a in actions}

REQUIRED_FIELDS = [
    "event_id", "timestamp", "event_type", "action", "status",
]

# Known/well-defined ports used by enrichment (Section 16)
KNOWN_SERVICE_PORTS = {
    22: "ssh", 23: "telnet", 25: "smtp", 53: "dns", 80: "http",
    110: "pop3", 143: "imap", 443: "https", 445: "smb", 3306: "mysql",
    3389: "rdp", 5432: "postgres", 5985: "winrm", 6379: "redis",
    8080: "http-alt", 9200: "elasticsearch",
}
SUSPICIOUS_PORTS = {4444, 1337, 31337, 6666, 12345, 54321}

PRIVILEGED_ACCOUNTS = {"administrator", "root", "svc-backup", "domainadmin", "sa"}


@dataclass
class SecurityEvent:
    """Normalized security event (Section 7)."""

    event_id: str
    timestamp: str
    event_type: str
    action: str
    status: str
    source_ip: Optional[str] = None
    destination_ip: Optional[str] = None
    source_port: Optional[int] = None
    destination_port: Optional[int] = None
    protocol: Optional[str] = None
    username: Optional[str] = None
    hostname: Optional[str] = None
    bytes: int = 0
    country: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    # --- provenance (added by the generator, never trusted downstream) ---
    is_synthetic_attack: bool = False
    scenario: Optional[str] = None

    @staticmethod
    def new_id() -> str:
        return f"evt-{uuid.uuid4().hex[:12]}"

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def to_json(self) -> Dict[str, Any]:
        return asdict(self)


class ValidationError(Exception):
    pass


def validate_event(raw: Dict[str, Any]) -> None:
    """
    Raise ValidationError if the raw event is structurally unusable.
    This is deliberately strict on the fields the pipeline cannot function
    without, and permissive everywhere else — malformed *optional* fields
    are repaired during normalization, not rejected outright (Section 15).
    """
    missing = [f for f in REQUIRED_FIELDS if f not in raw or raw[f] in (None, "")]
    if missing:
        raise ValidationError(f"missing required fields: {missing}")

    if raw["event_type"] not in EVENT_TYPES:
        raise ValidationError(f"unknown event_type: {raw['event_type']!r}")

    if raw["action"] not in EVENT_TYPES[raw["event_type"]]:
        raise ValidationError(
            f"action {raw['action']!r} not valid for event_type {raw['event_type']!r}"
        )

    try:
        ts = raw["timestamp"]
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        datetime.fromisoformat(ts)
    except Exception as e:
        raise ValidationError(f"malformed timestamp: {raw.get('timestamp')!r} ({e})")
