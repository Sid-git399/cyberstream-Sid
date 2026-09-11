# Detection Engine

All detectors live in `shared/detection.py` and share one signature:
`fn(events: list[dict]) -> dict | None`, where `events` is one
already-grouped (by host), already-windowed batch. See
`shared.detection.THRESHOLDS` for every numeric threshold used —
nothing is buried inline.

| Rule | Trigger | Window (conceptual) |
|---|---|---|
| `brute_force` | >= 8 failed logins, same source + same account | 1 min |
| `password_spraying` | >= 6 distinct accounts targeted by one source | 5 min |
| `port_scan` | >= 15 distinct destination ports from one source | 5 min |
| `privilege_escalation` | any `privilege_escalation`/`group_modified` event | — |
| `suspicious_process` | a `process_created` event flagged suspicious by the generator's synthetic metadata | — |
| `data_transfer_anomaly` | >= 50MB outbound to a non-internal destination from one host | 1 hr |
| `dns_anomaly` | >= 200 DNS queries from one source | 5 min |

These thresholds are deliberately explicit and adjustable — they are the
"detection rules" a real SOC would tune against its own environment's
baseline (see anomaly-detection.md for the statistical counterpart).

## Why this shape (grouped batches, not single-event rules)?
Every one of these is a *rate* phenomenon. A single failed login event
carries no information about whether it's part of a brute-force attempt;
only the count of failed logins for the same (source, account) pair
within a window does. Writing detectors as `list[event] -> alert|None`
keeps that fact explicit in the code, instead of hiding stateful
counting inside per-event mutable state.
