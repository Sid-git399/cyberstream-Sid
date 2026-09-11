from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from ..db import pg_conn

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


@router.get("")
def list_alerts(
    severity: Optional[str] = None,
    status: Optional[str] = None,
    hostname: Optional[str] = None,
    source_ip: Optional[str] = None,
    detection_rule: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
):
    """Paginated, server-side filtered alert list (Section 28 principle applied to alerts)."""
    clauses, params = [], []
    for col, val in [("severity", severity), ("status", status),
                      ("hostname", hostname), ("source_ip", source_ip),
                      ("detection_rule", detection_rule)]:
        if val:
            clauses.append(f"{col} = %s")
            params.append(val)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    offset = (page - 1) * page_size

    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS n FROM alerts {where}", params)
        total = cur.fetchone()["n"]
        cur.execute(
            f"SELECT * FROM alerts {where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
            params + [page_size, offset],
        )
        rows = cur.fetchall()
    return {"total": total, "page": page, "page_size": page_size, "alerts": rows}


@router.get("/{alert_id}")
def get_alert(alert_id: str):
    """Full alert investigation view (Section 42)."""
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM alerts WHERE alert_id = %s", (alert_id,))
        alert = cur.fetchone()
        if not alert:
            raise HTTPException(404, "alert not found")

        cur.execute(
            """SELECT * FROM alerts
               WHERE (hostname = %s OR source_ip = %s) AND alert_id != %s
               ORDER BY created_at DESC LIMIT 20""",
            (alert.get("hostname"), alert.get("source_ip"), alert_id),
        )
        related = cur.fetchall()

    return {
        "alert": alert,
        "related_alerts": related,
        "recommended_steps": _recommended_steps(alert["detection_rule"]),
    }


@router.patch("/{alert_id}/status")
def update_status(alert_id: str, status: str):
    valid = {"NEW", "ACKNOWLEDGED", "INVESTIGATING", "RESOLVED", "FALSE_POSITIVE"}
    if status not in valid:
        raise HTTPException(400, f"status must be one of {valid}")
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("UPDATE alerts SET status=%s WHERE alert_id=%s", (status, alert_id))
        conn.commit()
    return {"alert_id": alert_id, "status": status}


def _recommended_steps(rule: str) -> list[str]:
    steps = {
        "brute_force": [
            "Confirm whether the target account authenticated successfully after the failures.",
            "Check whether the source IP is expected for this account/host.",
            "Consider temporary lockout or MFA enforcement for the account.",
        ],
        "password_spraying": [
            "Identify all accounts targeted by this source in the window.",
            "Check for any successful logins among the targeted accounts.",
            "Block or rate-limit the source at the perimeter if external.",
        ],
        "port_scan": [
            "Identify which ports responded vs. were filtered.",
            "Check firewall logs for the same source around this time.",
            "Verify no lateral movement occurred from any responsive service.",
        ],
        "privilege_escalation": [
            "Verify the privilege change was authorized (change ticket, approver).",
            "Review subsequent activity by the affected account.",
        ],
        "data_transfer_anomaly": [
            "Identify the destination and whether it is a known/approved endpoint.",
            "Check for preceding suspicious process or privilege events on the host.",
        ],
    }
    return steps.get(rule, ["Review evidence and correlate with related alerts."])
