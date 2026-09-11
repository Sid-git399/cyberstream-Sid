from fastapi import APIRouter, HTTPException, Query
from ..db import pg_conn

router = APIRouter(prefix="/api/incidents", tags=["incidents"])


@router.get("")
def list_incidents(page: int = Query(1, ge=1), page_size: int = Query(25, ge=1, le=200)):
    """
    Correlated multi-stage incidents (Section 43) — populated by the
    background ingestor from Spark's incident correlation output
    (shared/correlation.py::correlate_incidents, called per micro-batch
    in streaming/spark_job.py).
    """
    offset = (page - 1) * page_size
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM incidents")
        total = cur.fetchone()["n"]
        cur.execute(
            "SELECT * FROM incidents ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (page_size, offset),
        )
        rows = cur.fetchall()
    return {"total": total, "page": page, "page_size": page_size, "incidents": rows}


@router.get("/{incident_id}")
def get_incident(incident_id: str):
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT * FROM incidents WHERE incident_id = %s", (incident_id,))
        incident = cur.fetchone()
        if not incident:
            raise HTTPException(404, "incident not found")

        stage_alert_ids = [s.get("alert_id") for s in (incident.get("stages") or [])
                           if s.get("alert_id")]
        alerts = []
        if stage_alert_ids:
            cur.execute("SELECT * FROM alerts WHERE alert_id = ANY(%s)", (stage_alert_ids,))
            alerts = cur.fetchall()

    return {"incident": incident, "alerts": alerts}
