from fastapi import APIRouter, Query
from collections import Counter
from ..db import pg_conn
from shared.data_quality import GLOBAL_TRACKER

router = APIRouter(prefix="/api/pipeline", tags=["data-quality"])


@router.get("/data-quality")
def data_quality():
    """
    Section 61: invalid events, missing fields, duplicates, late events,
    processing errors — read from the SAME tracker instance the streaming
    job updates (shared.data_quality.GLOBAL_TRACKER). In this backend
    process the values reflect whatever's happened in *this* process;
    the authoritative live counts come from the Spark job's own tracker,
    exposed the same way if this API is imported by that job's driver.
    Note field documents this explicitly rather than implying the two
    are always the same process.
    """
    snap = GLOBAL_TRACKER.snapshot()
    snap["note"] = ("Counts reflect this backend process's own tracker instance. "
                     "For live-pipeline data quality during a Docker Compose run, "
                     "see the Spark driver logs, which use the same shared/data_quality "
                     "module.")
    return snap


@router.get("/dlq")
def dlq_stats(page: int = Query(1, ge=1), page_size: int = Query(50, ge=1, le=500)):
    """
    Section 47: dead-letter queue statistics — original event, error
    reason, and timestamp for every event that failed normalization,
    ingested from Spark's DLQ JSON output by the background ingestor.
    """
    offset = (page - 1) * page_size
    with pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM dlq_entries")
        total = cur.fetchone()["n"]

        cur.execute("SELECT error_reason, count(*) AS n FROM dlq_entries "
                     "GROUP BY error_reason ORDER BY n DESC LIMIT 10")
        reasons = cur.fetchall()

        cur.execute(
            "SELECT * FROM dlq_entries ORDER BY created_at DESC LIMIT %s OFFSET %s",
            (page_size, offset),
        )
        rows = cur.fetchall()

    return {"total": total, "page": page, "page_size": page_size,
             "reason_breakdown": reasons, "entries": rows}
