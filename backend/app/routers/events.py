from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from ..db import duckdb_conn, processed_events_glob, has_processed_data

router = APIRouter(prefix="/api/events", tags=["events"])

ALLOWED_FILTER_COLS = {
    "event_type", "hostname", "username", "source_ip",
    "destination_ip", "action", "status",
}


@router.get("")
def list_events(
    event_type: Optional[str] = None,
    hostname: Optional[str] = None,
    username: Optional[str] = None,
    source_ip: Optional[str] = None,
    destination_ip: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    """
    Section 28: filter by common fields with pagination + server-side
    aggregation. Backed by DuckDB reading the Parquet lake directly —
    never loads the full dataset into API memory or the browser.
    """
    if not has_processed_data():
        return {"total": 0, "page": page, "page_size": page_size, "events": [],
                 "note": "no processed events yet — start the generator + streaming job"}

    clauses, params = [], {}
    for col, val in [("event_type", event_type), ("hostname", hostname),
                      ("username", username), ("source_ip", source_ip),
                      ("destination_ip", destination_ip)]:
        if val:
            clauses.append(f"{col} = ${col}")
            params[col] = val
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    offset = (page - 1) * page_size

    con = duckdb_conn()
    glob = processed_events_glob()
    total = con.execute(
        f"SELECT count(*) FROM read_parquet('{glob}') {where}", params
    ).fetchone()[0]
    rows = con.execute(
        f"""SELECT * FROM read_parquet('{glob}') {where}
            ORDER BY timestamp DESC LIMIT {page_size} OFFSET {offset}""",
        params,
    ).fetchdf().to_dict(orient="records")

    return {"total": total, "page": page, "page_size": page_size, "events": rows}


@router.post("/hunt")
def threat_hunt(query: dict):
    """
    Section 29: structured threat-hunting queries, e.g.
        {"source_ip": "10.20.5.12", "min_severity": "HIGH"}
    Deliberately a small structured filter set rather than a free-form
    query language, per the spec's guidance to avoid over-engineering.
    """
    if not has_processed_data():
        return {"total": 0, "events": [],
                 "note": "no processed events yet — start the generator + streaming job"}

    clauses, params = [], {}
    for col in ALLOWED_FILTER_COLS:
        if col in query and query[col]:
            clauses.append(f"{col} = ${col}")
            params[col] = query[col]
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    con = duckdb_conn()
    glob = processed_events_glob()
    rows = con.execute(
        f"SELECT * FROM read_parquet('{glob}') {where} ORDER BY timestamp DESC LIMIT 500",
        params,
    ).fetchdf().to_dict(orient="records")
    return {"total": len(rows), "events": rows}
