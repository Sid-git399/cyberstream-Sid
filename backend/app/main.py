from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
import asyncio
import os

from .routers import alerts, events, pipeline, loadlab, incidents, quality, reports, settings
from . import ingest

MAX_BODY_BYTES = 2_000_000  # Section 48: limit request size

app = FastAPI(
    title="CyberStream Security Analytics API",
    version="1.0.0",
    description="Serves processed security analytics — never raw Kafka data.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(","),
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["*"],
)


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        cl = request.headers.get("content-length")
        if cl and int(cl) > MAX_BODY_BYTES:
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "request body too large"}, status_code=413)
        return await call_next(request)


app.add_middleware(BodySizeLimitMiddleware)

app.include_router(alerts.router)
app.include_router(events.router)
app.include_router(pipeline.router)
app.include_router(loadlab.router)
app.include_router(incidents.router)
app.include_router(quality.router)
app.include_router(reports.router)
app.include_router(settings.router)


@app.on_event("startup")
async def start_background_ingestor():
    """
    Polls Spark's JSON alert/incident/DLQ output into Postgres (see
    app/ingest.py). This is what makes /api/alerts, /api/incidents, and
    /api/pipeline/dlq reflect what the streaming job has actually
    produced, without requiring Spark to talk to Postgres directly.
    """
    async def loop():
        while True:
            try:
                ingest.run_ingest_cycle()
            except Exception as e:
                print(f"[ingest] cycle failed: {e}")
            await asyncio.sleep(ingest.INGEST_INTERVAL_SECONDS)

    asyncio.create_task(loop())


@app.get("/healthz")
def healthz():
    return {"status": "ok"}
