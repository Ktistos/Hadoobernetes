"""
Job Master — main.py
====================
FastAPI entry point. Exposes the endpoints defined in the design doc:
  POST /worker_ping     — heartbeat / status updates from mapper and reducer pods
  GET  /readyz          — readiness probe (returns 200 only after init completes)
  GET  /healthz         — liveness probe

Additional profiling endpoint (enabled when PROFILE=1):
  GET  /debug/profile   — returns a live JSON snapshot of PhaseTimer timings

Environment variables (all required unless noted):
  JOB_ID                  UUID of the job this master owns
  DATABASE_URL            asyncpg DSN  e.g. postgresql://user:pass@host:5432/db
  CLUSTER_MANAGER_URL     e.g. http://cluster-manager-service:8000
  JOB_MASTER_SERVICE_URL  URL workers use to reach *this* pod
  WORKER_IMAGE            Docker image for mapper/reducer pods
  MINIO_ENDPOINT          e.g. minio-service:9000
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  MINIO_BUCKET
  K8S_NAMESPACE           (optional, default "default")
  PING_INTERVAL           (optional, default 10 seconds, passed to workers)
  PROFILE                 (optional, "1" to enable timing output, default "0")
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from state_machine import JobStateMachine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

PROFILE_ENABLED = os.environ.get("PROFILE", "0") == "1"

state_machine: JobStateMachine | None = None
_ready = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global state_machine, _ready

    job_id = os.environ["JOB_ID"]
    logger.info(f"Job Master starting for job_id={job_id}")

    state_machine = JobStateMachine(job_id)
    await state_machine.initialize()
    asyncio.create_task(state_machine.run())

    _ready = True
    logger.info(f"Job Master ready for job_id={job_id}")

    yield

    if state_machine and state_machine.db:
        await state_machine.db.close()
        logger.info("DB connection closed.")


app = FastAPI(title="Job Master", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class WorkerPingRequest(BaseModel):
    worker_id:   str   # e.g. "mapper_3" or "reducer_1"
    worker_type: str   # "mapper" | "reducer"
    status:      str   # "started" | "alive" | "completed"  (design doc §3.2)


class WorkerPingResponse(BaseModel):
    ok: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/worker_ping", response_model=WorkerPingResponse)
async def worker_ping(req: WorkerPingRequest):
    """
    Accepts heartbeat / phase-change pings from mapper and reducer pods.
    Design doc §3.2 — Worker Ping.
    """
    if state_machine is None:
        raise HTTPException(status_code=503, detail="State machine not initialised")
    if req.worker_type not in ("mapper", "reducer"):
        raise HTTPException(status_code=400, detail=f"Unknown worker_type: {req.worker_type}")
    if req.status not in ("started", "alive", "completed"):
        raise HTTPException(status_code=400, detail=f"Unknown status: {req.status}")

    await state_machine.handle_ping(req.worker_id, req.worker_type, req.status)
    return WorkerPingResponse(ok=True)


@app.get("/readyz")
async def readyz():
    """Readiness probe — design doc §3.2."""
    if not _ready:
        raise HTTPException(status_code=503, detail="Not ready yet")
    return {"status": "ready"}


@app.get("/healthz")
async def healthz():
    """Liveness probe — design doc §3.2."""
    return {"status": "alive"}


@app.get("/debug/profile")
async def debug_profile():
    """
    Returns a live JSON snapshot of the PhaseTimer accumulated timings.

    Available at any point during a job run.  Most useful to call:
      - After job submission, to see initialization overhead
      - While mappers are running, to see k8s spawn latency
      - After job completion, for the full picture

    Example response:
      {
        "job_id": "aaaa-bbbb-...",
        "profile_enabled": true,
        "timings": {
          "db_connect":         0.031,
          "db_fetch_job":       0.008,
          "k8s_spawn_mapper":   0.412,
          "handle_ping":        0.003,
          ...
        },
        "total_seconds": 0.987
      }

    This endpoint is always available regardless of PROFILE env var — the
    timer accumulates data either way.  PROFILE=1 additionally prints
    reports to pod logs at phase transitions.
    """
    if state_machine is None:
        raise HTTPException(status_code=503, detail="State machine not initialised")

    timings = state_machine.timer.snapshot()
    total   = sum(timings.values())

    return JSONResponse({
        "job_id":           state_machine.job_id,
        "profile_enabled":  PROFILE_ENABLED,
        "timings":          {k: round(v, 6) for k, v in timings.items()},
        "total_seconds":    round(total, 6),
    })