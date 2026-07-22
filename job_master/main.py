"""
Job Master — main.py
====================
FastAPI entry point. Exposes the endpoints defined in the design doc:
  POST /worker_ping     — heartbeat / status updates from mapper and reducer pods
  GET  /readyz          — readiness probe (returns 200 only after init completes)
  GET  /healthz         — liveness probe

Environment variables (all required unless noted):
  JOB_ID                  UUID of the job this master owns
  POSTGRES_HOST           PostgreSQL service host, e.g. postgres
  POSTGRES_PORT           (optional, default 5432)
  POSTGRES_USER
  POSTGRES_PASSWORD
  POSTGRES_DB
  CLUSTER_MANAGER_URL     e.g. http://cluster-manager-service:8000
  JOB_MASTER_SERVICE_URL  URL workers use to reach *this* pod
  MINIO_ENDPOINT          e.g. minio-service:9000
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  MINIO_BUCKET
  K8S_NAMESPACE           (optional, default "default")
  PING_INTERVAL           (optional, default 10 seconds, passed to workers)
"""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Response
from pydantic import BaseModel
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from state_machine import JobStateMachine
from logging_utils import profile_time, configure_logging
import metrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

state_machine: JobStateMachine | None = None
_ready = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global state_machine, _ready
    job_id = os.environ["JOB_ID"]
    configure_logging(f"Job Master {job_id[:8]}")
    
    if "JOB_MASTER_SERVICE_URL" not in os.environ:
        pod_ip = os.environ.get("POD_IP", "127.0.0.1")
        os.environ["JOB_MASTER_SERVICE_URL"] = f"http://{pod_ip}:8000"
        
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


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    start_time = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start_time
    
    route_name = request.scope.get("route").path if request.scope.get("route") else request.url.path
    method = request.method
    status = str(response.status_code)
    
    metrics.HTTP_REQUEST_LATENCY.labels(method=method, endpoint=route_name).observe(duration)
    metrics.HTTP_REQUESTS_TOTAL.labels(method=method, endpoint=route_name, status=status).inc()
    
    return response


@app.get("/metrics")
async def metrics_endpoint():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


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
@profile_time
async def worker_ping(req: WorkerPingRequest):
    """
    Accepts heartbeat / phase-change pings from mapper and reducer pods.
    Design doc §3.2 — Worker Ping.
    """
    if state_machine is None:
        raise HTTPException(status_code=503, detail="State machine not initialised")
    if req.worker_type not in ("mapper", "reducer"):
        raise HTTPException(status_code=400, detail=f"Unknown worker_type: {req.worker_type}")
    if req.status not in ("started", "alive", "completed", "failed"): #temporarily add "failed" status for better error handling
        raise HTTPException(status_code=400, detail=f"Unknown status: {req.status}")

    # Validate worker_id format (must end with _<integer>)
    parts = req.worker_id.split("_")
    if len(parts) < 2 or not parts[-1].isdigit():
        raise HTTPException(status_code=400, detail=f"Invalid worker_id format: {req.worker_id}")

    try:
        await state_machine.handle_ping(req.worker_id, req.worker_type, req.status)
        metrics.WORKER_PINGS_TOTAL.labels(worker_id=req.worker_id, worker_type=req.worker_type, status=req.status).inc()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
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
