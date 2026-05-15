"""
Main application entry point for the Cluster Manager FastAPI service.

This script wires together the data schemas, database interactions, Kubernetes operations,
and security protocols into a set of exposed HTTP endpoints.
"""

from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, Depends, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from schemas import JobSubmissionRequest, JobSubmissionResponse, JobStatusResponse, UpdateJobStateRequest
from security import get_current_user, require_admin
import database as db
import k8s_client as k8s

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the startup and shutdown lifecycle events of the FastAPI application.
    Initializes the database connection pool and Kubernetes configuration on startup,
    and cleanly closes connections on shutdown.
    """
    await db.init_db_pool()
    k8s.init_k8s()
    yield
    await db.close_db_pool()

app = FastAPI(title="Hadoobernetes Cluster Manager", lifespan=lifespan)


async def _check_database_ready() -> tuple[bool, str]:
    if db._pool is None:
        return False, "database pool not initialized"

    try:
        async with db._pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return True, "ok"
    except Exception as exc:
        return False, f"database check failed: {exc}"


async def _check_kubernetes_ready() -> tuple[bool, str]:
    try:
        await run_in_threadpool(lambda: k8s.client.VersionApi().get_code())
        return True, "ok"
    except Exception as exc:
        return False, f"kubernetes API check failed: {exc}"


@app.get("/readyz")
async def readiness_check():
    """
    Probes whether the service is ready to accept HTTP traffic.
    Used by Kubernetes readiness probes.
    """
    checks = {}

    db_ok, db_message = await _check_database_ready()
    checks["database"] = db_message

    k8s_ok, k8s_message = await _check_kubernetes_ready()
    checks["kubernetes"] = k8s_message

    if db_ok and k8s_ok:
        return {"status": "ready", "checks": checks}

    return JSONResponse(
        status_code=503,
        content={"status": "not ready", "checks": checks},
    )

@app.get("/healthz")
async def liveliness_check():
    """
    Probes whether the service application process is alive and functioning.
    Used by Kubernetes liveness probes.
    """
    return {"status": "alive"}

@app.post("/submit_job", response_model=JobSubmissionResponse)
async def submit_job(req: JobSubmissionRequest, user_id: str = Depends(get_current_user)):
    """
    Accepts a new Map-Reduce job payload from an authenticated user, registers the configuration
    in the database, and instructs Kubernetes to spawn a dedicated Job Master pod.
    """
    try:
        job_id = await db.create_job_record(user_id, req)
        k8s.spawn_job_master(job_id)
        return {"job_id": job_id, "message": "Job successfully submitted and master spawned"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit job: {str(e)}")

@app.get("/job_status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: UUID, user_id: str = Depends(get_current_user)):
    """
    Retrieves the real-time execution status and completion metrics for a specific job.
    Requires the user to pass a valid authentication token.
    """
    status_record = await db.get_job_status_for_user(job_id, user_id)
    if not status_record:
        raise HTTPException(status_code=404, detail="Job not found")
    return status_record

@app.post("/abort_job/{job_id}")
async def abort_job(job_id: UUID, user_id: str = Depends(get_current_user)):
    """
    Manually terminates an active job. 
    Updates the database status to aborted, deletes all associated Kubernetes pods,
    and initiates the cleanup of intermediate storage files.
    """
    status_record = await db.get_job_status_for_user(job_id, user_id)
    if not status_record:
        raise HTTPException(status_code=404, detail="Job not found")
        
    await db.update_job_status(job_id, "aborted")
    k8s.terminate_job_pods(job_id)
    
    return {"message": f"Job {job_id} aborted successfully"}

@app.get("/get_all_jobs")
async def get_all_jobs(user_id: str = Depends(get_current_user)):
    """
    Fetch all jobs that belong to the authenticated user.
    """
    jobs = await db.get_jobs_for_user(user_id)
    return {"jobs": jobs}

@app.get("/admin/jobs")
async def get_all_jobs_admin(admin_user_id: str = Depends(require_admin)):
    """
    Fetch the state of all jobs across the entire cluster for admin callers.
    """
    jobs = await db.get_all_jobs()
    return {"jobs": jobs}

@app.post("/update_job_state/{job_id}")
async def update_job_state(job_id: UUID, req: UpdateJobStateRequest):
    """
    Internal endpoint used exclusively by the Job Master pod to notify the Cluster Manager
    of major phase transitions (e.g., pending -> mapping -> reducing -> completed).
    """
    await db.update_job_status(job_id, req.status)
    return {"message": "State updated"}
