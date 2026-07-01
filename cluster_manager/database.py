"""
Database interaction layer for the Cluster Manager.

This module manages the asyncpg connection pool and executes the raw SQL queries
required to track the global state and configuration of Map-Reduce workloads.
"""

import os
import asyncpg
from asyncpg.pool import Pool
from uuid import uuid4, UUID
from schemas import JobSubmissionRequest

DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "admin")
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432") # Changed to DB_PORT
DB_NAME = os.getenv("POSTGRES_DB", "mapreduce")

_pool: Pool | None = None

async def init_db_pool():
    """
    Initializes the asynchronous PostgreSQL connection pool.
    This function should be called during the FastAPI application startup event.
    """
    global _pool
    _pool = await asyncpg.create_pool(
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        host=DB_HOST,
        port=DB_PORT,
    )

async def close_db_pool():
    """
    Gracefully closes all database connections in the pool.
    This function should be called during the FastAPI application shutdown event.
    """
    if _pool:
        await _pool.close()

async def create_job_record(user_id: str, job_req: JobSubmissionRequest) -> UUID:
    """
    Inserts a new job and its configuration parameters into the database.
    
    Executes within an atomic transaction to ensure both the 'jobs' and 'job_config'
    tables are updated simultaneously.
    
    Args:
        user_id (str): The Keycloak ID of the user submitting the job.
        job_req (JobSubmissionRequest): The validated configuration payload.
        
    Returns:
        UUID: The newly generated unique identifier for the job.
    """
    job_id = uuid4()
    intermediate_prefix = f"users/{user_id}/intermediate/{job_id}/"
    
    async with _pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO mapreduce.jobs 
                (job_id, user_id, status, input_data_path, output_data_path, intermediate_prefix, code_location, input_file_size_bytes)
                VALUES ($1, $2, 'pending', $3, $4, $5, $6, $7)
            """, job_id, user_id, job_req.input_data_path, job_req.output_data_path, 
                 intermediate_prefix, job_req.code_location, job_req.input_file_size_bytes)
            
            await conn.execute("""
                INSERT INTO mapreduce.job_config
                (job_id, num_mappers, num_reducers, default_chunk_size_bytes, worker_timeout_seconds, max_task_retries)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, job_id, job_req.num_mappers, job_req.num_reducers, job_req.default_chunk_size_bytes, 
                 job_req.worker_timeout_seconds, job_req.max_task_retries)
            
    return job_id

async def get_job_status(job_id: UUID) -> dict | None:
    """
    Retrieves the current execution status and task completion counts for a specific job.
    
    Args:
        job_id (UUID): The unique identifier of the job to query.
        
    Returns:
        dict | None: A dictionary containing the job's state metrics, or None if the job is not found.
    """
    async with _pool.acquire() as conn:
        record = await conn.fetchrow("""
            SELECT job_id, status, completed_mappers_count, completed_reducers_count, created_at, started_at, completed_at
            FROM mapreduce.jobs WHERE job_id = $1
        """, job_id)
        return dict(record) if record else None

async def get_job_status_for_user(job_id: UUID, user_id: str) -> dict | None:
    """
    Retrieves a job's status only if it belongs to the given authenticated user.

    Args:
        job_id (UUID): The unique identifier of the job to query.
        user_id (str): The authenticated user's subject identifier.

    Returns:
        dict | None: The job status record when owned by the user, otherwise None.
    """
    async with _pool.acquire() as conn:
        record = await conn.fetchrow("""
            SELECT job_id, status, completed_mappers_count, completed_reducers_count, created_at, started_at, completed_at
            FROM mapreduce.jobs
            WHERE job_id = $1 AND user_id = $2
        """, job_id, user_id)
        return dict(record) if record else None

async def update_job_status(job_id: UUID, status: str):
    """
    Updates the global status enum for a specific job.
    
    Args:
        job_id (UUID): The unique identifier of the job.
        status (str): The new status string to apply (must match the mapreduce.job_status enum).
    """
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE mapreduce.jobs SET status = $1::mapreduce.job_status WHERE job_id = $2", status, job_id)

async def get_jobs_for_user(user_id: str) -> list[dict]:
    """
    Retrieves all jobs owned by the given authenticated user.

    Args:
        user_id (str): The authenticated user's subject identifier.

    Returns:
        list[dict]: The jobs that belong to the user.
    """
    async with _pool.acquire() as conn:
        records = await conn.fetch(
            "SELECT * FROM mapreduce.jobs WHERE user_id = $1 ORDER BY created_at DESC",
            user_id,
        )
        return [dict(r) for r in records]

async def get_all_jobs() -> list[dict]:
    """
    Retrieves the complete state records for all jobs in the cluster.

    Returns:
        list[dict]: A list containing the metadata dictionaries for every job.
    """
    async with _pool.acquire() as conn:
        records = await conn.fetch("SELECT * FROM mapreduce.jobs ORDER BY created_at DESC")
        return [dict(r) for r in records]
