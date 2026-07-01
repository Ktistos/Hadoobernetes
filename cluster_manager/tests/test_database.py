import pytest
import asyncio
from uuid import uuid4
import database as db
from schemas import JobSubmissionRequest
from unittest.mock import AsyncMock, MagicMock

@pytest.fixture
def mock_pool(monkeypatch):
    pool = MagicMock()
    conn = MagicMock()
    
    # 1. Mock the async context manager returned by pool.acquire()
    acquire_ctx = AsyncMock()
    acquire_ctx.__aenter__.return_value = conn
    pool.acquire.return_value = acquire_ctx
    
    # 2. Mock the async context manager returned by conn.transaction()
    tx_ctx = AsyncMock()
    tx_ctx.__aenter__.return_value = AsyncMock()
    conn.transaction.return_value = tx_ctx

    # 3. Explicitly define awaited methods on the connection as AsyncMocks
    conn.execute = AsyncMock()
    conn.fetchrow = AsyncMock()
    conn.fetch = AsyncMock()
    
    monkeypatch.setattr(db, "_pool", pool)
    return pool, conn

def test_create_job_record(mock_pool):
    _, conn = mock_pool
    
    req = JobSubmissionRequest(
        num_mappers=2,
        num_reducers=1,
        input_data_path="in",
        output_data_path="out",
        code_location="code",
        input_file_size_bytes=100
    )
    
    # Run the async database function synchronously
    job_id = asyncio.run(db.create_job_record("user-123", req))
    
    assert job_id is not None
    assert conn.execute.call_count == 2
    
    # Check that the first query was for the jobs table
    first_call_args = conn.execute.call_args_list[0][0]
    assert "INSERT INTO mapreduce.jobs" in first_call_args[0]
    assert first_call_args[5] == f"users/user-123/intermediate/{job_id}/"

def test_get_job_status(mock_pool):
    _, conn = mock_pool
    mock_uuid = uuid4()
    
    conn.fetchrow.return_value = {"job_id": mock_uuid, "status": "running"}
    
    result = asyncio.run(db.get_job_status(mock_uuid))
    assert result["status"] == "running"

def test_get_job_status_for_user(mock_pool):
    _, conn = mock_pool
    mock_uuid = uuid4()

    conn.fetchrow.return_value = {"job_id": mock_uuid, "status": "running"}

    result = asyncio.run(db.get_job_status_for_user(mock_uuid, "user-123"))
    assert result["status"] == "running"

    query_args = conn.fetchrow.call_args[0]
    assert "WHERE job_id = $1 AND user_id = $2" in query_args[0]
    assert query_args[1] == mock_uuid
    assert query_args[2] == "user-123"

def test_get_jobs_for_user(mock_pool):
    _, conn = mock_pool

    conn.fetch.return_value = [
        {"job_id": uuid4(), "user_id": "user-123", "status": "running"},
        {"job_id": uuid4(), "user_id": "user-123", "status": "completed"},
    ]

    result = asyncio.run(db.get_jobs_for_user("user-123"))
    assert len(result) == 2
    assert all(job["user_id"] == "user-123" for job in result)

    query_args = conn.fetch.call_args[0]
    assert "WHERE user_id = $1" in query_args[0]
    assert "ORDER BY created_at DESC" in query_args[0]
    assert query_args[1] == "user-123"

def test_get_all_jobs(mock_pool):
    _, conn = mock_pool

    conn.fetch.return_value = [
        {"job_id": uuid4(), "user_id": "user-123", "status": "running"},
        {"job_id": uuid4(), "user_id": "user-456", "status": "completed"},
    ]

    result = asyncio.run(db.get_all_jobs())
    assert len(result) == 2

    query_args = conn.fetch.call_args[0]
    assert query_args[0] == "SELECT * FROM mapreduce.jobs ORDER BY created_at DESC"
