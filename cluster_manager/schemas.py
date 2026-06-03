"""
Pydantic schemas for the Cluster Manager API.
This module defines the data validation models used for incoming requests
and outgoing responses in the FastAPI application.
"""
from pydantic import BaseModel, Field
from typing import Optional
from uuid import UUID
from datetime import datetime
class JobSubmissionRequest(BaseModel):
    """
    Schema for a new Map-Reduce job submission request.
    Attributes:
        num_mappers (int): The exact number of map tasks to spawn. Must be greater than 0.
        num_reducers (int): The exact number of reduce tasks to spawn. Must be greater than 0.
        input_data_path (str): The MinIO path where the raw input data is stored.
        code_location (str): The MinIO path where the executable Python script is stored.
        input_file_size_bytes (int): Total byte size of the input data. Used for chunking.
        default_chunk_size_bytes (int): Target byte size for each map chunk. Defaults to 64MB.
        worker_timeout_seconds (int): Time in seconds before a worker is considered dead.
        max_task_retries (int): Maximum number of times a failed worker pod will be recreated.

    Note:
        output_data_path is no longer accepted from the client; the Cluster
        Manager derives a canonical per-job output prefix so results always
        land in a predictable, collision-free location.
    """
    num_mappers: int = Field(..., gt=0)
    num_reducers: int = Field(..., gt=0)
    input_data_path: str
    code_location: str
    input_file_size_bytes: int = Field(..., gt=0)
    default_chunk_size_bytes: int = 67108864
    worker_timeout_seconds: int = 300
    max_task_retries: int = 3
class JobSubmissionResponse(BaseModel):
    """
    Schema for a successful job submission response.
    Attributes:
        job_id (UUID): The unique identifier generated for the new job.
        message (str): A human-readable success message.
    """
    job_id: UUID
    message: str
class JobStatusResponse(BaseModel):
    """
    Schema representing the current execution state of a job.
    Attributes:
        job_id (UUID): The unique identifier of the job.
        status (str): Current global state (e.g., pending, mapping, reducing, completed).
        completed_mappers_count (int): Number of successfully finished map tasks.
        completed_reducers_count (int): Number of successfully finished reduce tasks.
        created_at (datetime): Timestamp of job submission.
        started_at (Optional[datetime]): Timestamp when the Job Master transitioned out of pending.
        completed_at (Optional[datetime]): Timestamp when the job finished or failed.
    """
    job_id: UUID
    status: str
    completed_mappers_count: int
    completed_reducers_count: int
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
class UpdateJobStateRequest(BaseModel):
    """
    Schema for internal state update requests sent by the Job Master.
    Attributes:
        status (str): The new status to apply to the job in the database.
    """
    status: str