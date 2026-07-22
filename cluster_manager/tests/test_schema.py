import pytest
from pydantic import ValidationError
from schemas import JobSubmissionRequest

def test_valid_job_submission():
    """Test that a valid payload parses successfully with correct defaults."""
    payload = {
        "num_mappers": 5,
        "num_reducers": 2,
        "input_data_path": "minio://bucket/input.txt",
        "output_data_path": "minio://bucket/output/",
        "code_location": "minio://bucket/script.py",
        "input_file_size_bytes": 1048576  # 1 MB
    }
    
    req = JobSubmissionRequest(**payload)
    
    assert req.num_mappers == 5
    assert req.num_reducers == 2
    # Check that defaults were applied correctly
    assert req.default_chunk_size_bytes == 67108864
    assert req.worker_timeout_seconds == 300
    assert req.max_task_retries == 3

def test_invalid_zero_mappers():
    """Test that requesting 0 mappers raises a validation error."""
    payload = {
        "num_mappers": 0,  # Invalid: must be > 0
        "num_reducers": 2,
        "input_data_path": "minio://bucket/input.txt",
        "output_data_path": "minio://bucket/output/",
        "code_location": "minio://bucket/script.py",
        "input_file_size_bytes": 1048576
    }
    
    with pytest.raises(ValidationError) as exc_info:
        JobSubmissionRequest(**payload)
        
    assert "Input should be greater than 0" in str(exc_info.value)

def test_missing_required_fields():
    """Test that omitting required fields raises a validation error."""
    payload = {
        "num_mappers": 5,
        # Missing num_reducers
        "input_data_path": "minio://bucket/input.txt",
        # Missing output_data_path
        "code_location": "minio://bucket/script.py",
        "input_file_size_bytes": 1048576
    }
    
    with pytest.raises(ValidationError) as exc_info:
        JobSubmissionRequest(**payload)
        
    errors = exc_info.value.errors()
    missing_fields = [err["loc"][0] for err in errors]
    assert "num_reducers" in missing_fields
    assert "output_data_path" in missing_fields

def test_invalid_negative_file_size():
    """Test that a negative file size is rejected."""
    payload = {
        "num_mappers": 5,
        "num_reducers": 2,
        "input_data_path": "minio://bucket/input.txt",
        "output_data_path": "minio://bucket/output/",
        "code_location": "minio://bucket/script.py",
        "input_file_size_bytes": -500  # Invalid
    }
    
    with pytest.raises(ValidationError) as exc_info:
        JobSubmissionRequest(**payload)
        
    assert "Input should be greater than 0" in str(exc_info.value)

def test_invalid_operational_fields():
    """Test that invalid values for default_chunk_size_bytes, worker_timeout_seconds, and max_task_retries are rejected."""
    base_payload = {
        "num_mappers": 5,
        "num_reducers": 2,
        "input_data_path": "minio://bucket/input.txt",
        "output_data_path": "minio://bucket/output/",
        "code_location": "minio://bucket/script.py",
        "input_file_size_bytes": 1048576
    }

    # Zero chunk size
    payload = base_payload.copy()
    payload["default_chunk_size_bytes"] = 0
    with pytest.raises(ValidationError) as exc_info:
        JobSubmissionRequest(**payload)
    assert "Input should be greater than 0" in str(exc_info.value)

    # Negative timeout
    payload = base_payload.copy()
    payload["worker_timeout_seconds"] = -10
    with pytest.raises(ValidationError) as exc_info:
        JobSubmissionRequest(**payload)
    assert "Input should be greater than 0" in str(exc_info.value)

    # Negative max_task_retries
    payload = base_payload.copy()
    payload["max_task_retries"] = -1
    with pytest.raises(ValidationError) as exc_info:
        JobSubmissionRequest(**payload)
    assert "Input should be greater than or equal to 0" in str(exc_info.value)