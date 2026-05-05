"""
Unit tests for the HTTP API Client.
"""
import pytest
import api_client

@pytest.fixture(autouse=True)
def mock_auth(monkeypatch):
    """Automatically mock the JWT token retrieval for all API tests."""
    monkeypatch.setattr(api_client, "get_access_token", lambda: "fake_token_123")

def test_submit_job(monkeypatch):
    """Test that the job submission payload is sent correctly and parsed."""
    # Mock the Cluster Manager response
    class MockResponse:
        status_code = 200
        text = "OK"
        def json(self):
            return {"job_id": "test-uuid-1234", "message": "Success"}
            
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: MockResponse())
    
    payload = {"num_mappers": 2, "input_data_path": "minio://test"}
    result = api_client.submit_job(payload)
    
    assert result["job_id"] == "test-uuid-1234"

def test_get_status_failure(monkeypatch):
    """Test that a 404 from the Cluster Manager raises an exception."""
    class MockResponse:
        status_code = 404
        text = "Job not found"
        
    monkeypatch.setattr("requests.get", lambda *args, **kwargs: MockResponse())
    
    with pytest.raises(Exception) as exc_info:
        api_client.get_status("bad-uuid")
        
    assert "Failed to get status" in str(exc_info.value)

def test_abort_job(monkeypatch):
    """Test that the abort request is sent correctly."""
    class MockResponse:
        status_code = 200
        text = "OK"
        def json(self):
            return {"message": "Job aborted successfully"}
            
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: MockResponse())
    result = api_client.abort_job("test-uuid")
    assert result["message"] == "Job aborted successfully"

def test_api_server_error(monkeypatch):
    """Test handling of 500 errors from the server."""
    class MockResponse:
        status_code = 500
        text = "Internal Server Error"
        
    monkeypatch.setattr("requests.post", lambda *args, **kwargs: MockResponse())
    with pytest.raises(Exception) as exc:
        api_client.submit_job({"num_mappers": 1})
    assert "Failed to submit job" in str(exc.value)