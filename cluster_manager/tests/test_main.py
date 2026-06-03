import pytest
from fastapi.testclient import TestClient
from uuid import uuid4
import main
from main import app
from security import get_current_user, require_admin
import database as db
import k8s_client as k8s

# Override the security dependency for route testing
app.dependency_overrides[get_current_user] = lambda: "mock-user-id"
app.dependency_overrides[require_admin] = lambda: "mock-admin-id"
client = TestClient(app)

def test_readiness_liveliness(monkeypatch):
    async def mock_db_ready():
        return True, "ok"

    async def mock_k8s_ready():
        return True, "ok"

    monkeypatch.setattr(main, "_check_database_ready", mock_db_ready)
    monkeypatch.setattr(main, "_check_kubernetes_ready", mock_k8s_ready)

    readiness = client.get("/readyz")
    liveliness = client.get("/healthz")

    assert readiness.status_code == 200
    assert readiness.json() == {
        "status": "ready",
        "checks": {"database": "ok", "kubernetes": "ok"},
    }
    assert liveliness.status_code == 200
    assert liveliness.json() == {"status": "alive"}


def test_readiness_fails_when_dependency_unavailable(monkeypatch):
    async def mock_db_ready():
        return False, "database check failed: timeout"

    async def mock_k8s_ready():
        return True, "ok"

    monkeypatch.setattr(main, "_check_database_ready", mock_db_ready)
    monkeypatch.setattr(main, "_check_kubernetes_ready", mock_k8s_ready)

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not ready",
        "checks": {
            "database": "database check failed: timeout",
            "kubernetes": "ok",
        },
    }

def test_submit_job(monkeypatch):
    """Test successful job submission triggers DB and K8s."""
    mock_uuid = uuid4()
    
    # Standard Python async function for the mock
    async def mock_create(*args, **kwargs):
        return mock_uuid
        
    monkeypatch.setattr(db, "create_job_record", mock_create)
    monkeypatch.setattr(k8s, "spawn_job_master", lambda jid: None)
    
    payload = {
        "num_mappers": 2,
        "num_reducers": 1,
        "input_data_path": "minio://in",
        "code_location": "minio://code",
        "input_file_size_bytes": 100
    }
    
    response = client.post("/submit_job", json=payload)
    assert response.status_code == 200
    assert response.json()["job_id"] == str(mock_uuid)

def test_get_job_status_found(monkeypatch):
    mock_uuid = uuid4()
    mock_record = {
        "job_id": mock_uuid,
        "status": "completed",
        "completed_mappers_count": 2,
        "completed_reducers_count": 1,
        "created_at": "2024-01-01T00:00:00Z"
    }
    
    async def mock_get(*args): 
        return mock_record

    monkeypatch.setattr(db, "get_job_status_for_user", mock_get)
    
    response = client.get(f"/job_status/{mock_uuid}")
    assert response.status_code == 200
    assert response.json()["status"] == "completed"

def test_get_job_status_not_found(monkeypatch):
    async def mock_get(*args): 
        return None

    monkeypatch.setattr(db, "get_job_status_for_user", mock_get)
    response = client.get(f"/job_status/{uuid4()}")
    assert response.status_code == 404

def test_abort_job(monkeypatch):
    mock_uuid = uuid4()
    
    async def mock_get(*args): return {"status": "pending"}
    async def mock_update(*args): return None
    
    monkeypatch.setattr(db, "get_job_status_for_user", mock_get)
    monkeypatch.setattr(db, "update_job_status", mock_update)
    monkeypatch.setattr(k8s, "terminate_job_pods", lambda jid: None)
    
    response = client.post(f"/abort_job/{mock_uuid}")
    assert response.status_code == 200
    assert "aborted successfully" in response.json()["message"]

def test_get_job_status_scopes_lookup_to_authenticated_user(monkeypatch):
    mock_uuid = uuid4()
    captured = {}

    async def mock_get(job_id, user_id):
        captured["job_id"] = job_id
        captured["user_id"] = user_id
        return {
            "job_id": job_id,
            "status": "mapping",
            "completed_mappers_count": 0,
            "completed_reducers_count": 0,
            "created_at": "2024-01-01T00:00:00Z",
        }

    monkeypatch.setattr(db, "get_job_status_for_user", mock_get)

    response = client.get(f"/job_status/{mock_uuid}")

    assert response.status_code == 200
    assert captured == {"job_id": mock_uuid, "user_id": "mock-user-id"}

def test_get_all_jobs_scopes_lookup_to_authenticated_user(monkeypatch):
    captured = {}

    async def mock_get(user_id):
        captured["user_id"] = user_id
        return [{"job_id": "job-1", "user_id": user_id}]

    monkeypatch.setattr(db, "get_jobs_for_user", mock_get)

    response = client.get("/get_all_jobs")

    assert response.status_code == 200
    assert response.json() == {
        "jobs": [{"job_id": "job-1", "user_id": "mock-user-id"}]
    }
    assert captured == {"user_id": "mock-user-id"}

def test_admin_jobs_uses_admin_scoped_listing(monkeypatch):
    captured = {"called": False}

    async def mock_get():
        captured["called"] = True
        return [{"job_id": "job-1", "user_id": "someone-else"}]

    monkeypatch.setattr(db, "get_all_jobs", mock_get)

    response = client.get("/admin/jobs")

    assert response.status_code == 200
    assert response.json() == {
        "jobs": [{"job_id": "job-1", "user_id": "someone-else"}]
    }
    assert captured["called"] is True


def test_update_job_state_requires_internal_token(monkeypatch):
    mock_uuid = uuid4()

    async def mock_update(*args):
        return None

    monkeypatch.setenv("INTERNAL_UPDATE_TOKEN", "expected-token")
    monkeypatch.setattr(db, "update_job_status", mock_update)

    response = client.post(f"/update_job_state/{mock_uuid}", json={"status": "completed"})

    assert response.status_code == 403

def test_update_job_state_accepts_internal_token(monkeypatch):
    mock_uuid = uuid4()
    captured = {}

    async def mock_update(job_id, status):
        captured["job_id"] = job_id
        captured["status"] = status

    monkeypatch.setenv("INTERNAL_UPDATE_TOKEN", "expected-token")
    monkeypatch.setattr(db, "update_job_status", mock_update)

    response = client.post(
        f"/update_job_state/{mock_uuid}",
        json={"status": "completed"},
        headers={"X-Internal-Token": "expected-token"},
    )

    assert response.status_code == 200
    assert captured == {"job_id": mock_uuid, "status": "completed"}


def test_update_job_state_rejects_invalid_status(monkeypatch):
    monkeypatch.setenv("INTERNAL_UPDATE_TOKEN", "expected-token")

    response = client.post(
        f"/update_job_state/{uuid4()}",
        json={"status": "running"},
        headers={"X-Internal-Token": "expected-token"},
    )

    assert response.status_code == 422
