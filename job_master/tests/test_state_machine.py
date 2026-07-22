import asyncio

import state_machine
from state_machine import JobStateMachine, _object_path_under


def test_object_path_under_normalizes_prefix_slashes():
    assert _object_path_under("outputs/job-1", "part_0.json") == "outputs/job-1/part_0.json"
    assert _object_path_under("outputs/job-1/", "/part_0.json") == "outputs/job-1/part_0.json"


def test_notify_cluster_manager_sends_internal_token(monkeypatch):
    captured = {}

    class FakeResponse:
        def raise_for_status(self):
            return None

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url, json, headers, timeout):
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            captured["timeout"] = timeout
            return FakeResponse()

    monkeypatch.setenv("CLUSTER_MANAGER_URL", "http://cluster-manager:8000")
    monkeypatch.setenv("CLUSTER_MANAGER_INTERNAL_TOKEN", "state-token")
    monkeypatch.setattr(state_machine.httpx, "AsyncClient", FakeClient)

    machine = JobStateMachine("job-123")
    asyncio.run(machine._notify_cluster_manager("completed"))

    assert captured == {
        "url": "http://cluster-manager:8000/update_job_state/job-123",
        "json": {"job_id": "job-123", "status": "completed"},
        "headers": {"X-Internal-Token": "state-token"},
        "timeout": 10,
    }


def test_handle_ping_invalid_worker_id():
    import pytest
    machine = JobStateMachine("job-123")
    
    with pytest.raises(ValueError) as exc:
        asyncio.run(machine.handle_ping("invalid", "mapper", "started"))
    assert "Invalid worker_id format" in str(exc.value)

    with pytest.raises(ValueError) as exc:
        asyncio.run(machine.handle_ping("mapper_abc", "mapper", "started"))
    assert "Invalid worker_id format" in str(exc.value)
