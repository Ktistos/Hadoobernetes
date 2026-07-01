from unittest.mock import MagicMock
from uuid import uuid4
import k8s_client

def test_spawn_job_master(monkeypatch):
    mock_batch = MagicMock()
    monkeypatch.setattr(k8s_client.client, "BatchV1Api", lambda: mock_batch)
    monkeypatch.setenv("POSTGRES_HOST", "postgres-service")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_USER", "cm-user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "cm-pass")
    monkeypatch.setenv("POSTGRES_DB", "cm-db")
    monkeypatch.setenv("CLUSTER_MANAGER_INTERNAL_TOKEN", "state-token")
    
    test_id = uuid4()
    k8s_client.spawn_job_master(test_id)
    
    mock_batch.create_namespaced_job.assert_called_once()
    
    # Verify the body passed to the K8s API has the correct name
    call_kwargs = mock_batch.create_namespaced_job.call_args[1]
    job_body = call_kwargs['body']
    container = job_body.spec.template.spec.containers[0]
    assert str(test_id)[:8] in job_body.metadata.name
    assert job_body.spec.template.metadata.labels["job_id"] == str(test_id)
    assert container.ports[0].container_port == 8000
    assert container.readiness_probe.http_get.path == "/readyz"
    assert container.readiness_probe.http_get.port == 8000
    assert container.liveness_probe.http_get.path == "/healthz"
    assert container.liveness_probe.http_get.port == 8000
    env = {item.name: item.value for item in container.env}
    assert env["JOB_ID"] == str(test_id)
    assert env["POSTGRES_HOST"] == "postgres-service"
    assert env["POSTGRES_PORT"] == "5433"
    assert env["POSTGRES_USER"] == "cm-user"
    assert env["POSTGRES_PASSWORD"] == "cm-pass"
    assert env["POSTGRES_DB"] == "cm-db"
    assert env["CLUSTER_MANAGER_INTERNAL_TOKEN"] == "state-token"

def test_terminate_job_pods(monkeypatch):
    mock_batch = MagicMock()
    mock_core = MagicMock()

    job_master = MagicMock()
    job_master.metadata.name = "test-job-master"
    worker_job = MagicMock()
    worker_job.metadata.name = "test-worker-job"
    job_master_pod = MagicMock()
    job_master_pod.metadata.name = "test-job-master-pod"
    worker_pod = MagicMock()
    worker_pod.metadata.name = "test-worker-pod"

    mock_batch.list_namespaced_job.side_effect = [
        MagicMock(items=[job_master]),
        MagicMock(items=[worker_job]),
    ]
    mock_core.list_namespaced_pod.side_effect = [
        MagicMock(items=[job_master_pod]),
        MagicMock(items=[worker_pod]),
    ]

    monkeypatch.setattr(k8s_client.client, "BatchV1Api", lambda: mock_batch)
    monkeypatch.setattr(k8s_client.client, "CoreV1Api", lambda: mock_core)

    test_id = uuid4()
    k8s_client.terminate_job_pods(test_id)

    assert [call.kwargs["label_selector"] for call in mock_batch.list_namespaced_job.call_args_list] == [
        f"job_id={test_id}",
        f"mr-job-id={str(test_id)[:8]}",
    ]
    assert [call.kwargs["label_selector"] for call in mock_core.list_namespaced_pod.call_args_list] == [
        f"job_id={test_id}",
        f"mr-job-id={str(test_id)[:8]}",
    ]
    assert [call.kwargs["name"] for call in mock_batch.delete_namespaced_job.call_args_list] == [
        "test-job-master",
        "test-worker-job",
    ]
    assert [call.kwargs["name"] for call in mock_core.delete_namespaced_pod.call_args_list] == [
        "test-job-master-pod",
        "test-worker-pod",
    ]
