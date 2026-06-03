import pytest
from unittest.mock import MagicMock
from uuid import uuid4
import k8s_client

def test_spawn_job_master(monkeypatch):
    mock_batch = MagicMock()
    mock_core = MagicMock()
    monkeypatch.setattr(k8s_client.client, "BatchV1Api", lambda: mock_batch)
    monkeypatch.setattr(k8s_client.client, "CoreV1Api", lambda: mock_core)
    monkeypatch.setenv("POSTGRES_HOST", "postgres-service")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_USER", "cm-user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "cm-pass")
    monkeypatch.setenv("POSTGRES_DB", "cm-db")
    monkeypatch.setenv("CLUSTER_MANAGER_URL", "http://cluster-manager:8000")
    monkeypatch.setenv("MINIO_ENDPOINT", "minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "access")
    monkeypatch.setenv("MINIO_SECRET_KEY", "secret")
    monkeypatch.setenv("MINIO_BUCKET", "bucket")
    monkeypatch.setenv("INTERNAL_UPDATE_TOKEN", "internal-token")

    test_id = uuid4()
    k8s_client.spawn_job_master(test_id)

    mock_core.create_namespaced_service.assert_called_once()
    mock_batch.create_namespaced_job.assert_called_once()

    service_body = mock_core.create_namespaced_service.call_args[1]["body"]
    assert service_body.metadata.name == f"job-master-{str(test_id)}"
    assert service_body.spec.selector == {"app": "job-master", "mr-job-id": str(test_id)}

    job_body = mock_batch.create_namespaced_job.call_args[1]["body"]
    container = job_body.spec.template.spec.containers[0]
    assert str(test_id)[:8] in job_body.metadata.name
    assert job_body.metadata.labels["mr-job-id"] == str(test_id)
    assert job_body.spec.template.metadata.labels["mr-job-id"] == str(test_id)
    assert job_body.spec.template.spec.service_account_name == "mapreduce-sa"
    assert container.image_pull_policy == "Always"
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
    assert env["CLUSTER_MANAGER_URL"] == "http://cluster-manager:8000"
    assert env["MINIO_ENDPOINT"] == "minio:9000"
    assert env["MINIO_ACCESS_KEY"] == "access"
    assert env["MINIO_SECRET_KEY"] == "secret"
    assert env["MINIO_BUCKET"] == "bucket"
    assert env["INTERNAL_UPDATE_TOKEN"] == "internal-token"
    assert env["JOB_MASTER_SERVICE_URL"] == f"http://job-master-{str(test_id)}.mapreduce.svc.cluster.local:8000"

def test_terminate_job_pods(monkeypatch):
    mock_batch = MagicMock()
    mock_core = MagicMock()
    
    # Setup mock items returned by list queries
    mock_job = MagicMock()
    mock_job.metadata.name = "test-job-master"
    mock_batch.list_namespaced_job.return_value.items = [mock_job]
    
    mock_pod = MagicMock()
    mock_pod.metadata.name = "test-pod-worker"
    mock_core.list_namespaced_pod.return_value.items = [mock_pod]
    
    monkeypatch.setattr(k8s_client.client, "BatchV1Api", lambda: mock_batch)
    monkeypatch.setattr(k8s_client.client, "CoreV1Api", lambda: mock_core)
    
    test_id = uuid4()
    k8s_client.terminate_job_pods(test_id)
    
    mock_batch.delete_namespaced_job.assert_called_once_with(
        name="test-job-master", 
        namespace="mapreduce", 
        propagation_policy="Background"
    )
    mock_core.delete_namespaced_pod.assert_called_once_with(
        name="test-pod-worker", 
        namespace="mapreduce"
    )
    mock_core.list_namespaced_service.assert_called_once_with(
        namespace="mapreduce",
        label_selector=f"mr-job-id={str(test_id)}",
    )


def test_spawn_job_master_cleans_up_service_when_job_creation_fails(monkeypatch):
    mock_batch = MagicMock()
    mock_core = MagicMock()
    mock_batch.create_namespaced_job.side_effect = RuntimeError("job create failed")

    monkeypatch.setattr(k8s_client.client, "BatchV1Api", lambda: mock_batch)
    monkeypatch.setattr(k8s_client.client, "CoreV1Api", lambda: mock_core)
    monkeypatch.setenv("INTERNAL_UPDATE_TOKEN", "internal-token")

    test_id = uuid4()
    with pytest.raises(RuntimeError, match="job create failed"):
        k8s_client.spawn_job_master(test_id)

    mock_core.delete_namespaced_service.assert_called_once_with(
        name=f"job-master-{str(test_id)}",
        namespace="mapreduce",
    )

def test_spawn_job_master_does_not_delete_preexisting_service_on_job_failure(monkeypatch):
    exc = k8s_client.client.exceptions.ApiException(status=409)
    mock_batch = MagicMock()
    mock_core = MagicMock()
    mock_core.create_namespaced_service.side_effect = exc
    mock_batch.create_namespaced_job.side_effect = RuntimeError("job create failed")

    monkeypatch.setattr(k8s_client.client, "BatchV1Api", lambda: mock_batch)
    monkeypatch.setattr(k8s_client.client, "CoreV1Api", lambda: mock_core)
    monkeypatch.setenv("INTERNAL_UPDATE_TOKEN", "internal-token")

    with pytest.raises(RuntimeError, match="job create failed"):
        k8s_client.spawn_job_master(uuid4())

    mock_core.delete_namespaced_service.assert_not_called()
