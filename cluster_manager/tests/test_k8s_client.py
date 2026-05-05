from unittest.mock import MagicMock
from uuid import uuid4
import k8s_client

def test_spawn_job_master(monkeypatch):
    mock_batch = MagicMock()
    monkeypatch.setattr(k8s_client.client, "BatchV1Api", lambda: mock_batch)
    
    test_id = uuid4()
    k8s_client.spawn_job_master(test_id)
    
    mock_batch.create_namespaced_job.assert_called_once()
    
    # Verify the body passed to the K8s API has the correct name
    call_kwargs = mock_batch.create_namespaced_job.call_args[1]
    job_body = call_kwargs['body']
    assert str(test_id)[:8] in job_body.metadata.name
    assert job_body.spec.template.metadata.labels["job_id"] == str(test_id)

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