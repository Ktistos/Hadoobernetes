"""
Kubernetes API interaction layer for the Cluster Manager.

This module is responsible for directly communicating with the Kubernetes control plane
to dynamically spawn orchestrator pods and tear down workloads upon job aborts.
"""

import os
from kubernetes import client, config
from uuid import UUID

def init_k8s():
    """
    Loads the Kubernetes configuration.
    
    Attempts to load the in-cluster service account configuration first. If running
    locally or outside the cluster, it falls back to the local ~/.kube/config.
    """
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()

def spawn_job_master(job_id: UUID):
    """
    Constructs and submits a Kubernetes Job manifest to spawn the Job Master pod.
    
    The Job Master is responsible for the granular execution of the Map-Reduce phases.
    It receives the job_id and database credentials via environment variables.
    
    Args:
        job_id (UUID): The identifier of the job this master pod will orchestrate.
    """
    batch_v1 = client.BatchV1Api()
    namespace = "mapreduce"
    job_name = f"job-master-{str(job_id)[:8]}"

    postgres_port = os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432"))
    if "://" in postgres_port:
        postgres_port = postgres_port.split(":")[-1]

    container = client.V1Container(
        name="job-master",
        image="hadoobernetes/job-master:latest",
        image_pull_policy="Never", # Use local image for development; change to "IfNotPresent" or "Always" for production
        ports=[
            client.V1ContainerPort(container_port=8000),
        ],
        readiness_probe=client.V1Probe(
            http_get=client.V1HTTPGetAction(path="/readyz", port=8000),
            initial_delay_seconds=3,
            period_seconds=5,
            timeout_seconds=3,
            failure_threshold=3,
        ),
        liveness_probe=client.V1Probe(
            http_get=client.V1HTTPGetAction(path="/healthz", port=8000),
            initial_delay_seconds=20,
            period_seconds=10,
            timeout_seconds=3,
            failure_threshold=3,
        ),
        env=[
            client.V1EnvVar(name="K8S_NAMESPACE", value="mapreduce"),
            client.V1EnvVar(name="JOB_ID", value=str(job_id)),
            client.V1EnvVar(name="POSTGRES_HOST", value=os.getenv("POSTGRES_HOST", "postgres")),
            client.V1EnvVar(name="POSTGRES_PORT", value=postgres_port),
            client.V1EnvVar(name="POSTGRES_USER", value=os.getenv("POSTGRES_USER", "admin")),
            client.V1EnvVar(name="POSTGRES_PASSWORD", value=os.getenv("POSTGRES_PASSWORD", "admin")),
            client.V1EnvVar(name="POSTGRES_DB", value=os.getenv("POSTGRES_DB", "mapreduce")),
            client.V1EnvVar(name="CLUSTER_MANAGER_URL", value="http://cluster-manager:8000"),
            client.V1EnvVar(name="CLUSTER_MANAGER_INTERNAL_TOKEN", value=os.getenv("CLUSTER_MANAGER_INTERNAL_TOKEN", "")),
            # client.V1EnvVar(name="MINIO_ENDPOINT", value="minio:80"),
            client.V1EnvVar(name="MINIO_ENDPOINT", value="minio.minio-tenant.svc.cluster.local:80"),
            client.V1EnvVar(name="MINIO_ACCESS_KEY", value="minioadmin"),
            client.V1EnvVar(name="MINIO_SECRET_KEY", value="minioadmin"),
            client.V1EnvVar(name="MINIO_BUCKET", value="mapreduce"),
            client.V1EnvVar(
                name="POD_IP",
                value_from=client.V1EnvVarSource(
                    field_ref=client.V1ObjectFieldSelector(field_path="status.podIP")
                )
            )
        ]
    )

    # template = client.V1PodTemplateSpec(
    #     metadata=client.V1ObjectMeta(labels={"app": "job-master", "job_id": str(job_id)}),
    #     spec=client.V1PodSpec(restart_policy="Never", containers=[container])
    # )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "job-master", "job_id": str(job_id)}),
        spec=client.V1PodSpec(
            restart_policy="Never", 
            service_account_name="mapreduce-sa",
            containers=[container]
        )
    )

    job_spec = client.V1JobSpec(
        template=template,
        backoff_limit=5
    )

    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name),
        spec=job_spec
    )

    batch_v1.create_namespaced_job(body=job, namespace=namespace)

def terminate_job_pods(job_id: UUID):
    """
    Forcefully terminates the Job Master and all associated worker pods for a given job.
    
    This is executed during an abort workflow. It queries the Kubernetes API using
    label selectors to identify and delete all resources tied to the specific job_id.
    
    Args:
        job_id (UUID): The identifier of the job whose pods should be terminated.
    """
    batch_v1 = client.BatchV1Api()
    core_v1 = client.CoreV1Api()
    namespace = "mapreduce"
    
    label_selectors = [
        f"job_id={str(job_id)}",
        f"mr-job-id={str(job_id)[:8]}",
    ]
    deleted_jobs: set[str] = set()
    deleted_pods: set[str] = set()

    for label_selector in label_selectors:
        jobs = batch_v1.list_namespaced_job(namespace=namespace, label_selector=label_selector)
        for j in jobs.items:
            if j.metadata.name in deleted_jobs:
                continue
            batch_v1.delete_namespaced_job(
                name=j.metadata.name,
                namespace=namespace,
                propagation_policy="Background"
            )
            deleted_jobs.add(j.metadata.name)

        pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
        for p in pods.items:
            if p.metadata.name in deleted_pods:
                continue
            core_v1.delete_namespaced_pod(name=p.metadata.name, namespace=namespace)
            deleted_pods.add(p.metadata.name)
