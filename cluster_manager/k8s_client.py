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
    core_v1 = client.CoreV1Api()
    namespace = os.getenv("K8S_NAMESPACE", "mapreduce")
    job_name = f"job-master-{str(job_id)}"
    service_name = job_name  # headless Service shares the Job's name
    # Stable in-cluster DNS for this job's master, independent of pod IP.
    # Survives master pod restarts (K8s recreates the pod under the same Job;
    # the Service re-targets the new Ready pod automatically).
    job_master_service_url = f"http://{service_name}.{namespace}.svc.cluster.local:8000"
    container = client.V1Container(
        name="job-master",
        image="hadoobernetes/job-master:latest",
        image_pull_policy="Always",
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
            client.V1EnvVar(name="K8S_NAMESPACE", value=namespace),
            client.V1EnvVar(name="JOB_ID", value=str(job_id)),
            client.V1EnvVar(name="POSTGRES_HOST", value=os.getenv("POSTGRES_HOST", "postgres")),
            client.V1EnvVar(name="POSTGRES_PORT", value=os.getenv("POSTGRES_PORT", os.getenv("DB_PORT", "5432"))),
            client.V1EnvVar(name="POSTGRES_USER", value=os.getenv("POSTGRES_USER", "admin")),
            client.V1EnvVar(name="POSTGRES_PASSWORD", value=os.getenv("POSTGRES_PASSWORD", "admin")),
            client.V1EnvVar(name="POSTGRES_DB", value=os.getenv("POSTGRES_DB", "mapreduce")),
            client.V1EnvVar(name="CLUSTER_MANAGER_URL", value=os.getenv("CLUSTER_MANAGER_URL", "http://cluster-manager:8000")),
            client.V1EnvVar(name="MINIO_ENDPOINT", value=os.getenv("MINIO_ENDPOINT", "minio.minio-tenant.svc.cluster.local:80")),
            client.V1EnvVar(name="MINIO_ACCESS_KEY", value=os.getenv("MINIO_ACCESS_KEY", "minioadmin")),
            client.V1EnvVar(name="MINIO_SECRET_KEY", value=os.getenv("MINIO_SECRET_KEY", "minioadmin")),
            client.V1EnvVar(name="MINIO_BUCKET", value=os.getenv("MINIO_BUCKET", "mapreduce")),
            client.V1EnvVar(name="JOB_MASTER_SERVICE_URL", value=job_master_service_url),
            client.V1EnvVar(name="INTERNAL_UPDATE_TOKEN", value=os.environ["INTERNAL_UPDATE_TOKEN"]),
            client.V1EnvVar(
                name="POD_IP",
                value_from=client.V1EnvVarSource(
                    field_ref=client.V1ObjectFieldSelector(field_path="status.podIP")
                )
            )
        ]
    )
    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "job-master", "mr-job-id": str(job_id)}),
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
        metadata=client.V1ObjectMeta(name=job_name, labels={"app": "job-master", "mr-job-id": str(job_id)}),
        spec=job_spec
    )

    # Create the headless Service FIRST. It gives the master a stable DNS name
    # (selects on the full job_id label, so it always routes to the current
    # master pod even after a restart; clusterIP=None keeps it headless).
    # Creating it before the Job means any failure here (RBAC, naming, API)
    # fails the submit cleanly without leaving an orphaned master pod running.
    service = client.V1Service(
        api_version="v1",
        kind="Service",
        metadata=client.V1ObjectMeta(
            name=service_name,
            labels={"app": "job-master", "mr-job-id": str(job_id)},
        ),
        spec=client.V1ServiceSpec(
            cluster_ip="None",
            selector={"app": "job-master", "mr-job-id": str(job_id)},
            ports=[client.V1ServicePort(port=8000, target_port=8000, name="http")],
        ),
    )
    service_created = False
    try:
        core_v1.create_namespaced_service(body=service, namespace=namespace)
        service_created = True
    except client.exceptions.ApiException as exc:
        if exc.status != 409:  # 409 = already exists (idempotent resubmit)
            raise

    try:
        batch_v1.create_namespaced_job(body=job, namespace=namespace)
    except Exception:
        if service_created:
            try:
                core_v1.delete_namespaced_service(name=service_name, namespace=namespace)
            except client.exceptions.ApiException:
                pass
        raise
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
    namespace = os.getenv("K8S_NAMESPACE", "mapreduce")
    label_selector = f"mr-job-id={str(job_id)}"
    jobs = batch_v1.list_namespaced_job(namespace=namespace, label_selector=label_selector)
    for j in jobs.items:
        batch_v1.delete_namespaced_job(
            name=j.metadata.name,
            namespace=namespace,
            propagation_policy="Background"
        )
    pods = core_v1.list_namespaced_pod(namespace=namespace, label_selector=label_selector)
    for p in pods.items:
        core_v1.delete_namespaced_pod(name=p.metadata.name, namespace=namespace)
    services = core_v1.list_namespaced_service(namespace=namespace, label_selector=label_selector)
    for s in services.items:
        core_v1.delete_namespaced_service(name=s.metadata.name, namespace=namespace)