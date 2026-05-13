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

    container = client.V1Container(
        name="job-master",
        image="hadoobernetes/job-master:latest",
        image_pull_policy="Always",
        env=[
            client.V1EnvVar(name="JOB_ID", value=str(job_id)),
            client.V1EnvVar(name="DB_HOST", value=os.getenv("POSTGRES_HOST", "postgres")),
        ]
    )

    template = client.V1PodTemplateSpec(
        metadata=client.V1ObjectMeta(labels={"app": "job-master", "job_id": str(job_id)}),
        spec=client.V1PodSpec(restart_policy="Never", containers=[container])
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
    
    label_selector = f"job_id={str(job_id)}"
    
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