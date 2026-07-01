"""
Job Master — worker_spawner.py
================================
Creates and deletes Kubernetes Jobs for mapper and reducer worker pods.

Design-doc alignment
---------------------
Workers are implemented as Kubernetes Jobs (§4 — Worker Pods (Mappers & Reducers)).
  backoff_limit = 0   so Kubernetes never auto-retries — the Job Master is the
                      sole retry authority (§5.4 fault-tolerance model).
  restart_policy = Never   so a failed pod stays visible for log inspection.

The Job Master pod must run under a ServiceAccount that has:
  verbs: [create, delete, get, list, watch]
  resources: [jobs, pods]
  apiGroups: [batch, ""]
See k8s/job-master-rbac.yaml.

Environment variables consumed here (all set on the Job Master pod):
  K8S_NAMESPACE           (optional, default "default")
  JOB_MASTER_SERVICE_URL  URL workers use to call /worker_ping
  MINIO_ENDPOINT
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  MINIO_BUCKET
  PING_INTERVAL           (optional, default "10")
"""

import logging
import os

from kubernetes import client as k8s_client, config as k8s_config

logger = logging.getLogger(__name__)

_NAMESPACE = os.environ.get("K8S_NAMESPACE", "default")
_WORKER_IMAGE = "mapreduce-worker:latest"

# ---------------------------------------------------------------------------
# Kubernetes client — lazy initialisation
# ---------------------------------------------------------------------------
# We defer loading the kube config until the first actual API call so that:
#   1. Unit tests can import this module without a live cluster / kubeconfig.
#   2. The Job Master pod starts up even if the k8s API is momentarily slow.

_batch_v1: k8s_client.BatchV1Api | None = None


def _get_batch_v1() -> k8s_client.BatchV1Api:
    """Return (and lazily create) the BatchV1Api client."""
    global _batch_v1
    if _batch_v1 is None:
        try:
            k8s_config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
            logger.info("Loaded local kube config (development mode)")
        _batch_v1 = k8s_client.BatchV1Api()
    return _batch_v1


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _job_name(role: str, job_id: str, task_id: int, attempt: int) -> str:
    """
    Produces a deterministic, DNS-safe Kubernetes Job name.
    Format:  mr-{role}-{job_id_prefix8}-{task_id}-{attempt}
    Example: mr-mapper-a1b2c3d4-0-1
    Max length is well within the 63-char Kubernetes name limit.
    """
    return f"mr-{role}-{job_id[:8]}-{task_id}-{attempt}"


def _base_env(job_id: str, job: dict, config: dict) -> list[k8s_client.V1EnvVar]:
    """
    Environment variables shared by both mapper and reducer pods.
    All values come from the jobs and job_config rows loaded by the state machine
    (field names match §6 ER diagram exactly).
    """
    return [
        k8s_client.V1EnvVar(name="JOB_ID",               value=str(job_id)),
        k8s_client.V1EnvVar(name="JOB_MASTER_URL",        value=os.environ["JOB_MASTER_SERVICE_URL"]),
        k8s_client.V1EnvVar(name="MINIO_ENDPOINT",        value=os.environ["MINIO_ENDPOINT"]),
        k8s_client.V1EnvVar(name="MINIO_ACCESS_KEY",      value=os.environ["MINIO_ACCESS_KEY"]),
        k8s_client.V1EnvVar(name="MINIO_SECRET_KEY",      value=os.environ["MINIO_SECRET_KEY"]),
        k8s_client.V1EnvVar(name="MINIO_BUCKET",          value=os.environ["MINIO_BUCKET"]),
        # jobs table columns forwarded as env vars
        k8s_client.V1EnvVar(name="INPUT_PATH",            value=str(job["input_data_path"])),
        k8s_client.V1EnvVar(name="CODE_PATH",             value=str(job["code_location"])),
        k8s_client.V1EnvVar(name="INTERMEDIATE_PREFIX",   value=str(job["intermediate_prefix"])),
        # job_config columns forwarded as env vars
        k8s_client.V1EnvVar(name="NUM_MAPPERS",           value=str(config["num_mappers"])),
        k8s_client.V1EnvVar(name="NUM_REDUCERS",          value=str(config["num_reducers"])),
        k8s_client.V1EnvVar(name="PING_INTERVAL",         value=os.environ.get("PING_INTERVAL", "10")),
    ]


def _create_k8s_job(
    job_name:  str,
    command:   list[str],
    env_vars:  list[k8s_client.V1EnvVar],
    labels:    dict[str, str],
) -> None:
    """
    Submits a Kubernetes Job to the cluster.
    backoff_limit=0: Kubernetes must NOT retry workers — the Job Master does that.
    restart_policy=Never: keeps the failed pod around for log inspection.
    """
    job_body = k8s_client.V1Job(
        api_version = "batch/v1",
        kind        = "Job",
        metadata    = k8s_client.V1ObjectMeta(
            name      = job_name,
            namespace = _NAMESPACE,
            labels    = labels,
        ),
        spec = k8s_client.V1JobSpec(
            backoff_limit = 0,          # CRITICAL — see module docstring
            ttl_seconds_after_finished = 600,   # auto-cleanup after 10 min
            template = k8s_client.V1PodTemplateSpec(
                metadata = k8s_client.V1ObjectMeta(labels=labels),
                spec     = k8s_client.V1PodSpec(
                    restart_policy = "Never",   # CRITICAL — see module docstring
                    containers     = [
                        k8s_client.V1Container(
                            name    = "worker",
                            image   = _WORKER_IMAGE,
                            image_pull_policy="Never",
                            command = command,
                            env     = env_vars,
                            # Resource requests — tune for your cluster
                            resources = k8s_client.V1ResourceRequirements(
                                requests = {"cpu": "100m", "memory": "256Mi"},
                                limits   = {"cpu": "500m", "memory": "512Mi"},
                            ),
                        )
                    ],
                ),
            ),
        ),
    )

    try:
        _get_batch_v1().create_namespaced_job(namespace=_NAMESPACE, body=job_body)
        logger.info(f"Kubernetes Job '{job_name}' created in namespace '{_NAMESPACE}'")
    except k8s_client.exceptions.ApiException as exc:
        if exc.status == 409:
            # Job already exists (possible duplicate call) — log and continue
            logger.warning(f"Kubernetes Job '{job_name}' already exists — skipping creation")
        else:
            logger.error(f"Failed to create Kubernetes Job '{job_name}': {exc}")
            raise


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def spawn_mapper(
    job_id:       str,
    map_id:       int,
    attempt:      int,
    config:       dict,   # job_config row
    job:          dict,   # jobs row
    offset_start: int,
    offset_end:   int,
) -> None:
    """
    Spawn a mapper worker Kubernetes Job.

    The mapper pod will:
      1. Stream INPUT_PATH from MinIO in buffered range reads
      2. Own lines according to [OFFSET_START, OFFSET_END) boundary rules
      3. Run the user's map() function from CODE_PATH
      4. Upload reducer-specific JSONL shard objects to MinIO under
         intermediate/{JOB_ID}/reducer_{REDUCER_ID}/from_mapper_{MAP_ID}_chunk_{BATCH}.jsonl
      5. Send pings to JOB_MASTER_URL/worker_ping
    """
    job_name = _job_name("mapper", job_id, map_id, attempt)
    labels   = {
        "app":          "mr-worker",
        "mr-role":      "mapper",
        "job_id":       job_id,
        "mr-job-id":    job_id[:8],
        "mr-map-id":    str(map_id),
    }

    extra_env = [
        k8s_client.V1EnvVar(name="WORKER_ID",     value=f"mapper_{map_id}"),
        k8s_client.V1EnvVar(name="MAP_ID",         value=str(map_id)),
        k8s_client.V1EnvVar(name="OFFSET_START",   value=str(offset_start)),
        k8s_client.V1EnvVar(name="OFFSET_END",     value=str(offset_end)),
    ]

    env_vars = _base_env(job_id, job, config) + extra_env
    _create_k8s_job(job_name, ["python", "/app/worker/mapper.py"], env_vars, labels)


def spawn_reducer(
    job_id:      str,
    reduce_id:   int,
    attempt:     int,
    config:      dict,   # job_config row
    job:         dict,   # jobs row
    output_path: str,    # final output object path in MinIO (reduce_tasks.output_data_path)
) -> None:
    """
    Spawn a reducer worker Kubernetes Job.

    The reducer pod will:
      1. Fetch all mapper partition files for its reduce_id from MinIO
      2. Sort by key and group values
      3. Run the user's reduce() function from CODE_PATH
      4. Write results to OUTPUT_PATH in MinIO
      5. Send pings to JOB_MASTER_URL/worker_ping
    """
    job_name = _job_name("reducer", job_id, reduce_id, attempt)
    labels   = {
        "app":           "mr-worker",
        "mr-role":       "reducer",
        "job_id":        job_id,
        "mr-job-id":     job_id[:8],
        "mr-reduce-id":  str(reduce_id),
    }

    extra_env = [
        k8s_client.V1EnvVar(name="WORKER_ID",    value=f"reducer_{reduce_id}"),
        k8s_client.V1EnvVar(name="REDUCER_ID",   value=str(reduce_id)),
        k8s_client.V1EnvVar(name="OUTPUT_PATH",  value=output_path),
    ]

    env_vars = _base_env(job_id, job, config) + extra_env
    _create_k8s_job(job_name, ["python", "/app/worker/reducer.py"], env_vars, labels)


def delete_worker_job(role: str, job_id: str, task_id: int, attempt: int) -> None:
    """
    Delete a Kubernetes Job (and its pod) by name.
    Called when aborting a job (§5.7 Abort Job) or cleaning up straggler pods
    after all tasks in a phase complete.
    propagation_policy=Foreground ensures the pod is also deleted.
    """
    job_name = _job_name(role, job_id, task_id, attempt)
    try:
        _get_batch_v1().delete_namespaced_job(
            name      = job_name,
            namespace = _NAMESPACE,
            body      = k8s_client.V1DeleteOptions(propagation_policy="Foreground"),
        )
        logger.info(f"Deleted Kubernetes Job '{job_name}'")
    except k8s_client.exceptions.ApiException as exc:
        if exc.status == 404:
            logger.warning(f"Kubernetes Job '{job_name}' not found — already deleted?")
        else:
            logger.error(f"Failed to delete Kubernetes Job '{job_name}': {exc}")
