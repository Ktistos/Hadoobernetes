"""
worker/mapper.py
=================
Mapper worker pod.  Executed as a Kubernetes Job spawned by the Job Master.

Environment variables injected by worker_spawner.py
-----------------------------------------------------
Required:
  JOB_MASTER_URL    URL of the Job Master Service for this job
  WORKER_ID         e.g. "mapper_3"
  JOB_ID            UUID of the parent job
  MAP_ID            Integer index of this mapper
  OFFSET_START      Byte offset into INPUT_PATH where this chunk begins
  OFFSET_END        Byte offset where this chunk ends (exclusive)
  NUM_REDUCERS      Total reducer count (used for partitioning)
  INPUT_PATH        MinIO object path of the input file  (jobs.input_data_path)
  CODE_PATH         MinIO object path of the user's .py  (jobs.code_location)
  MINIO_ENDPOINT    e.g. minio-service:9000
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  MINIO_BUCKET

Optional:
  PING_INTERVAL     Heartbeat cadence in seconds (default 10)

Note: worker_spawner also sends INTERMEDIATE_PREFIX and DATABASE_URL.
  - INTERMEDIATE_PREFIX is not read here; intermediate paths are derived
    directly from JOB_ID for simplicity.
  - DATABASE_URL is not read here; workers use MinIO, not PostgreSQL.
  Both are harmless unused env vars.

Design-doc references
----------------------
  §3.2  Worker Ping API  — worker_id, worker_type, status
  §5.2  Upload Input Data
  §5.4  Job Execution Workflow (mapper phase)
"""

import hashlib
import importlib.util
import os
import tempfile
import asyncio

import httpx
import orjson
from minio import Minio

# ── Environment ──────────────────────────────────────────────────────────────

JOB_MASTER_URL   = os.environ["JOB_MASTER_URL"]
WORKER_ID        = os.environ["WORKER_ID"]
JOB_ID           = os.environ["JOB_ID"]
MAP_ID           = int(os.environ["MAP_ID"])
OFFSET_START     = int(os.environ["OFFSET_START"])
OFFSET_END       = int(os.environ["OFFSET_END"])
NUM_REDUCERS     = int(os.environ["NUM_REDUCERS"])
INPUT_PATH       = os.environ["INPUT_PATH"]
CODE_PATH        = os.environ["CODE_PATH"]
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET     = os.environ["MINIO_BUCKET"]
PING_INTERVAL    = int(os.environ.get("PING_INTERVAL", "10"))

# ── Clients ───────────────────────────────────────────────────────────────────

minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

# Persistent HTTP client (Opt 3.2): one TCP connection reused for all pings,
# eliminating repeated handshake overhead.  Closed in the finally block.
_http_client: httpx.AsyncClient | None = None


async def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient()
    return _http_client


# ── Heartbeat (design doc §3.2) ───────────────────────────────────────────────

async def ping(status: str) -> None:
    """Send a status update to the Job Master.  Never raises — a ping failure
    must not crash the worker."""
    client = await _get_http_client()
    try:
        await client.post(
            f"{JOB_MASTER_URL}/worker_ping",
            json={"worker_id": WORKER_ID, "worker_type": "mapper", "status": status},
            timeout=5,
        )
    except Exception as exc:
        print(f"[mapper_{MAP_ID}] Ping '{status}' failed: {exc}")


async def ping_loop() -> None:
    """Background task — sends 'alive' every PING_INTERVAL seconds."""
    while True:
        await asyncio.sleep(PING_INTERVAL)
        await ping("alive")


# ── User code loading ─────────────────────────────────────────────────────────

def load_user_map_function(code_path: str):
    """Download user's .py from MinIO and return its map() function."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
        minio_client.fget_object(MINIO_BUCKET, code_path, f.name)
        spec   = importlib.util.spec_from_file_location("user_code", f.name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.map


# ── Partitioning ──────────────────────────────────────────────────────────────

def partition_key(key: str) -> int:
    """Consistent hash to assign a key to one of NUM_REDUCERS buckets."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_REDUCERS


# ── Core logic ────────────────────────────────────────────────────────────────

def _run_sync_core(user_map, tmp_input_path: str, partition_handles: dict) -> int:
    """
    Read the assigned byte range, run the user map function, and write
    JSONL partitions.
    Returns the number of (key, value) pairs emitted.
    """
    pairs_emitted = 0
    is_first_line  = True
    bytes_consumed = 0
    chunk_size     = OFFSET_END - OFFSET_START

    with open(tmp_input_path, "rb") as f:
        f.seek(OFFSET_START)

        for raw_line in f:
            bytes_consumed += len(raw_line)

            # Skip the first partial line when starting mid-file.
            if is_first_line:
                is_first_line = False
                if OFFSET_START > 0 and bytes_consumed <= chunk_size:
                    continue

            line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
            if not line.strip():
                if bytes_consumed >= chunk_size:
                    break
                continue

            pairs = list(user_map(str(OFFSET_START + bytes_consumed), line))

            for key, value in pairs:
                r_id = partition_key(key)
                partition_handles[r_id].write(
                    orjson.dumps([key, str(value)]).decode() + "\n"
                )
                pairs_emitted += 1

            if bytes_consumed >= chunk_size:
                break

    return pairs_emitted


# ── Main async entry point ────────────────────────────────────────────────────

async def run() -> None:
    await ping("started")
    ping_task = asyncio.create_task(ping_loop())

    tmp_input_path    = None
    partition_handles = {}
    partition_paths   = {}

    try:
        # Step 1 — load user code
        user_map = load_user_map_function(CODE_PATH)

        # Step 2 — download input file to local disk
        with tempfile.NamedTemporaryFile(delete=False, suffix=".input") as f:
            minio_client.fget_object(MINIO_BUCKET, INPUT_PATH, f.name)
            tmp_input_path = f.name

        # Step 3 — open one JSONL partition file per reducer (Opt 1.2)
        for reducer_id in range(NUM_REDUCERS):
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
            )
            partition_handles[reducer_id] = tmp
            partition_paths[reducer_id]   = tmp.name

        # Step 4 — stream byte range and emit pairs (Opt 1.1)
        pairs_emitted = _run_sync_core(user_map, tmp_input_path, partition_handles)

        # Step 4a — flush and close all partition handles
        for fh in partition_handles.values():
            fh.close()
        partition_handles = {}

        # Step 5 — upload partition files to MinIO (Opt 1.3)
        # Path format groups files by destination reducer:
        #   intermediate/{JOB_ID}/reducer_{r_id}/from_mapper_{MAP_ID}.jsonl
        # This lets the reducer use list_objects() on its own prefix instead
        # of blindly polling every mapper folder.
        for reducer_id, local_path in partition_paths.items():
            remote_path = (
                f"intermediate/{JOB_ID}"
                f"/reducer_{reducer_id}"
                f"/from_mapper_{MAP_ID}.jsonl"
            )
            file_size = os.path.getsize(local_path)
            with open(local_path, "rb") as fh:
                minio_client.put_object(
                    MINIO_BUCKET, remote_path,
                    fh, file_size,
                    content_type="application/jsonl",
                )

        prefix = f"mapper_{MAP_ID}"
        print(f"[{prefix}] Total pairs emitted: {pairs_emitted}")

        ping_task.cancel()
        await ping("completed")
        print(f"[{prefix}] Mapper completed successfully.")

    except Exception as exc:
        ping_task.cancel()
        await ping("failed")
        raise exc

    finally:
        if tmp_input_path and os.path.exists(tmp_input_path):
            os.unlink(tmp_input_path)

        for fh in partition_handles.values():
            try:
                fh.close()
            except Exception:
                pass

        for local_path in partition_paths.values():
            try:
                os.unlink(local_path)
            except Exception:
                pass

        if _http_client and not _http_client.is_closed:
            await _http_client.aclose()


if __name__ == "__main__":
    asyncio.run(run())
