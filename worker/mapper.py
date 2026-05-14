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

Note: worker_spawner also sends INTERMEDIATE_PREFIX.
  - INTERMEDIATE_PREFIX is not read here; intermediate paths are derived
    directly from JOB_ID for simplicity.
  It is a harmless unused env var.

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

# Buffered range-read size for streaming input from MinIO.
READ_BLOCK_SIZE  = 64 * 1024 * 1024

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

def _read_input_range(object_path: str, offset: int, length: int) -> bytes:
    """
    Read a byte range from the input object and always release the underlying
    HTTP connection back to MinIO's pool.
    """
    response = minio_client.get_object(
        MINIO_BUCKET,
        object_path,
        offset=offset,
        length=length,
    )
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def _iter_owned_line_batches(object_path: str):
    """
    Stream newline-delimited records from MinIO using buffered range reads and
    yield complete owned lines grouped by the input batch that completed them.

    Ownership rule:
      - if OFFSET_START lands mid-line, discard bytes until the next newline
      - a mapper owns every line whose first byte offset is < OFFSET_END
      - if OFFSET_END lands mid-line, keep reading until that line completes
    """
    if OFFSET_END <= OFFSET_START:
        return

    carry = b""
    carry_start = OFFSET_START
    cursor = OFFSET_START
    first_request = True
    skipping_partial_line = False

    while True:
        request_offset = cursor
        request_length = READ_BLOCK_SIZE

        if first_request and OFFSET_START > 0:
            request_offset -= 1
            request_length += 1

        payload = _read_input_range(object_path, request_offset, request_length)
        include_previous_byte = first_request and OFFSET_START > 0
        first_request = False

        if include_previous_byte:
            if not payload:
                return
            skipping_partial_line = payload[:1] != b"\n"
            block = payload[1:]
        else:
            block = payload

        cursor = request_offset + len(payload)

        if not block:
            if carry and not skipping_partial_line and carry_start < OFFSET_END:
                yield [(carry_start, carry)]
            return

        data = carry + block
        data_start = carry_start
        scan_offset = 0

        batch = []

        while True:
            newline_index = data.find(b"\n", scan_offset)
            if newline_index == -1:
                break

            raw_line = data[scan_offset:newline_index + 1]
            line_start = data_start + scan_offset
            scan_offset = newline_index + 1

            if skipping_partial_line:
                skipping_partial_line = False
                continue

            if line_start >= OFFSET_END:
                if batch:
                    yield batch
                return

            batch.append((line_start, raw_line))

        if batch:
            yield batch

        carry = data[scan_offset:]
        carry_start = data_start + scan_offset


def _partition_object_path(reducer_id: int, batch_index: int) -> str:
    return (
        f"intermediate/{JOB_ID}"
        f"/reducer_{reducer_id}"
        f"/from_mapper_{MAP_ID}_chunk_{batch_index:06d}.jsonl"
    )


def _upload_partition_batch(partition_buffers: dict[int, tempfile.SpooledTemporaryFile], batch_index: int) -> int:
    """Upload one reducer shard object per non-empty partition buffer."""
    uploaded = 0

    for reducer_id, buffer in partition_buffers.items():
        file_size = buffer.tell()
        if file_size == 0:
            continue

        buffer.seek(0)
        minio_client.put_object(
            MINIO_BUCKET,
            _partition_object_path(reducer_id, batch_index),
            buffer,
            file_size,
            content_type="application/jsonl",
        )
        uploaded += 1

    return uploaded


def _run_sync_core(user_map, object_path: str = INPUT_PATH) -> int:
    """
    Stream the assigned byte range from MinIO, run the user map function, and
    upload reducer shard objects incrementally per input batch.
    Returns the number of (key, value) pairs emitted.
    """
    pairs_emitted = 0
    batch_index = 0

    for line_batch in _iter_owned_line_batches(object_path):
        partition_buffers: dict[int, tempfile.SpooledTemporaryFile] = {}

        try:
            for line_start, raw_line in line_batch:
                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
                if not line.strip():
                    continue

                for key, value in user_map(str(line_start), line):
                    r_id = partition_key(key)
                    if r_id not in partition_buffers:
                        partition_buffers[r_id] = tempfile.SpooledTemporaryFile(
                            max_size=READ_BLOCK_SIZE,
                            mode="w+b",
                        )
                    partition_buffers[r_id].write(
                        orjson.dumps([key, str(value)]) + b"\n"
                    )
                    pairs_emitted += 1

            _upload_partition_batch(partition_buffers, batch_index)
        finally:
            for buffer in partition_buffers.values():
                buffer.close()

        batch_index += 1

    return pairs_emitted


# ── Main async entry point ────────────────────────────────────────────────────

async def run() -> None:
    await ping("started")
    ping_task = asyncio.create_task(ping_loop())

    try:
        # Step 1 — load user code
        user_map = load_user_map_function(CODE_PATH)

        # Step 2 — stream byte range from MinIO and upload reducer shards per batch.
        pairs_emitted = _run_sync_core(user_map)

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
        if _http_client and not _http_client.is_closed:
            await _http_client.aclose()


if __name__ == "__main__":
    asyncio.run(run())
