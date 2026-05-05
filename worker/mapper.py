import os, hashlib, asyncio, importlib.util, tempfile
from minio import Minio

# orjson is faster than stdlib json for both serialisation and deserialisation.
# It returns bytes from dumps() instead of str — handled explicitly below.
# If the image doesn't have orjson installed this will raise ImportError at
# startup, which is intentional: fail fast rather than run slowly.
import orjson

# --- Config from environment ---
# The Job Master and Kubernetes will inject these values when the container starts.
JOB_MASTER_URL   = os.environ["JOB_MASTER_URL"]
WORKER_ID        = os.environ["WORKER_ID"]        # e.g. "mapper_3"
JOB_ID           = os.environ["JOB_ID"]
MAP_ID           = int(os.environ["MAP_ID"])
OFFSET_START     = int(os.environ["OFFSET_START"])
OFFSET_END       = int(os.environ["OFFSET_END"])
NUM_REDUCERS     = int(os.environ["NUM_REDUCERS"])
INPUT_PATH       = os.environ["INPUT_PATH"]       # MinIO object path
CODE_PATH        = os.environ["CODE_PATH"]        # MinIO path to user's .py file

# MinIO Connection details
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET     = os.environ["MINIO_BUCKET"]
PING_INTERVAL    = int(os.environ.get("PING_INTERVAL", "10"))

# Initialize our connection to the storage server
minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)


# ── Persistent HTTP client (Optimisation 3.2) ────────────────────────────────
# A single client is instantiated once and reused for every ping, eliminating
# repeated TCP handshake overhead.  Closed explicitly in the finally block.
import httpx
_http_client: httpx.AsyncClient | None = None

async def get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient()
    return _http_client


# ── Heartbeat ────────────────────────────────────────────────────────────────

async def ping(status: str):
    """Send a status update to the Job Master."""
    client = await get_http_client()
    try:
        await client.post(
            f"{JOB_MASTER_URL}/worker_ping",
            json={"worker_id": WORKER_ID, "worker_type": "mapper", "status": status},
            timeout=5,
        )
    except Exception as e:
        # If the ping fails (e.g., brief network hiccup), don't crash.
        print(f"Ping failed: {e}")

async def ping_loop():
    """Runs continuously in the background, pinging every PING_INTERVAL seconds."""
    while True:
        await asyncio.sleep(PING_INTERVAL)
        await ping("alive")


# ── User code loading ────────────────────────────────────────────────────────

def load_user_map_function(code_path: str):
    """Download user code from MinIO and import the map() function."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
        minio_client.fget_object(MINIO_BUCKET, code_path, f.name)
        spec   = importlib.util.spec_from_file_location("user_code", f.name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.map  # User must define a function named 'map'


# ── Partitioning ─────────────────────────────────────────────────────────────

def partition_key(key: str) -> int:
    """Determines which reducer should get this key by hashing it."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_REDUCERS


# ── Main ─────────────────────────────────────────────────────────────────────

async def run():
    await ping("started")
    ping_task = asyncio.create_task(ping_loop())

    # All temp paths tracked here so the finally block can clean up even on crash.
    tmp_input_path    = None
    partition_handles = {}  # reducer_id -> open file handle (text write mode)
    partition_paths   = {}  # reducer_id -> local disk path

    try:
        # ── Step 1: Load user map function ───────────────────────────────────
        user_map = load_user_map_function(CODE_PATH)

        # ── Step 2: Download input file to local disk ─────────────────────────
        # We download the whole file once and then read only our assigned byte
        # range one line at a time.  The file lives on disk, not in RAM.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".input") as f:
            minio_client.fget_object(MINIO_BUCKET, INPUT_PATH, f.name)
            tmp_input_path = f.name

        # ── Step 3: Open one JSONL partition file per reducer (Opt 1.2) ───────
        # Instead of building a dict-of-lists in RAM, each reducer partition
        # gets its own temp file on the local disk.  Each line in the file is
        # a single JSON-encoded [key, value] array (JSON Lines format), which
        # lets the reducer stream without loading everything into RAM at once.
        for reducer_id in range(NUM_REDUCERS):
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
            )
            partition_handles[reducer_id] = tmp
            partition_paths[reducer_id]   = tmp.name

        # ── Step 4: Stream byte range line-by-line (Opt 1.1) ─────────────────
        # f.seek(OFFSET_START) positions the read head.  Python's file iterator
        # then loads exactly one raw line per loop iteration — never the whole
        # chunk.  We stop as soon as we have consumed OFFSET_END-OFFSET_START
        # bytes so we don't stray into the next mapper's territory.
        is_first_line  = True
        bytes_consumed = 0
        chunk_size     = OFFSET_END - OFFSET_START

        with open(tmp_input_path, "rb") as f:
            f.seek(OFFSET_START)

            for raw_line in f:
                bytes_consumed += len(raw_line)

                # If our chunk starts mid-file the very first raw_line is
                # almost certainly a partial line cut by the byte-offset split.
                # We skip it — the previous mapper owns that line in full.
                if is_first_line:
                    is_first_line = False
                    if OFFSET_START > 0:
                        # Check we haven't already passed OFFSET_END on this
                        # one line (edge case: chunk smaller than one line).
                        if bytes_consumed <= chunk_size:
                            continue

                line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")

                if line.strip():
                    # Run the user's map function on this single decoded line.
                    for key, value in user_map(str(OFFSET_START + bytes_consumed), line):
                        r_id = partition_key(key)
                        # orjson.dumps() returns bytes; decode to str for the
                        # text-mode file handle.  The [key, value] array format
                        # is identical to the original code so it stays compatible.
                        partition_handles[r_id].write(
                            orjson.dumps([key, str(value)]).decode() + "\n"
                        )

                # Stop once we have read our full assigned chunk.
                if bytes_consumed >= chunk_size:
                    break

        # Flush and close all handles before uploading.
        # MinIO's put_object needs a clean, seekable file descriptor.
        for fh in partition_handles.values():
            fh.close()
        partition_handles = {}  # Mark as closed so finally block skips them

        # ── Step 5: Upload each partition to MinIO (Opt 1.3) ─────────────────
        # CHANGED path format — grouped by destination reducer, not source mapper:
        #
        #   intermediate/{JOB_ID}/reducer_{r_id}/from_mapper_{MAP_ID}.jsonl
        #
        # This means the reducer can call list_objects() on its own prefix
        # folder and get exactly the files it needs — no blind per-mapper loop.
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

        ping_task.cancel()
        await ping("completed")
        print("Mapper completed successfully.")

    except Exception as e:
        ping_task.cancel()
        await ping("failed")
        raise e

    finally:
        # ── Cleanup ───────────────────────────────────────────────────────────
        if tmp_input_path and os.path.exists(tmp_input_path):
            os.unlink(tmp_input_path)

        # Close any handles still open (only if we crashed before the explicit
        # close block above ran).
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

        # Close the persistent HTTP client gracefully.
        if _http_client and not _http_client.is_closed:
            await _http_client.aclose()


if __name__ == "__main__":
    asyncio.run(run())