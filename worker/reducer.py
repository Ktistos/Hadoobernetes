import os, asyncio, importlib.util, tempfile, sqlite3
from minio import Minio

# orjson is faster than stdlib json for both serialisation and deserialisation.
# Returns bytes from dumps() — handled explicitly below.
import orjson

# --- Config from environment ---
# Injected by the Job Master and Kubernetes
JOB_MASTER_URL   = os.environ["JOB_MASTER_URL"]
WORKER_ID        = os.environ["WORKER_ID"]        # e.g. "reducer_0"
JOB_ID           = os.environ["JOB_ID"]
REDUCER_ID       = int(os.environ["REDUCER_ID"])
NUM_MAPPERS      = int(os.environ["NUM_MAPPERS"])  # kept for reference / logging
CODE_PATH        = os.environ["CODE_PATH"]
OUTPUT_PATH      = os.environ["OUTPUT_PATH"]       # final output object path in MinIO

# MinIO Connection details
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET     = os.environ["MINIO_BUCKET"]
PING_INTERVAL    = int(os.environ.get("PING_INTERVAL", "10"))

# Initialize MinIO client
minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)


# ── Persistent HTTP client (Optimisation 3.2) ────────────────────────────────
# One client for the lifetime of this pod; reused by every ping call.
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
            json={"worker_id": WORKER_ID, "worker_type": "reducer", "status": status},
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

def load_user_reduce_function(code_path: str):
    """Download user code from MinIO and import the reduce() function."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
        minio_client.fget_object(MINIO_BUCKET, code_path, f.name)
        spec   = importlib.util.spec_from_file_location("user_code", f.name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.reduce  # User must define a function named 'reduce'


# ── Main ─────────────────────────────────────────────────────────────────────

async def run():
    await ping("started")
    ping_task = asyncio.create_task(ping_loop())

    db_path         = None   # path of the SQLite temp file
    tmp_output_path = None   # path of the local output JSONL before upload

    try:
        user_reduce = load_user_reduce_function(CODE_PATH)

        # ── Phase A: SQLite setup (Opt 2.2) ──────────────────────────────────
        # We use a temp file (not ":memory:") so SQLite can spill to disk when
        # the dataset is larger than RAM.  The PRAGMAs trade durability for
        # speed — fine here because this data is throwaway intermediate state.
        db_fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(db_fd)  # mkstemp opens the fd; sqlite3.connect opens its own

        conn   = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.executescript("""
            PRAGMA synchronous  = OFF;
            PRAGMA journal_mode = MEMORY;
            CREATE TABLE map_data (key TEXT NOT NULL, value TEXT NOT NULL);
        """)
        conn.commit()

        # ── Phase B: Shuffle — list_objects + stream to SQLite (Opt 2.1 + 3.3) ─
        # Instead of looping blindly over every mapper_id, we ask MinIO which
        # partition files actually exist in OUR reducer's prefix folder.
        # This eliminates 404-swallowing and is correct even when a mapper
        # produced zero pairs for this reducer (it simply won't create a file).
        prefix  = f"intermediate/{JOB_ID}/reducer_{REDUCER_ID}/"
        objects = minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)

        total_rows = 0
        for obj in objects:
            # Direct network-to-database streaming (Opt 3.3):
            # get_object() returns an urllib3 HTTPResponse whose readline()
            # reads from the network socket directly — no temp file on disk.
            response = None
            try:
                response = minio_client.get_object(MINIO_BUCKET, obj.object_name)

                batch = []
                BATCH_SIZE = 5000  # insert in batches for speed

                for raw_line in response:
                    raw_line = raw_line.rstrip(b"\n")
                    if not raw_line:
                        continue

                    # Each line is a JSON array [key, value] written by the mapper.
                    pair = orjson.loads(raw_line)
                    batch.append((pair[0], pair[1]))

                    if len(batch) >= BATCH_SIZE:
                        cursor.executemany(
                            "INSERT INTO map_data (key, value) VALUES (?, ?)", batch
                        )
                        total_rows += len(batch)
                        batch.clear()

                # Insert any remaining rows in the last partial batch.
                if batch:
                    cursor.executemany(
                        "INSERT INTO map_data (key, value) VALUES (?, ?)", batch
                    )
                    total_rows += len(batch)

            finally:
                # Always release the network connection back to the pool —
                # not doing this leaks the urllib3 socket.
                if response:
                    response.close()
                    response.release_conn()

        conn.commit()
        print(f"Shuffle complete: {total_rows} rows ingested from {prefix}")

        # ── Phase C: Index + Sort (Opt 2.2 continued) ────────────────────────
        # The index lets SQLite use a B-tree scan instead of a full sort pass,
        # which is faster and uses O(log n) memory rather than O(n).
        cursor.execute("CREATE INDEX idx_key ON map_data (key)")
        conn.commit()

        # ── Phase D: Streaming Reduce (Opt 2.3) ──────────────────────────────
        # We write results line-by-line to a local JSONL temp file rather than
        # accumulating a results list in RAM.  The cursor iterates one SQLite
        # row at a time so the DB handles all memory management.
        tmp_out_fd, tmp_output_path = tempfile.mkstemp(suffix=".jsonl")
        os.close(tmp_out_fd)

        current_key    = None
        current_values = []
        result_count   = 0

        # SELECT with ORDER BY key uses our index to deliver rows already sorted.
        sort_cursor = conn.cursor()
        sort_cursor.execute("SELECT key, value FROM map_data ORDER BY key")

        with open(tmp_output_path, "wb") as out_fh:
            for db_key, db_value in sort_cursor:
                if db_key != current_key:
                    # Key changed — flush the accumulated values for the old key.
                    if current_key is not None:
                        for out_key, out_value in user_reduce(current_key, current_values):
                            # orjson.dumps returns bytes — write directly to the
                            # binary file handle (no encode() call needed).
                            out_fh.write(
                                orjson.dumps([out_key, str(out_value)]) + b"\n"
                            )
                            result_count += 1
                    current_key    = db_key
                    current_values = [db_value]
                else:
                    current_values.append(db_value)

            # Flush the very last key group after the loop ends.
            if current_key is not None:
                for out_key, out_value in user_reduce(current_key, current_values):
                    out_fh.write(
                        orjson.dumps([out_key, str(out_value)]) + b"\n"
                    )
                    result_count += 1

        conn.close()
        print(f"Reduce complete: {result_count} output pairs written.")

        # ── Phase E: Upload final output to MinIO ─────────────────────────────
        # Stream the local output file directly to MinIO — no in-memory buffer.
        output_size = os.path.getsize(tmp_output_path)
        with open(tmp_output_path, "rb") as fh:
            minio_client.put_object(
                MINIO_BUCKET, OUTPUT_PATH,
                fh, output_size,
                content_type="application/jsonl",
            )

        ping_task.cancel()
        await ping("completed")
        print("Reducer completed successfully.")

    except Exception as e:
        ping_task.cancel()
        await ping("failed")
        raise e

    finally:
        # ── Cleanup ───────────────────────────────────────────────────────────
        if db_path and os.path.exists(db_path):
            os.unlink(db_path)

        if tmp_output_path and os.path.exists(tmp_output_path):
            os.unlink(tmp_output_path)

        # Close the persistent HTTP client gracefully.
        if _http_client and not _http_client.is_closed:
            await _http_client.aclose()


if __name__ == "__main__":
    asyncio.run(run())