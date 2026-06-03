"""
worker/reducer.py
==================
Reducer worker pod.  Executed as a Kubernetes Job spawned by the Job Master.

Environment variables injected by worker_spawner.py
-----------------------------------------------------
Required:
  JOB_MASTER_URL    URL of the Job Master Service for this job
  WORKER_ID         e.g. "reducer_0"
  JOB_ID            UUID of the parent job
  REDUCER_ID        Integer index of this reducer
  NUM_MAPPERS       Total mapper count (kept for logging/validation)
  CODE_PATH         MinIO object path of the user's .py  (jobs.code_location)
  OUTPUT_PATH       MinIO object path for final output   (reduce_tasks.output_data_path)
  MINIO_ENDPOINT    e.g. minio-service:9000
  MINIO_ACCESS_KEY
  MINIO_SECRET_KEY
  MINIO_BUCKET

Optional:
  PING_INTERVAL     Heartbeat cadence in seconds (default 10)

Note: worker_spawner also sends INPUT_PATH and INTERMEDIATE_PREFIX.
  Neither is read by the reducer — they are harmless unused env vars.

Design-doc references
----------------------
  §3.2  Worker Ping API
  §5.4  Job Execution Workflow (reducer phase, fault-tolerance via pings)
"""

import asyncio
import importlib.util
import os
import sqlite3
import tempfile

import httpx
import orjson
from minio import Minio

# ── Environment ──────────────────────────────────────────────────────────────

JOB_MASTER_URL   = os.environ["JOB_MASTER_URL"]
WORKER_ID        = os.environ["WORKER_ID"]
JOB_ID           = os.environ["JOB_ID"]
REDUCER_ID       = int(os.environ["REDUCER_ID"])
NUM_MAPPERS      = int(os.environ["NUM_MAPPERS"])   # for logging / validation
CODE_PATH        = os.environ["CODE_PATH"]
OUTPUT_PATH      = os.environ["OUTPUT_PATH"]
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET     = os.environ["MINIO_BUCKET"]
PING_INTERVAL    = int(os.environ.get("PING_INTERVAL", "10"))
INTERMEDIATE_PREFIX = os.environ.get("INTERMEDIATE_PREFIX", f"intermediate/{JOB_ID}/")

# SQLite batch size: number of rows inserted per executemany call.
# 5000 is a good balance between memory use and insert overhead.
SQLITE_BATCH_SIZE = 5000

# ── Clients ───────────────────────────────────────────────────────────────────

minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

# Persistent HTTP client (Opt 3.2)
_http_client: httpx.AsyncClient | None = None


async def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient()
    return _http_client


# ── Heartbeat (design doc §3.2) ───────────────────────────────────────────────

async def ping(status: str) -> None:
    """Send a status update to the Job Master.  Never raises."""
    client = await _get_http_client()
    try:
        await client.post(
            f"{JOB_MASTER_URL}/worker_ping",
            json={"worker_id": WORKER_ID, "worker_type": "reducer", "status": status},
            timeout=5,
        )
    except Exception as exc:
        print(f"[reducer_{REDUCER_ID}] Ping '{status}' failed: {exc}")


async def ping_loop() -> None:
    """Background task — sends 'alive' every PING_INTERVAL seconds."""
    while True:
        await asyncio.sleep(PING_INTERVAL)
        await ping("alive")


# ── User code loading ─────────────────────────────────────────────────────────

def load_user_reduce_function(code_path: str):
    """Download user's .py from MinIO and return its reduce() function."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
        minio_client.fget_object(MINIO_BUCKET, code_path, f.name)
        spec   = importlib.util.spec_from_file_location("user_code", f.name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.reduce


# ── SQLite helpers ────────────────────────────────────────────────────────────

def _setup_sqlite(db_path: str) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """
    Open a SQLite database optimised for bulk writes on ephemeral storage.
    - synchronous=OFF   : don't fsync after every write (data is throwaway)
    - journal_mode=MEMORY : keep the rollback journal in RAM, not on disk
    - cache_size=-65536  : allow 64 MB page cache (speeds up ORDER BY scan)
    """
    # check_same_thread=False allows our asyncio background threads to use this connection
    conn   = sqlite3.connect(db_path, check_same_thread=False)
    cursor = conn.cursor()
    cursor.executescript("""
        PRAGMA synchronous   = OFF;
        PRAGMA journal_mode  = MEMORY;
        PRAGMA cache_size    = -65536;
        CREATE TABLE map_data (key TEXT NOT NULL, value TEXT NOT NULL);
    """)
    conn.commit()
    return conn, cursor


def _ingest_partition(
    cursor:   sqlite3.Cursor,
    response,                   # urllib3 HTTPResponse from minio.get_object()
) -> int:
    """
    Stream one partition file from a MinIO HTTPResponse directly into SQLite.
    Reads the network socket line-by-line (Opt 3.3), decodes with orjson,
    and batch-inserts into map_data (Opt 2.2).
    Returns the number of rows inserted.
    """
    batch      = []
    rows_added = 0

    for raw_line in response:
        raw_line = raw_line.rstrip(b"\n")
        if not raw_line:
            continue

        pair = orjson.loads(raw_line)

        batch.append((pair[0], pair[1]))

        if len(batch) >= SQLITE_BATCH_SIZE:
            cursor.executemany(
                "INSERT INTO map_data (key, value) VALUES (?, ?)", batch
            )
            rows_added += len(batch)
            batch.clear()

    if batch:
        cursor.executemany(
            "INSERT INTO map_data (key, value) VALUES (?, ?)", batch
        )
        rows_added += len(batch)

    return rows_added


def _run_reduce_phase(
    conn:        sqlite3.Connection,
    user_reduce,
    out_fh,                         # binary file handle for output JSONL
) -> int:
    """
    Index the map_data table, iterate in sorted key order, call user_reduce()
    per key group, and write output lines.  Returns total output pairs written.
    """
    conn.cursor().execute("CREATE INDEX idx_key ON map_data (key)")
    conn.commit()

    sort_cursor = conn.cursor()
    sort_cursor.execute("SELECT key, value FROM map_data ORDER BY key")

    current_key    = None
    current_values = []
    result_count   = 0

    for db_key, db_value in sort_cursor:
        if db_key != current_key:
            if current_key is not None:
                pairs = list(user_reduce(current_key, current_values))
                for out_key, out_value in pairs:
                    out_fh.write(
                        orjson.dumps([out_key, str(out_value)]) + b"\n"
                    )
                    result_count += 1
            current_key    = db_key
            current_values = [db_value]
        else:
            current_values.append(db_value)

    # Flush the last key group
    if current_key is not None:
        pairs = list(user_reduce(current_key, current_values))
        for out_key, out_value in pairs:
            out_fh.write(
                orjson.dumps([out_key, str(out_value)]) + b"\n"
            )
            result_count += 1

    return result_count


# ── Main async entry point ────────────────────────────────────────────────────

async def run() -> None:
    await ping("started")
    ping_task = asyncio.create_task(ping_loop())

    db_path         = None
    tmp_output_path = None

    try:
        # Step 1 — load user reduce function, offload synchronous MinIO download
        user_reduce = await asyncio.to_thread(load_user_reduce_function, CODE_PATH)

        # Step 2 — set up SQLite database on local ephemeral disk
        db_fd, db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(db_fd)
        conn, cursor   = _setup_sqlite(db_path)

        # Step 3 — Shuffle: list our reducer's prefix folder and stream all
        # partition files directly from MinIO into SQLite (Opt 2.1 + 3.3).
        # list_objects() returns only files that actually exist, so we never
        # 404 on mappers that had no data for this reducer.
        prefix  = f"{INTERMEDIATE_PREFIX.rstrip('/')}/reducer_{REDUCER_ID}/"
        objects = list(minio_client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True))

        print(
            f"[reducer_{REDUCER_ID}] Found {len(objects)} partition files "
            f"(expected up to {NUM_MAPPERS}) at {prefix}"
        )

        total_rows = 0
        for obj in objects:
            response = None
            try:
                # Offload the blocking network call and the heavy DB ingestion
                response = await asyncio.to_thread(minio_client.get_object, MINIO_BUCKET, obj.object_name)
                rows = await asyncio.to_thread(_ingest_partition, cursor, response)
                total_rows += rows
            finally:
                # Always release the urllib3 socket back to the pool.
                if response:
                    response.close()
                    response.release_conn()

        conn.commit()

        print(f"[reducer_{REDUCER_ID}] Shuffle complete: {total_rows} rows ingested.")

        # Step 4 — Sort + Reduce + write output JSONL
        tmp_out_fd, tmp_output_path = tempfile.mkstemp(suffix=".jsonl")
        os.close(tmp_out_fd)

        with open(tmp_output_path, "wb") as out_fh:
            # Offload the heavy sorting and reducing phase
            result_count = await asyncio.to_thread(_run_reduce_phase, conn, user_reduce, out_fh)

        conn.close()
        print(f"[reducer_{REDUCER_ID}] Reduce complete: {result_count} output pairs.")

        # Step 5 — Upload final output to MinIO
        output_size = os.path.getsize(tmp_output_path)
        with open(tmp_output_path, "rb") as fh:
            # Offload the final synchronous network upload
            await asyncio.to_thread(
                minio_client.put_object,
                MINIO_BUCKET, OUTPUT_PATH,
                fh, output_size,
                content_type="application/jsonl"
            )

        ping_task.cancel()
        await ping("completed")
        print(f"[reducer_{REDUCER_ID}] Reducer completed successfully.")

    except Exception as exc:
        ping_task.cancel()
        await ping("failed")
        raise exc

    finally:
        if db_path and os.path.exists(db_path):
            os.unlink(db_path)
        if tmp_output_path and os.path.exists(tmp_output_path):
            os.unlink(tmp_output_path)
        if _http_client and not _http_client.is_closed:
            await _http_client.aclose()


if __name__ == "__main__":
    asyncio.run(run())
