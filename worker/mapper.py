import os, json, hashlib, asyncio, httpx, importlib.util, tempfile
from minio import Minio

# --- Config from environment ---
# The Job Master and Kubernetes will inject these values when the container starts.
JOB_MASTER_URL = os.environ["JOB_MASTER_URL"]
WORKER_ID = os.environ["WORKER_ID"]        # e.g. "mapper_3"
JOB_ID = os.environ["JOB_ID"]
MAP_ID = int(os.environ["MAP_ID"])
OFFSET_START = int(os.environ["OFFSET_START"])
OFFSET_END = int(os.environ["OFFSET_END"])
NUM_REDUCERS = int(os.environ["NUM_REDUCERS"])
INPUT_PATH = os.environ["INPUT_PATH"]      # MinIO object path
CODE_PATH = os.environ["CODE_PATH"]        # MinIO path to user's .py file

# MinIO Connection details
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
PING_INTERVAL = int(os.environ.get("PING_INTERVAL", "10"))

# Initialize our connection to the storage server
minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

# Send a status update to the Job Master
async def ping(status: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{JOB_MASTER_URL}/worker_ping", json={
                "worker_id": WORKER_ID,
                "worker_type": "mapper",
                "status": status
            }, timeout=5)
        except Exception as e:
            # If the ping fails (e.g., brief network hiccup), we don't want the worker to crash.
            print(f"Ping failed: {e}")  # Don't crash on ping failure

# Runs continuously in the background, pinging every 10 seconds
async def ping_loop():
    while True:
        await asyncio.sleep(PING_INTERVAL)
        await ping("alive")

# Download user code from MinIO and load the map() function into memory.
def load_user_map_function(code_path: str):
    """Download user code from MinIO and import the map() function."""
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
        minio_client.fget_object(MINIO_BUCKET, code_path, f.name)
        spec = importlib.util.spec_from_file_location("user_code", f.name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.map  # User must define a function named 'map'

# Determines which reducer should get this key by hashing it.
def partition_key(key: str) -> int:
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_REDUCERS

async def run():
    await ping("started")
    # Start the heartbeat running in the background
    ping_task = asyncio.create_task(ping_loop())

    try:
        # 1. Download and load user map function
        user_map = load_user_map_function(CODE_PATH)

        # 2. Download input data chunk
        with tempfile.NamedTemporaryFile(delete=False) as f:
            minio_client.fget_object(MINIO_BUCKET, INPUT_PATH, f.name)
            input_file = f.name

        # 3. Read assigned byte range
        with open(input_file, "rb") as f:
            f.seek(OFFSET_START)
            data = f.read(OFFSET_END - OFFSET_START).decode("utf-8", errors="replace")

        # 4. Run map function — emit (key, value) pairs
        # CAUTION: Handle line boundaries! A byte offset may split a line.
        # Simple fix: skip first partial line if not at start of file.
        lines = data.splitlines()
        if OFFSET_START > 0:
            lines = lines[1:]  # First line may be partial

        # Create empty buckets for each Reducer
        partitions = {i: [] for i in range(NUM_REDUCERS)}
        for line_num, line in enumerate(lines):
            if not line.strip():
                continue
            # Run the user's code. It returns key-value pairs
            for key, value in user_map(str(line_num), line):
                # Put the pair in the correct Reducer's bucket based on the hash
                partitions[partition_key(key)].append((key, value))

        # 5. Upload partitions to MinIO
        for reducer_id, pairs in partitions.items():
            content = json.dumps(pairs).encode("utf-8")
            path = f"intermediate/{JOB_ID}/mapper_{MAP_ID}/partition_{reducer_id}.json"
            import io
            minio_client.put_object(
                MINIO_BUCKET, path,
                io.BytesIO(content), len(content),
                content_type="application/json"
            )

        # We are done! Stop the heartbeat and send a final success message.
        ping_task.cancel()
        await ping("completed")
        print("Mapper completed successfully.")

    except Exception as e:
        # If anything crashes, stop the heartbeat and tell the Job Master we failed.
        ping_task.cancel()
        await ping("failed")
        raise e

# This tells Python to actually run the async code when the file is executed.
if __name__ == "__main__":
    asyncio.run(run())