import os, json, asyncio, httpx, importlib.util, tempfile, io
from minio import Minio
from collections import defaultdict

# --- Config from environment ---
# Injected by the Job Master and Kubernetes
JOB_MASTER_URL = os.environ["JOB_MASTER_URL"]
WORKER_ID = os.environ["WORKER_ID"]     # e.g., "reducer_0"
JOB_ID = os.environ["JOB_ID"]
REDUCER_ID = int(os.environ["REDUCER_ID"])
NUM_MAPPERS = int(os.environ["NUM_MAPPERS"])    # How many mappers did the work?
CODE_PATH = os.environ["CODE_PATH"]
OUTPUT_PATH = os.environ["OUTPUT_PATH"]   # e.g. "output/{job_id}/part_{reducer_id}.json"   (where the final answer goes)

# MinIO Connection details
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]
PING_INTERVAL = int(os.environ.get("PING_INTERVAL", "10"))

# Initialize MinIO client
minio_client = Minio(MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

# Send a status update to the Job Master
async def ping(status: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{JOB_MASTER_URL}/worker_ping", json={
                "worker_id": WORKER_ID,
                "worker_type": "reducer",
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

# Download user code from MinIO and load the reduce() function.
def load_user_reduce_function(code_path: str):
    with tempfile.NamedTemporaryFile(suffix=".py", delete=False) as f:
        minio_client.fget_object(MINIO_BUCKET, code_path, f.name)
        spec = importlib.util.spec_from_file_location("user_code", f.name)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.reduce    # We assume the user defined a function named 'reduce'

# Main execution loop
async def run():
    await ping("started")
    # Start the heartbeat running in the background
    ping_task = asyncio.create_task(ping_loop())

    try:
        user_reduce = load_user_reduce_function(CODE_PATH)

        # --- Phase A: The Shuffle (Gathering Data) ---
        # Pull all mapper partitions for this reducer's ID
        # We use a defaultdict. If a key doesn't exist, it automatically creates an empty list.
        grouped = defaultdict(list)

        # We must check EVERY mapper to see if they left a partition file for us.
        for mapper_id in range(NUM_MAPPERS):
            # We look for the file specifically meant for OUR Reducer ID
            path = f"intermediate/{JOB_ID}/mapper_{mapper_id}/partition_{REDUCER_ID}.json"
            try:
                with tempfile.NamedTemporaryFile(delete=False) as f:
                    minio_client.fget_object(MINIO_BUCKET, path, f.name)
                    # Load the JSON data the mapper created
                    pairs = json.loads(open(f.name).read())
                    # Group all identical keys together. 
                    # E.g., If the key is "apple", the list becomes ["1", "1", "1"]
                    for key, value in pairs:
                        grouped[key].append(value)
            except Exception as e:
                # If a mapper didn't produce any data for us, that's okay, we just skip it.
                print(f"Warning: could not fetch partition from mapper {mapper_id}: {e}")

        # --- Phase B: The Sort & Reduce ---
        # Sort by key (the "sort" in MapReduce)
        results = []
        # MapReduce guarantees that keys are processed in alphabetical/sorted order
        for key in sorted(grouped.keys()):
            # We pass the key (e.g., "apple") and the LIST of values (e.g., ["1", "1", "1"]) 
            # to the user's custom function.
            for output_key, output_value in user_reduce(key, grouped[key]):
                results.append((output_key, output_value))

        # --- Phase C: Save Final Output ---
        # Convert our final results to JSON and upload output to MinIO
        content = json.dumps(results).encode("utf-8")
        minio_client.put_object(
            MINIO_BUCKET, OUTPUT_PATH,
            io.BytesIO(content), len(content),
            content_type="application/json"
        )

        # We are done! Stop the heartbeat and send a final success message.
        ping_task.cancel()
        await ping("completed")
        print("Reducer completed successfully.")

    except Exception as e:
        # If anything crashes, stop the heartbeat and tell the Job Master we failed.
        ping_task.cancel()
        await ping("failed")
        raise e

# Run the async code when the file is executed.
if __name__ == "__main__":
    asyncio.run(run())