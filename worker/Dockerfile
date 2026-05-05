#FROM python:3.11-slim

# ── System dependencies ───────────────────────────────────────────────────────
# orjson is a C extension; it compiles cleanly on slim but needs gcc for the
# build wheel.  Using --no-cache-dir keeps the image small.

#RUN apt-get update \
#    && apt-get install -y --no-install-recommends gcc \
#    && rm -rf /var/lib/apt/lists/*

#WORKDIR /app

# ── Python dependencies ───────────────────────────────────────────────────────
# Copy requirements first so this layer is cached unless deps change.

#COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt

# ── Application source ────────────────────────────────────────────────────────
# Both mapper and reducer live in the same image.
# The Kubernetes Job spec selects which one to run via the `command` field:
#   command: ["python", "mapper.py"]   or   command: ["python", "reducer.py"]

#COPY mapper.py reducer.py ./

# ── Runtime ───────────────────────────────────────────────────────────────────
# No CMD here — the Kubernetes Job manifest supplies the command.
# Running as non-root is good practice even for ephemeral pods.

#RUN useradd -m workeruser
#USER workeruser
FROM python:3.12-slim

# Install the required libraries (including sqlite3 which is built into python)
RUN pip install minio httpx orjson

# Copy your worker scripts into the image
COPY . /app/worker/

WORKDIR /app