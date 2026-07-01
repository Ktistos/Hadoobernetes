FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer-cached unless requirements.txt changes)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY main.py state_machine.py worker_spawner.py heartbeat_monitor.py ./

# Non-root user for security
RUN useradd -m appuser
USER appuser

# Uvicorn serves the FastAPI app on port 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info"]