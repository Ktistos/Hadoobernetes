FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY mapper.py reducer.py ./
# No CMD — command is passed by the Kubernetes Job spec