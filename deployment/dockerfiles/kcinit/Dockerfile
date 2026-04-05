FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir python-keycloak
COPY init_keycloak.py .
CMD ["python", "init_keycloak.py"]
