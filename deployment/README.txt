================================================================================
  HADOOBERNETES - DEPLOYMENT
================================================================================

This directory contains everything needed to deploy and test the Hadoobernetes
stack on a local Minikube cluster.

--------------------------------------------------------------------------------
  DIRECTORY STRUCTURE
--------------------------------------------------------------------------------

deployment/
|
|-- dockerfiles/              Custom Docker images built and pushed to DockerHub.
|   |                         These are used by Kubernetes init jobs.
|   |
|   |-- dbinit/               Image: ktistos/postgres-init:latest
|   |   |-- Dockerfile        Extends postgres:16. Bundles init.sql into /sql/.
|   |   |-- init.sql          Creates the mapreduce schema, tables, and enums.
|   |
|   |-- kcinit/               Image: ktistos/kcinit:latest
|       |-- Dockerfile        Python 3.12 slim image. Runs init_keycloak.py on start.
|       |-- init_keycloak.py  Creates the hadoobernetes realm, mapreduce-client,
|                             and testuser in Keycloak via the admin API.
|
|-- k8s_resources/            All Kubernetes manifests. Applied recursively with:
|   |                           kubectl apply -f ./k8s_resources/ -R
|   |
|   |-- namespaces/           Namespace definitions.
|   |   |-- mapreduce-namespace.yaml
|   |
|   |-- Deployments/          Kubernetes Deployments.
|   |   |-- keycloak-deployment.yaml
|   |
|   |-- StatefulSets/         Kubernetes StatefulSets.
|   |   |-- postgres-statefulset.yaml
|   |
|   |-- Services/             ClusterIP Services for internal communication.
|   |   |-- keycloak-service.yaml
|   |   |-- postgres-service.yaml
|   |
|   |-- ingresses/            Nginx Ingress rules for external access.
|   |   |-- keycloak-ingress.yaml     -> kc.minikube.local
|   |   |-- minio-ingress.yaml        -> minio.minikube.local (S3 API)
|   |                                 -> minio-console.minikube.local (UI)
|   |
|   |-- Tenants/              MinIO Tenant CR (managed by the MinIO Operator).
|   |   |-- minio-tenant.yaml         Defines the minio-tenant namespace and
|   |                                 a single-pool MinIO tenant.
|   |
|   |-- initJobs/             One-shot Kubernetes Jobs that run after services
|   |   |                     are up. Each uses an init container to wait for
|   |   |                     its target service before running.
|   |   |-- dbInitJob.yaml    Runs init.sql against PostgreSQL.
|   |   |-- kcInitJob.yaml    Runs init_keycloak.py to configure Keycloak.
|   |   |-- minioInitJob.yaml Creates the mapreduce bucket, disables anonymous
|   |                         access, and seeds placeholder prefixes.
|   |
|   |-- secrets/              Kubernetes Secrets (plaintext for local dev only).
|       |-- postgres-secret.yaml
|       |-- keycloak-secret.yaml
|       |-- minio-secret.yaml
|
|-- setup/                    Python environment setup.
|   |-- requirements.txt      Python dependencies for the test scripts.
|   |-- setup_venv.sh         Creates .venv at the repo root and installs deps.
|
|-- tests/                    Smoke tests for each service.
    |-- test_postgres.sh      Verifies schemas, tables, and enums in PostgreSQL.
    |-- test_keycloak.sh      Verifies realm, client, token flow, and refresh.
    |-- test_minio.sh         Verifies bucket init, authorized access, and
    |                         authorization rejection.
    |-- lib/
        |-- test_postgres.py
        |-- test_keycloak.py
        |-- test_minio.py

--------------------------------------------------------------------------------
  PREREQUISITES
--------------------------------------------------------------------------------

The following must be installed on your machine (or WSL on Windows):
  - Docker
  - Minikube
  - kubectl
  - Python 3.12+
  - git

Windows users: see init_scripts/configure-hosts.bat at the repo root.

--------------------------------------------------------------------------------
  MINIKUBE SETUP  (run once per machine)
--------------------------------------------------------------------------------

From the repo root, run the init script to start Minikube and install all
cluster-level dependencies (ingress, ingress-dns, metrics-server, MinIO operator):

  bash init_scripts/minikube-init.sh

Then add the Minikube host entries to your /etc/hosts (Linux/WSL):

  echo "$(minikube ip) kc.minikube.local minio.minikube.local minio-console.minikube.local" | sudo tee -a /etc/hosts

Windows users: run init_scripts/configure-hosts.bat as Administrator instead.

--------------------------------------------------------------------------------
  DEPLOYING THE STACK
--------------------------------------------------------------------------------

From the repo root:

  kubectl apply -f ./deployment/k8s_resources/ -R

This creates all namespaces, secrets, services, deployments, and init jobs in
the correct order. The init jobs use init containers to wait for their
dependencies before running, so no manual sequencing is needed.

To tear down and start fresh (including persistent volumes):

  kubectl delete namespace mapreduce
  kubectl delete namespace minio-tenant
  kubectl delete pvc postgres-data-postgres-0 -n mapreduce 2>/dev/null || true

--------------------------------------------------------------------------------
  PYTHON ENVIRONMENT SETUP
--------------------------------------------------------------------------------

The test scripts require a Python virtual environment at .venv/ in the repo root.
Set it up once with:

  bash deployment/setup/setup_venv.sh

This creates the venv and installs all dependencies from setup/requirements.txt.
Current dependencies: python-keycloak, psycopg2-binary, minio.

--------------------------------------------------------------------------------
  RUNNING THE TESTS
--------------------------------------------------------------------------------

Each test script connects to the live services over the ingress hostnames.
The PostgreSQL test additionally requires a port-forward (handled automatically
by the script).

  bash deployment/tests/test_postgres.sh
  bash deployment/tests/test_keycloak.sh
  bash deployment/tests/test_minio.sh

What each test verifies:

  test_postgres   Connects to PostgreSQL and checks that the mapreduce and
                  keycloak schemas, all expected tables, and enums exist.

  test_keycloak   Logs in as testuser, validates the token via userinfo,
                  decodes claims, and verifies token refresh.

  test_minio      Checks bucket and placeholder initialization, verifies
                  anonymous access is denied, creates a scoped user derived
                  from the Keycloak subject ID, tests authorized upload/
                  download/delete, and confirms access to other prefixes
                  and system paths is rejected.

--------------------------------------------------------------------------------
  DOCKERFILES
--------------------------------------------------------------------------------

The images in dockerfiles/ are pre-built and pushed to DockerHub. You only
need to rebuild and push them if you change their contents.

  dbinit  (ktistos/postgres-init:latest)
  --------
  Extends the official postgres:16 image. Copies init.sql into /sql/init.sql.
  The dbInitJob runs psql against this file to initialize the mapreduce schema.
  The imagePullPolicy is set to Always so the latest image is always pulled
  on each deployment.

  Rebuild and push:
    cd deployment/dockerfiles/dbinit
    docker build -t ktistos/postgres-init:latest .
    docker push ktistos/postgres-init:latest

  kcinit  (ktistos/kcinit:latest)
  --------
  Python 3.12 slim image. Installs python-keycloak and runs init_keycloak.py
  on container start. The script connects to Keycloak with the bootstrap admin
  credentials (injected via secrets) and creates the hadoobernetes realm,
  the mapreduce-client, and the testuser account.

  Rebuild and push:
    cd deployment/dockerfiles/kcinit
    docker build -t ktistos/kcinit:latest .
    docker push ktistos/kcinit:latest

================================================================================
