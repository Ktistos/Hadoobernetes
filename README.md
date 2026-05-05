## CLI/CLUSTER MANAGER (WINDOWS VERSION)

Created the core directories, added the necessary deployment files, and updated the init script to build everything locally. It hasn't been fully stress-tested yet, but the end-to-end flow seems to work so far!

### 1. What was added/fixed
* **`cluster_manager/`**: Created the FastAPI app to handle API requests and spawn Kubernetes jobs.
* **`cli/`**: Created the `hadoob` CLI for staging files and submitting jobs.
* **K8s Manifests**: Added deployment, service, and ingress files for the Cluster Manager. 
* **`minikube-init.sh`**: Updated to automatically build the `cluster-manager` image directly into Minikube's internal registry.

### 2. How to set it up from scratch
Wipe the old cluster and run the updated automated script:
```bash
minikube delete
bash init_scripts/minikube-init.sh
kubectl apply -f ./deployment/k8s_resources/ -R
```

Open your network tunnel in a separate terminal tab:
```bash
sudo kubectl --kubeconfig ~/.kube/config port-forward --namespace=ingress-nginx service/ingress-nginx-controller 80:80 --address 0.0.0.0
```

### 3. How to test it
Activate your CLI environment and run the commands:
```bash
# Activate and set environment variables
source cli/.venv/bin/activate
export CLUSTER_MANAGER_URL="http://api.minikube.local"
export KEYCLOAK_URL="http://kc.minikube.local"
export MINIO_URL="minio.minikube.local:80"

# Run the automated unit tests
pytest cli/tests/
pytest cluster_manager/tests/

# Or test the CLI manually
hadoob login --username testuser --password test
echo "dummy data" > input.txt
echo "print('hello')" > job.py
hadoob submit --mappers 2 --reducers 1 --input-file ./input.txt --code ./job.py
```

### 4. WIP, not fully tested out 