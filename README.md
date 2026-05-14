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

### 5. Mapper/reducer intermediate data flow
Current worker behavior on `windows_support`:

* **Mapper input**: each mapper reads `INPUT_PATH` from MinIO with buffered range reads. The current buffer size is `64 MiB`.
* **Chunk ownership**:
  * if `OFFSET_START` lands in the middle of a line, the mapper skips that partial line
  * if a line starts before `OFFSET_END`, that mapper owns the whole line even if the line ends after `OFFSET_END`
* **Intermediate format**: mapper-emitted pairs are stored as `JSONL`, one `[key, value]` array per line.
* **Intermediate object layout**: for each streamed input batch, the mapper uploads one shard object per reducer that received data:

```text
intermediate/{JOB_ID}/reducer_{REDUCER_ID}/from_mapper_{MAP_ID}_chunk_{BATCH_INDEX}.jsonl
```

* **Reducer input**: each reducer lists its own `intermediate/{JOB_ID}/reducer_{REDUCER_ID}/` prefix in MinIO and ingests every shard object it finds there.

This means reducers are compatible with multiple mapper shard files per reducer directory, and mappers do not need one long-lived local output file per reducer.
