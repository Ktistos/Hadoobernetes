# Hadoobernetes - Windows Setup and Usage Guide

## 1. Setup 

Ensure that:
- Docker Desktop (with WSL2 backend) 
- WSL2
- Minikube (Inside WSL)

are installed. Wipe any existing cluster and run the initialization script from the repository root:

```bash
minikube delete
 ```

```bash
bash init_scripts/minikube-init.sh
```
```bash
kubectl apply -f ./deployment/k8s_resources/ -R
```
Open your network tunnel in a separate terminal tab to route ingress traffic:

```bash
sudo kubectl --kubeconfig ~/.kube/config port-forward --namespace=ingress-nginx service/ingress-nginx-controller 80:80 --address 0.0.0.0
```

Activate the CLI environment and set the required environment variables:

```bash
cd cli
pip install -e .
export CLUSTER_MANAGER_URL="http://api.minikube.local"
export KEYCLOAK_URL="http://kc.minikube.local"
export MINIO_URL="minio.minikube.local:80"
```

Now you must modify your hosts file to include the following ips (add the following line):
```
127.0.0.1 api.minikube.local kc.minikube.local minio.minikube.local minio-console.minikube.local
```

You can also access the minIO and Keycloak pages on the following links:

- *Keycloak*:      http://kc.minikube.local (admin/admin)
- *MinIO* : http://minio-console.minikube.local (minioadmin/minioadmin)


## 2. Login

Authenticate with the Keycloak service. You can pass credentials directly or let the CLI prompt you.

```bash
hadoob login --username testuser --password test
```

## 3. Place the Text File / Code File

The `submit` command automatically handles staging your local input data and execution code to MinIO. However, if you need to manually upload files to your isolated storage directory in MinIO, use the `upload` command:

```bash
hadoob upload ./local_dataset.txt remote_folder/dataset.txt
```

## 4. Run Job

Submit the Map-Reduce job. Provide the local paths to your input file and Python script. The CLI uploads these to MinIO automatically, calculates sizes, and dispatches the payload to the Cluster Manager.

```bash
hadoob submit --mappers 2 --reducers 1 --input-file ./input.txt --code ./job.py
```
The system will output a Job ID upon successful submission.

## 5. See Status

Fetch the real-time execution status of the Map-Reduce job using the returned Job ID.
```bash
hadoob status <job_id>
```
If you need to forcefully terminate an active job:
```bash
hadoob abort <job_id>
```
## 6. Access Output Files

Once the job status reflects completion, download the result files from MinIO to your local machine.

```bash
hadoob download <remote_path> ./local_output.txt
```

You may also see output files in the minIO url shown above in 1. Setup

Anything comes up, ask your coding agent :P
