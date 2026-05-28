#!/bin/bash
set -e

echo "==> Starting Minikube..."
minikube start --driver=docker --cpus=4 --memory=6144

echo ""
echo "==> Enabling addons..."
minikube addons enable ingress
minikube addons enable ingress-dns
minikube addons enable metrics-server

echo ""
echo "==> Waiting for ingress controller to be ready..."
kubectl rollout status deployment/ingress-nginx-controller -n ingress-nginx --timeout=120s

echo ""
echo "==> Installing MinIO Operator..."
kubectl apply -k github.com/minio/operator

echo ""
echo "==> Scaling MinIO Operator to 1 replica (single-node minikube workaround)..."
kubectl rollout status deployment/minio-operator -n minio-operator --timeout=60s || true
kubectl scale deployment minio-operator -n minio-operator --replicas=1
kubectl rollout status deployment/minio-operator -n minio-operator --timeout=60s

# new, for the cluster manager service (custom service, not publicly available).
echo ""
echo "==> Pointing Docker to Minikube's internal registry..."
eval $(minikube docker-env)

echo ""
echo "==> Building the Cluster Manager image locally..."
if [ -d "./cluster_manager" ]; then
    docker build -t hadoobernetes/cluster-manager:latest ./cluster_manager
    echo "    Image built successfully!"
else
    echo "--------------------------------------------------"
    echo " ERROR: ./cluster_manager directory not found!"
    echo " Please ensure you are running this script from the root of the repository:"
    echo " bash init_scripts/minikube-init.sh"
    echo "--------------------------------------------------"
    exit 1
fi

echo ""
echo "Minikube is ready. Deploy the stack with:"
echo "  kubectl apply -f ./deployment/k8s_resources/ -R"
