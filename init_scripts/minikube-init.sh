#!/bin/bash
set -e

echo "==> Starting Minikube..."
minikube start --driver=docker --cpus=no-limit --memory=no-limit

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

echo ""
echo "Minikube is ready. Deploy the stack with:"
echo "  kubectl apply -f ./deployment/k8s_resources/ -R"
