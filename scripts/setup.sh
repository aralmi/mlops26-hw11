#!/bin/sh
set -eu
cluster=mlops-hw11
node=kindest/node:v1.34.0@sha256:7416a61b42b1662ca6ca89f02028ac133a309a2a30ba309614e8ec94d976dc5a
mkdir -p .local
docker info --format 'CPU={{.NCPU}} RAM_bytes={{.MemTotal}} Docker={{.ServerVersion}}'
df -h .
if ! kind get clusters | grep -qx "$cluster"; then
    kind create cluster --name "$cluster" --kubeconfig "$KUBECONFIG" --image "$node" --wait 120s
else
    kind export kubeconfig --name "$cluster" --kubeconfig "$KUBECONFIG"
fi
kubectl create namespace hw11 --dry-run=client -o yaml | kubectl apply -f -
docker build -t hw11-api:1.0.0 .
kind load docker-image --name "$cluster" hw11-api:1.0.0
while read -r tag digest; do
    if ! docker image inspect "$tag" --format '{{range .RepoDigests}}{{println .}}{{end}}' 2>/dev/null | grep -Fxq "$digest"; then
        docker pull "$digest"
        docker tag "$digest" "$tag"
    fi
    kind load docker-image --name "$cluster" "$tag"
done < monitoring/images.lock
helm lint chart
helm upgrade --install hw11 chart --namespace hw11 --reset-values --wait --timeout 110s
kubectl apply -f monitoring/clients.yaml
kubectl wait --namespace hw11 pod -l app=hw11-client --for=condition=Ready --timeout=80s
sh scripts/monitoring.sh
