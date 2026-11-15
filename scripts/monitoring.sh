#!/bin/sh
set -eu
k() { kubectl --namespace hw11 "$@"; }
config() { k create configmap "$1" --from-file="$2" --dry-run=client -o yaml | k apply -f -; }
k create configmap hw11-prom-config --from-file=monitoring/prometheus.yml --from-file=monitoring/rules.yml --dry-run=client -o yaml | k apply -f -
config hw11-grafana-datasource monitoring/grafana-datasource.yaml
config hw11-grafana-provider monitoring/grafana-provider.yaml
config hw11-dashboard monitoring/dashboard.json
k apply -f monitoring/stack.yaml
kubectl apply -k monitoring
kubectl rollout status deployment/metrics-server -n kube-system --timeout=150s
k rollout status deployment/hw11-prometheus --timeout=120s
k rollout status deployment/hw11-grafana --timeout=120s
