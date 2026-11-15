SHELL := /bin/sh
export PATH := $(CURDIR)/.local/bin:$(PATH)
export KUBECONFIG := $(CURDIR)/.local/kubeconfig
export UV_CACHE_DIR ?= $(CURDIR)/.local/uv-cache
export MPLCONFIGDIR ?= $(CURDIR)/.local/matplotlib
.PHONY: setup clean-cluster overview health predict grafana prometheus check check-live rollout balance autoscale figures report
setup:
	sh scripts/setup.sh
clean-cluster:
	kind delete cluster --name mlops-hw11 --kubeconfig "$(KUBECONFIG)"
overview:
	kubectl -n hw11 get deployment,pods,svc,hpa -o wide
	kubectl -n hw11 top pod -l app=hw11-api
health:
	kubectl -n hw11 exec hw11-client-0 -- python -c 'import urllib.request; print(urllib.request.urlopen("http://hw11-plain:8080/ready").read().decode())'
predict:
	kubectl -n hw11 exec hw11-client-0 -- python /app/load.py http://hw11-plain:8080/generate --single '{"request_id":"demo","payload":{"prompt":"Как взять первую строку pandas?","max_new_tokens":4}}'
grafana:
	kubectl -n hw11 port-forward --address 127.0.0.1 svc/hw11-grafana 3000:3000
prometheus:
	kubectl -n hw11 port-forward --address 127.0.0.1 svc/hw11-prometheus 9090:9090
rollout:
	python3 scripts/rollout.py
balance:
	python3 scripts/balance.py
	python3 scripts/metrics_compare.py
autoscale:
	python3 scripts/autoscale.py
check:
	python3 scripts/check.py
check-live:
	python3 scripts/check.py --live
figures:
	python3 scripts/analyze.py
	uv run --frozen python scripts/figures.py
report: figures
	python3 scripts/report.py
