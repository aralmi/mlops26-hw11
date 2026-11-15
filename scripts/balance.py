"""Three 120-second repetitions per policy and traffic mix."""
import argparse
import collections
import json
import time

from cluster import RESULTS, cmd, helm, jsonfile, parallel_load, pod_rows, query, raw, wait_ready

METRICS = ("hw11_ttfc_seconds_bucket", "hw11_duration_seconds_bucket", "hw11_requests_total",
           "hw11_prefix_total")


def snapshot():
    return {metric: query(metric) for metric in METRICS}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=float, default=7.5)
    args = parser.parse_args()
    root = RESULTS / "balance"
    helm("-f", "configs/balance.yaml", wait=True)
    jsonfile(root / "network.json", {"services": raw("get", "svc", "hw11-plain", "hw11-sticky"),
             "clients": raw("get", "pods", "-l", "app=hw11-client"),
             "kube_proxy": cmd("-n", "kube-system", "get", "configmap", "kube-proxy", "-o", "yaml")})
    mixes = [("plain", "uniform"), ("sticky", "uniform"), ("sticky", "skew"), ("plain", "skew")]
    for attempt in range(1, 4):
        for policy, traffic in mixes:
            run = f"{policy}-{traffic}-{attempt}"
            folder = root / run
            if (folder / "summary.json").exists():
                print(f"Reusing {run}", flush=True)
                continue
            print(f"Starting {run}", flush=True)
            cmd("rollout", "restart", "deploy/hw11-api")
            wait_ready()
            time.sleep(4)
            pods = pod_rows()
            assert len([p for p in pods if p["ready"] and not p["deleting"]]) == 2
            baseline = snapshot()
            rows = parallel_load(folder, run, policy, traffic, 120, args.rate)
            time.sleep(4)
            end = snapshot()
            jsonfile(folder / "histograms.json", {"before": baseline, "after": end, "pods": pods})
            for pod in pods:
                if not pod["deleting"]:
                    (folder / (pod["pod"] + ".jsonl")).write_text(cmd("logs", pod["pod"]))
            by_pod = dict(collections.Counter(row["pod"] or "unknown" for row in rows))
            outcome = dict(collections.Counter(row["outcome"] for row in rows))
            jsonfile(folder / "summary.json", {"run": run, "outcome": outcome, "by_pod": by_pod})
            print(run, json.dumps(outcome), by_pod, flush=True)


if __name__ == "__main__":
    main()
