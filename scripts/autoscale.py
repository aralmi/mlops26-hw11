"""Exercise CPU HPA, retained ClientIP affinity and both Prometheus alerts."""
import concurrent.futures
import json
import threading
import time

from cluster import PROM, RESULTS, append, cmd, fetch, helm, jsonfile, parallel_load, raw, state, wait_ready, wait_until

ROOT = RESULTS / "autoscale"


def sample():
    current = state()
    current["hpa"] = raw("get", "hpa", "hw11-api").get("status", {})
    data = json.loads(cmd("get", "--raw", "/apis/metrics.k8s.io/v1beta1/namespaces/hw11/pods"))
    current["resource_metrics"] = [r for r in data["items"] if r["metadata"]["name"].startswith("hw11-api-")]
    alerts = fetch(PROM + "/api/v1/rules")["data"]["groups"]
    current["alerts"] = {rule["name"]: rule["state"] for group in alerts for rule in group["rules"]}
    return current


def main():
    ROOT.mkdir(parents=True, exist_ok=True)
    for name in ("timeline.jsonl", "events.jsonl", "sample-errors.jsonl"):
        (ROOT / name).unlink(missing_ok=True)
    helm("-f", "configs/hpa.yaml", wait=True)
    wait_ready()

    def has_metrics():
        status = raw("get", "hpa", "hw11-api").get("status", {})
        return status.get("currentMetrics") and status["currentReplicas"] == 2

    wait_until(has_metrics, 150, interval=3)
    jsonfile(ROOT / "configuration.json", raw("get", "hpa", "hw11-api"))
    jsonfile(ROOT / "deployment.json", raw("get", "deploy", "hw11-api"))
    stopped = threading.Event()
    snapshots = []

    def watch():
        while not stopped.is_set():
            try:
                row = sample()
                snapshots.append(row)
                append(ROOT / "timeline.jsonl", row)
            except (ValueError, RuntimeError, TimeoutError) as exc:
                append(ROOT / "sample-errors.jsonl", dict(unix=time.time(), error=str(exc)))
            stopped.wait(1.5)

    monitor = threading.Thread(target=watch)
    monitor.start()
    try:
        def event(name, **fields):
            append(ROOT / "events.jsonl", dict(event=name, unix=time.time(), **fields))

        event("high_load_submitted")
        high = parallel_load(ROOT / "high", "hpa-high", "sticky", "skew", 105, 13, chunks=5, pace=16)
        event("high_load_completed")
        # A low but nonzero arrival rate allows both alert conditions to clear while still observed.
        event("low_load_submitted")
        low = parallel_load(ROOT / "low", "hpa-low", "sticky", "skew", 105, 0.9, chunks=1, pace=16)
        event("low_load_completed")
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(wait_until, lambda: next((r for r in reversed(snapshots)
                if r["hpa"].get("currentReplicas") == 2 and r["hpa"].get("desiredReplicas") == 2
                and sum(p["ready"] and not p["deleting"] for p in r["pods"]) == 2
                and all(s == "inactive" for s in r["alerts"].values())
                and r["unix"] > low[0]["scheduled_unix"]), None), 135, 2)
            final = future.result()
        jsonfile(ROOT / "final-state.json", final)
        (ROOT / "hpa-describe.txt").write_text(cmd("describe", "hpa", "hw11-api"))
        (ROOT / "events-kubernetes.txt").write_text(cmd("get", "events", "--sort-by=.metadata.creationTimestamp"))
        all_success = sum(r["outcome"] == "ok" for r in high + low)
        max_ready = max(sum(p["ready"] and not p["deleting"] for p in row["pods"]) for row in snapshots)
        fired = sorted({name for row in snapshots for name, status in row["alerts"].items() if status == "firing"})
        jsonfile(ROOT / "result.json", {"max_ready": max_ready, "fired": fired, "successful_requests": all_success})
        print("HPA:", max_ready, "Ready at peak. Alerts fired:", fired, flush=True)
    finally:
        stopped.set()
        monitor.join()


if __name__ == "__main__":
    main()
