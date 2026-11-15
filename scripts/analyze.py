"""Derive all displayed aggregates from the raw request and Kubernetes traces."""
import collections
import csv
import json
import math

from cluster import RESULTS, jsonfile, lines


def read(path):
    return json.loads(path.read_text())


def stream(path):
    return lines(path.read_text())


def percentile(values, p):
    x = sorted(v for v in values if v is not None)
    if not x:
        return None
    i = (len(x) - 1) * p
    lo, hi = int(i), math.ceil(i)
    return x[lo] + (i - lo) * (x[hi] - x[lo])


def metric(rows, key):
    values = [r.get(key) for r in rows if r.get(key) is not None]
    return dict(n=len(values), p50=percentile(values,.5), p95=percentile(values,.95))


def aggregate(rows):
    ok = [r for r in rows if r["outcome"] == "ok"]
    counts = collections.Counter(r["outcome"] for r in rows)
    return dict(planned=len(rows), attempted=sum(r.get("connect_mono_ns") is not None for r in rows),
                sent=sum(r["sent_mono_ns"] is not None for r in rows), successful=len(ok),
                completed_http=sum(r["outcome"] in ("ok", "rejected", "http_error") for r in rows),
                rejected=counts["rejected"], aborted=counts["aborted"], timeout=counts["timeout"],
                connect_error=counts["connect_error"], http_error=counts["http_error"],
                http_5xx=sum(r["status"] is not None and r["status"] >= 500 for r in rows),
                errors=len(rows)-len(ok), ttfc=metric(ok, "ttfc_seconds"),
                queue=metric(ok, "queue_seconds"), total=metric(ok, "total_seconds"),
                send_lag=metric(rows, "send_lag_seconds"),
                acceptance=len(ok)/len(rows) if rows else None,
                cache_hits=sum(bool(r.get("cache_hit")) for r in ok),
                inter_chunk=metric([{"seconds": v} for r in ok for v in r["inter_chunk_seconds"]], "seconds"))


def per_pod(rows):
    groups = collections.defaultdict(list)
    for r in rows:
        groups[r["pod"] or "unknown"].append(r)
    return {pod: aggregate(group) for pod,group in sorted(groups.items())}


def balance_data():
    runs = []
    rows_all = []
    for file in sorted((RESULTS/"balance").glob("*/requests.jsonl")):
        folder = file.parent
        if not (folder / "summary.json").exists():
            continue
        meta = read(folder/"schedule.json")
        rows = stream(file)
        assert len(rows) == math.ceil(meta["rate"] * meta["duration_seconds"])
        assert len({r["request_id"] for r in rows}) == len(rows)
        assert {r["index"] for r in rows} == set(range(len(rows)))
        good = [r for r in rows if r["outcome"] == "ok"]
        clients = collections.Counter(r["client_ip"] for r in rows)
        matrix = collections.defaultdict(collections.Counter)
        for r in rows:
            matrix[r["client_ip"]][r["pod"] or "unknown"] += 1
        result = dict(schedule=meta, stats=aggregate(rows), pod=per_pod(rows), clients=dict(clients),
                      routes={client:dict(counts) for client,counts in matrix.items()},
                      source_ip_matches=sum(r["client_ip"] == r["source_ip"] for r in good),
                      first_sent=min(r["sent_unix"] for r in rows if r["sent_unix"]),
                      last_sent=max(r["sent_unix"] for r in rows if r["sent_unix"]))
        if (folder/"promql.json").exists():
            result["promql"] = read(folder/"promql.json")
        runs.append(result)
        rows_all.extend(rows)
    return runs, rows_all


def rollout_data():
    book = {}
    collection = []
    root = RESULTS/"rollout"
    for phase in ("broken","fixed","rollback"):
        folder = root/phase
        if not (folder/"requests.jsonl").exists():
            continue
        rows = stream(folder/"requests.jsonl")
        extra = []
        if phase == "fixed" and (folder/"long-requests.json").exists():
            extra = read(folder/"long-requests.json")
        if phase == "rollback" and (folder/"after-rollback.json").exists():
            extra = [read(folder/"direct-B.json"), *read(folder/"after-rollback.json")]
        book[phase] = dict(stats=aggregate(rows), versions=dict(collections.Counter(r["version"] or "unloaded" for r in rows)),
                           extra=extra, events=stream(folder/"events.jsonl"))
        collection.extend({**r, "run":f"rollout-{phase}"} for r in rows+extra)
    if (root/"forced"/"requests.json").exists():
        forced = read(root/"forced"/"requests.json")
        recovery = next(r["request"] for r in stream(root/"forced"/"events.jsonl")
                        if r["event"] == "first_remaining_service_success")
        book["forced"] = dict(requests=forced, events=stream(root/"forced"/"events.jsonl"),
                              recovery=recovery)
        collection.extend({**r, "run":"rollout-forced"} for r in (
            forced["alpha_before"], forced["beta_before"], forced["alpha_after"], forced["beta_after"],
            forced["rejection"], forced["interrupted"], recovery, *forced["collateral"]))
    return book, collection


def autoscale_data():
    root = RESULTS/"autoscale"
    if not (root/"result.json").exists():
        return {}, []
    high, low = [stream(root/phase/"requests.jsonl") for phase in ("high","low")]
    samples = stream(root/"timeline.jsonl")
    configuration = read(root/"configuration.json")
    start = read(root/"high"/"schedule.json")["start_unix"]
    low_start = read(root/"low"/"schedule.json")["start_unix"]
    desired3 = next((r["unix"] for r in samples if r["hpa"].get("desiredReplicas") == 3), None)
    after3 = next((r for r in samples if sum(p["ready"] and not p["deleting"] for p in r["pods"]) == 3), None)
    original = {p["uid"] for p in samples[0]["pods"]}
    new = next((p for p in after3["pods"] if p["uid"] not in original), None) if after3 else None
    return2 = next((r["unix"] for r in samples if r["unix"] > low_start and r["hpa"].get("desiredReplicas") == 2
                    and sum(p["ready"] and not p["deleting"] for p in r["pods"]) == 2), None)
    alerts = collections.defaultdict(list)
    for r in samples:
        for name, mode in r["alerts"].items():
            if not alerts[name] or alerts[name][-1][1] != mode:
                alerts[name].append((r["unix"] - start, mode))
    hot_when_three = per_pod([r for r in high if after3 and r["sent_unix"] and r["sent_unix"] > after3["unix"]])
    return dict(result=read(root/"result.json"), events=stream(root/"events.jsonl"),
                high=aggregate(high), low=aggregate(low), high_pods=per_pod(high), after_three=hot_when_three,
                high_start=start, low_start=low_start, scale_up=desired3, third_ready=after3["unix"] if after3 else None,
                third=new, scale_down=return2, alerts=dict(alerts),
                stabilization=configuration["spec"]["behavior"]["scaleDown"]["stabilizationWindowSeconds"]), high+low


def main():
    bal, a = balance_data()
    roll, b = rollout_data()
    hpa, c = autoscale_data()
    jsonfile(RESULTS/"analysis.json", dict(balance=bal, rollout=roll, autoscale=hpa))
    fields = ("run","request_id","index","client_pod","client_ip","source_ip","scheduled_unix",
              "scheduled_mono_ns","sent_mono_ns","sent_unix","first_mono_ns","end_mono_ns","end_unix",
              "status","pod","version","chunks","done","outcome","backend_mode","queue_seconds","ttfc_seconds",
              "total_seconds","send_lag_seconds","inter_chunk_seconds","cache_hit","artifact_sha256","error")
    with (RESULTS/"requests.csv").open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in a+b+c:
            writer.writerow(row)


if __name__ == "__main__":
    main()
