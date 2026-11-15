"""Shared client and cluster operations for the HW11 experiments."""
import concurrent.futures
import json
import math
import subprocess
import time
import urllib.parse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
NS = "hw11"
PROM = "http://hw11-prometheus:9090"


def shell(*args, check=True, timeout=150):
    result = subprocess.run(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, check=False, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{' '.join(args)}: {result.stderr or result.stdout}")
    return result


def cmd(*args, **kwargs):
    return shell("kubectl", "-n", NS, *args, **kwargs).stdout


def raw(*args):
    return json.loads(cmd(*args, "-o", "json"))


def lines(data):
    return [json.loads(x) for x in data.splitlines() if x.strip()]


def jsonfile(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")


def append(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def service(policy="plain"):
    return f"http://hw11-{policy}:8080/generate"


def in_client(client, *args):
    return ("kubectl", "-n", NS, "exec", f"hw11-client-{client}", "--", *args)


def fetch(url, client=0):
    code = "import urllib.request,sys; print(urllib.request.urlopen(sys.argv[1],timeout=8).read().decode())"
    return json.loads(shell(*in_client(client, "python", "-c", code, url), timeout=15).stdout)


def query(expression, at=None):
    params = {"query": expression}
    if at is not None:
        params["time"] = at
    result = fetch(PROM + "/api/v1/query?" + urllib.parse.urlencode(params))
    if result["status"] != "success":
        raise RuntimeError(result)
    return result["data"]["result"]


def pod_rows():
    return [dict(pod=p["metadata"]["name"], ip=p["status"].get("podIP"),
                 uid=p["metadata"]["uid"], created=p["metadata"]["creationTimestamp"],
                 deleting=p["metadata"].get("deletionTimestamp"), phase=p["status"].get("phase"),
                 ready=next((x["status"] == "True" for x in p["status"].get("conditions", [])
                             if x["type"] == "Ready"), False),
                 ready_transition=next((x["lastTransitionTime"] for x in p["status"].get("conditions", [])
                                         if x["type"] == "Ready"), None),
                 restarts=sum(x["restartCount"] for x in p["status"].get("containerStatuses", [])),
                 requested_version=next(e["value"] for e in p["spec"]["containers"][0]["env"]
                                        if e["name"] == "MODEL_VERSION"))
            for p in raw("get", "pod", "-l", "app=hw11-api")["items"]]


def state():
    slices = raw("get", "endpointslice", "-l", "kubernetes.io/service-name=hw11-plain")["items"]
    endpoints = [dict(pod=e.get("targetRef", {}).get("name"), addresses=e["addresses"], **e["conditions"])
                 for es in slices for e in es["endpoints"]]
    return dict(unix=time.time(), pods=pod_rows(), endpoints=endpoints)


def helm(*values, wait=False):
    args = ["helm", "upgrade", "--install", "hw11", "chart", "-n", NS]
    args.extend(values)
    if wait:
        args.extend(["--wait", "--timeout", "105s"])
    return shell(*args).stdout


def wait_ready():
    cmd("rollout", "status", "deploy/hw11-api", "--timeout=115s")


def parallel_load(folder, run, policy, traffic, duration, rate, chunks=4, pace=24, url=None):
    folder.mkdir(parents=True, exist_ok=True)
    start = time.time() + 5
    job = dict(run=run, policy=policy, traffic=traffic, start_unix=start, duration_seconds=duration,
               rate=rate, chunks=chunks, pace_ms=pace, url=url or service(policy), client_count=4,
               connection_close=True)
    jsonfile(folder / "schedule.json", job)

    def run_client(index):
        output = shell(*in_client(index, "python", "/app/load.py", job["url"],
                       "--run", run, "--start", str(start), "--duration", str(duration),
                       "--rate", str(rate), "--traffic", traffic, "--client", str(index),
                       "--chunks", str(chunks), "--pace", str(pace)), timeout=duration+80)
        (folder / f"client-{index}.jsonl").write_text(output.stdout)
        if output.stderr:
            (folder / f"client-{index}.stderr").write_text(output.stderr)
        return lines(output.stdout)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        rows = sorted((row for client in pool.map(run_client, range(4)) for row in client),
                      key=lambda row: row["index"])
    expected = math.ceil(duration * rate)
    assert [row["index"] for row in rows] == list(range(expected)), "missing/duplicate scheduled requests"
    data = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    (folder / "requests.jsonl").write_text(data)
    return rows


def single(request_id, policy="plain", url=None, client=0, background=False, **payload):
    item = dict(request_id=request_id, announce_first=background,
                payload={"prompt": "pandas test", "history": [], "max_new_tokens": 3, "pace_ms": 30, **payload})
    args = in_client(client, "python", "/app/load.py", url or service(policy), "--single", json.dumps(item))
    if background:
        return subprocess.Popen(args, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return lines(shell(*args).stdout)[-1]


def wait_until(predicate, limit, interval=1):
    until = time.monotonic() + limit
    while time.monotonic() < until:
        result = predicate()
        if result:
            return result
        time.sleep(interval)
    raise TimeoutError(f"condition did not hold within {limit}s")
