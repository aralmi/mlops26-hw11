"""Open-loop HTTP load in client Pods. Every scheduled attempt is emitted as JSONL."""
import argparse
import concurrent.futures
import http.client
import json
import math
import os
import socket
import time
import urllib.parse


def record(url, item, timeout=30):
    req = dict(item)
    payload = req.pop("payload")
    target = urllib.parse.urlsplit(url)
    req.update(client_ip=os.environ.get("POD_IP"), client_pod=os.environ.get("POD_NAME"),
               sent_mono_ns=None, sent_unix=None, first_mono_ns=None, end_mono_ns=None,
               status=None, pod=None, version=None, chunks=0, outcome="unsent", done=False)
    conn = http.client.HTTPConnection(target.hostname, target.port or 80, timeout=timeout)
    intervals = []
    previous = None
    try:
        # Failed TCP connections are counted as attempts, with a separate connect timestamp.
        req["connect_mono_ns"] = time.monotonic_ns()
        conn.connect()
        req["sent_mono_ns"], req["sent_unix"] = time.monotonic_ns(), time.time()
        conn.request("POST", target.path or "/generate", json.dumps(payload),
                     {"Content-Type": "application/json", "Connection": "close",
                      "X-Request-ID": req["request_id"]})
        with conn.getresponse() as response:
            req.update(status=response.status, pod=response.getheader("X-Pod"),
                       version=response.getheader("X-Model-Version"),
                       backend_mode=response.getheader("X-Backend-Mode"))
            if response.status != 200:
                req["outcome"] = "rejected" if response.status == 429 else "http_error"
                req["error"] = response.read().decode(errors="replace")[:500]
            else:
                for line in response:
                    part = json.loads(line)
                    if part["type"] == "done":
                        req["done"] = part["chunks"] == req["chunks"] == payload.get("max_new_tokens", 4)
                        break
                    if part["type"] != "chunk" or part["index"] != req["chunks"]:
                        raise ValueError("invalid stream order")
                    now = time.monotonic_ns()
                    req["chunks"] += 1
                    if previous is None:
                        req.update(first_mono_ns=now, cache_hit=part.get("cache_hit"),
                                   queue_seconds=part.get("queue_seconds"), source_ip=part.get("source_ip"),
                                   artifact_sha256=part.get("artifact_sha256"), context_marker=part.get("context_marker"),
                                   history_sha256=part.get("history_sha256"))
                        if item.get("announce_first"):
                            print(json.dumps({"event": "first", "request_id": req["request_id"],
                                              "pod": req["pod"], "version": req["version"], "unix": time.time()}), flush=True)
                    else:
                        intervals.append((now - previous) / 1e9)
                    previous = now
                req["outcome"] = "ok" if req["done"] else "aborted"
    except (TimeoutError, socket.timeout) as exc:
        req.update(outcome="timeout", error=str(exc))
    except (OSError, ValueError, http.client.HTTPException) as exc:
        req.update(outcome="aborted" if req["status"] == 200 else "connect_error",
                   error=f"{type(exc).__name__}: {exc}")
    finally:
        conn.close()
        req["end_mono_ns"], req["end_unix"] = time.monotonic_ns(), time.time()
    sent = req["sent_mono_ns"]
    first = req["first_mono_ns"]
    req.update(ttfc_seconds=(first - sent) / 1e9 if sent and first else None,
               total_seconds=(req["end_mono_ns"] - sent) / 1e9 if sent else None,
               send_lag_seconds=req["sent_unix"] - req["scheduled_unix"] if sent else None,
               inter_chunk_seconds=intervals)
    return req


def client_for(index, traffic):
    if traffic == "uniform":
        return index % 4
    return (1 + (index // 5) % 3) if index % 5 == 4 else 0


def schedule(args):
    for index in range(math.ceil(args.duration * args.rate)):
        source = client_for(index, args.traffic)
        if source != args.client:
            continue
        prefix = f"prefix-{index % 16:02d}"
        yield dict(request_id=f"{args.run}-{index:06d}", run=args.run, index=index, client=source,
                   phase="measurement", scheduled_unix=args.start + index / args.rate,
                   payload={"prompt": prefix + "\n" + "input " * 16, "max_new_tokens": args.chunks,
                            "pace_ms": args.pace, "session_id": f"load-{index % 16:02d}"})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("--single", help="JSON for a single request with payload and request_id")
    parser.add_argument("--run")
    parser.add_argument("--start", type=float)
    parser.add_argument("--duration", type=float, default=120)
    parser.add_argument("--rate", type=float, default=7.5)
    parser.add_argument("--traffic", choices=["uniform", "skew"], default="uniform")
    parser.add_argument("--client", type=int, default=0)
    parser.add_argument("--chunks", type=int, default=4)
    parser.add_argument("--pace", type=int, default=24)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    if args.single:
        item = json.loads(args.single)
        item.setdefault("scheduled_unix", time.time())
        print(json.dumps(record(args.url, item, args.timeout), ensure_ascii=False), flush=True)
        return
    anchor = time.monotonic() + (args.start - time.time())
    with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
        futures = []
        for item in schedule(args):
            deadline = anchor + item["index"] / args.rate
            time.sleep(max(deadline - time.monotonic(), 0))
            item["scheduled_mono_ns"] = int(deadline * 1e9)
            future = pool.submit(record, args.url, item, args.timeout)
            future.add_done_callback(lambda done: print(json.dumps(done.result()), flush=True))
            futures.append(future)
        concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()
