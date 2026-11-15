"""Controlled CPU backend with bounded prefix cache and request admission."""
import asyncio
import hashlib
import json
import os
import time
from collections import OrderedDict
from pathlib import Path

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, ProcessCollector


def log(event, **fields):
    print(json.dumps({"event": event, "unix": time.time(), **fields}, ensure_ascii=False), flush=True)


class Simulator:
    def __init__(self):
        self.pod = os.environ.get("POD_NAME", "local")
        self.requested_version = os.environ.get("MODEL_VERSION", "A")
        self.artifact = Path(os.environ.get("ARTIFACT", "chart/model-A.json"))
        self.expected_sha = os.environ.get("MODEL_SHA256")
        self.warmup = float(os.environ.get("WARMUP_SECONDS", "20"))
        self.broken = os.environ.get("PROBE_MODE") == "broken"
        self.slots = asyncio.Semaphore(int(os.environ.get("RUNNING_LIMIT", "1")))
        self.max_inflight = int(os.environ.get("INFLIGHT_LIMIT", "9"))
        self.cache_size = int(os.environ.get("CACHE_SIZE", "16"))
        self.hit_work = int(os.environ.get("HIT_WORK", "50000"))
        self.miss_work = int(os.environ.get("MISS_WORK", "180000"))
        self.chunk_work = int(os.environ.get("CHUNK_WORK", "90000"))
        self.cache = OrderedDict()
        self.inflight = 0
        self.loaded = None
        self.sha = None
        self.draining = False
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        buckets = (.025, .05, .075, .1, .15, .2, .3, .4, .5, .75, 1, 1.5, 2, 3, 5, 8, 13, 21)
        self.ttfc = Histogram("hw11_ttfc_seconds", "Time to first generated fragment on server",
                              buckets=buckets, registry=self.registry)
        self.total = Histogram("hw11_duration_seconds", "Completed stream duration on server",
                               buckets=buckets, registry=self.registry)
        self.requests = Counter("hw11_requests", "Terminal request outcomes", ["outcome"], registry=self.registry)
        for status in ("ok", "rejected", "invalid", "unready", "aborted", "error"):
            self.requests.labels(status)
        self.running = Gauge("hw11_running", "Requests producing output", registry=self.registry)
        self.waiting = Gauge("hw11_waiting", "Requests waiting for compute", registry=self.registry)
        self.ready_metric = Gauge("hw11_backend_ready", "Backend verified and warmed", registry=self.registry)
        self.hits = Counter("hw11_prefix", "Prefix cache accesses", ["result"], registry=self.registry)
        for outcome in ("hit", "miss"):
            self.hits.labels(outcome)

    async def load(self):
        log("process_started", pod=self.pod, requested_version=self.requested_version)
        try:
            data = self.artifact.read_bytes()
            sha = hashlib.sha256(data).hexdigest()
            profile = json.loads(data)
            if sha != self.expected_sha or profile["version"] != self.requested_version:
                raise ValueError("artifact SHA-256 or version mismatch")
            await asyncio.sleep(self.warmup)
            await self.compute(b"warmup", self.miss_work)
            self.loaded, self.sha = profile["version"], sha
            self.ready_metric.set(1)
            log("backend_ready", pod=self.pod, version=self.loaded, sha256=sha)
        except (OSError, ValueError, KeyError) as exc:
            log("load_failed", pod=self.pod, requested_version=self.requested_version, error=str(exc))

    @staticmethod
    async def compute(data, iterations):
        if iterations:
            await asyncio.to_thread(hashlib.pbkdf2_hmac, "sha256", data, b"hw11-cpu", iterations)

    async def prefill(self, prefix):
        hit = prefix in self.cache
        if hit:
            self.cache.move_to_end(prefix)
        await self.compute(prefix.encode(), self.hit_work if hit else self.miss_work)
        if self.cache_size:
            self.cache[prefix] = None
            self.cache.move_to_end(prefix)
            while len(self.cache) > self.cache_size:
                self.cache.popitem(last=False)
        self.hits.labels("hit" if hit else "miss").inc()
        return hit
