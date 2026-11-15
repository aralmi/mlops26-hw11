"""HW8–10 HTTP contract, extended with streaming and a CPU simulator for HW11."""
import asyncio
import hashlib
import json
import os
import time
import uuid
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from simulator import Simulator, log


def validate(payload):
    if not isinstance(payload, dict) or set(payload) - {"prompt", "max_new_tokens", "history", "session_id", "pace_ms"}:
        raise ValueError("expected prompt, history, session_id, max_new_tokens and pace_ms")
    prompt = payload.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > 8000:
        raise ValueError("prompt must contain 1..8000 characters")
    history = payload.get("history", [])
    if not isinstance(history, list) or len(history) > 16:
        raise ValueError("history must contain up to 16 messages")
    for message in history:
        if (not isinstance(message, dict) or set(message) != {"role", "content"}
                or message["role"] not in ("user", "assistant")
                or not isinstance(message["content"], str) or len(message["content"]) > 2000):
            raise ValueError("invalid history message")
    chunks, pace = payload.get("max_new_tokens", 4), payload.get("pace_ms", 40)
    if type(chunks) is not int or not 1 <= chunks <= 24 or type(pace) is not int or not 0 <= pace <= 750:
        raise ValueError("limits: 1..24 fragments and 0..750 ms pacing")
    session = payload.get("session_id", "")
    if not isinstance(session, str) or len(session) > 80:
        raise ValueError("invalid session ID")
    return prompt, history, chunks, pace, session


@asynccontextmanager
async def lifespan(app):
    app.state.backend = Simulator()
    loader = asyncio.create_task(app.state.backend.load())
    yield
    loader.cancel()
    with suppress(asyncio.CancelledError):
        await loader


app = FastAPI(title="Pandas model release API · Kubernetes", lifespan=lifespan)


def probe(request, ready):
    backend = request.app.state.backend
    return JSONResponse({
        "ready": bool(ready), "pod": backend.pod, "model_id": backend.loaded,
        "artifact_sha256": backend.sha, "backend_mode": "simulated",
    }, status_code=200 if ready else 503)


@app.get("/health")
async def health(request: Request):
    return probe(request, True)


@app.get("/startup")
async def startup(request: Request):
    backend = request.app.state.backend
    return probe(request, backend.broken or backend.loaded is not None)


@app.get("/ready")
async def ready(request: Request):
    backend = request.app.state.backend
    return probe(request, (backend.broken or backend.loaded is not None) and not backend.draining)


@app.get("/metrics")
async def metrics(request: Request):
    return Response(generate_latest(request.app.state.backend.registry), headers={"Content-Type": CONTENT_TYPE_LATEST})


@app.post("/generate")
async def generate(request: Request):
    backend = request.app.state.backend
    started = time.monotonic()
    supplied_id = request.headers.get("x-request-id", "")
    request_id = supplied_id if supplied_id and len(supplied_id) <= 100 else str(uuid.uuid4())
    remote = request.client.host if request.client else None
    meta = dict(request_id=request_id, pod=backend.pod, version=backend.loaded,
                backend_mode="simulated", source_ip=remote, artifact_sha256=backend.sha)
    headers = {"X-Request-ID": request_id, "X-Pod": backend.pod, "X-Model-Version": backend.loaded or "",
               "X-Backend-Mode": "simulated"}
    session = ""

    def reject(status, outcome, message):
        backend.requests.labels(outcome).inc()
        log("request_end", **meta, outcome=outcome, status=status, session_id=session)
        return JSONResponse({**meta, "error": message}, status_code=status, headers=headers)

    body = bytearray()
    async for part in request.stream():
        body.extend(part)
        if len(body) > 65536:
            return reject(413, "invalid", "body too large")
    try:
        prompt, history, chunks, pace, session = validate(json.loads(body))
    except (ValueError, TypeError) as exc:
        return reject(422, "invalid", str(exc))
    if backend.loaded is None:
        return reject(503, "unready", "backend warming or failed")
    if backend.inflight >= backend.max_inflight:
        return reject(429, "rejected", "inference queue full")

    backend.inflight += 1
    backend.waiting.inc()

    async def stream():
        running, sent = False, 0
        outcome = "aborted"
        queued, first, hit = None, None, None
        try:
            async with backend.slots:
                running = True
                backend.waiting.dec()
                backend.running.inc()
                queued = time.monotonic() - started
                # Cache identity is derived from the reused input prefix and explicit history.
                digest = hashlib.sha256(json.dumps(history, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
                prefix = prompt.split("\n", 1)[0] + ":" + digest
                hit = await backend.prefill(prefix)
                marker = history[-1]["content"] if history else ""
                for index in range(chunks):
                    await backend.compute(prefix.encode(), backend.chunk_work)
                    await asyncio.sleep(pace / 1000)
                    if first is None:
                        first = time.monotonic() - started
                    fragment = dict(type="chunk", index=index, text=f"fragment {index + 1}", **meta)
                    if index == 0:
                        fragment.update(queue_seconds=queued, cache_hit=hit, history_sha256=digest,
                                        context_marker=marker)
                    sent += 1
                    yield json.dumps(fragment, ensure_ascii=False) + "\n"
                yield json.dumps(dict(type="done", chunks=sent, **meta)) + "\n"
                outcome = "ok"
                backend.ttfc.observe(first)
                backend.total.observe(time.monotonic() - started)
        except Exception:
            outcome = "error"
            raise
        finally:
            backend.inflight -= 1
            (backend.running if running else backend.waiting).dec()
            backend.requests.labels(outcome).inc()
            log("request_end", **meta, session_id=session, outcome=outcome, status=200,
                chunks=sent, queue_seconds=queued, ttfc_seconds=first, cache_hit=hit)

    return StreamingResponse(stream(), media_type="application/x-ndjson", headers=headers)
