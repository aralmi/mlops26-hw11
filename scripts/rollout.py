"""Warmup regression, graceful A→B rollout, bad B rollback and forced Pod loss."""
import concurrent.futures
import json
import threading
import time
from contextlib import contextmanager

from cluster import RESULTS, append, cmd, helm, jsonfile, lines, parallel_load, pod_rows, single, state, wait_ready, wait_until

ROOT = RESULTS / "rollout"


def event(folder, name, **fields):
    append(folder / "events.jsonl", {"event": name, "unix": time.time(), **fields})


@contextmanager
def watch(folder):
    folder.mkdir(parents=True, exist_ok=True)
    for name in ("events.jsonl", "timeline.jsonl", "sample-errors.jsonl"):
        (folder / name).unlink(missing_ok=True)
    stopped = threading.Event()

    def sample():
        while not stopped.is_set():
            try:
                append(folder / "timeline.jsonl", state())
            except RuntimeError as exc:
                append(folder / "sample-errors.jsonl", dict(unix=time.time(), error=str(exc)))
            stopped.wait(.6)

    thread = threading.Thread(target=sample)
    thread.start()
    try:
        yield
    finally:
        stopped.set()
        thread.join()


def first_fragment(process):
    marker = json.loads(process.stdout.readline())
    assert marker["event"] == "first", marker
    return marker


def last_record(process):
    out, err = process.communicate(timeout=60)
    if process.returncode:
        raise RuntimeError(err or out)
    return lines(out)[-1]


def warmup_trial(mode):
    folder = ROOT / mode
    helm("--set", f"probeMode={mode}", wait=True)
    before = [pod for pod in pod_rows() if pod["ready"] and not pod["deleting"]]
    assert len(before) == 2
    if mode == "broken":
        # The deliberately broken Kubernetes probe is not evidence that A finished warming.
        for pod in before:
            wait_until(lambda: '"event": "backend_ready"' in cmd("logs", pod["pod"], check=False), 35)
    with watch(folder), concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(parallel_load, folder, mode, "plain", "uniform", 63, 2)
        wait_until(lambda: (folder / "schedule.json").is_file(), 10)
        time.sleep(8)
        old = {p["pod"] for p in before if p["ready"] and not p["deleting"]}
        probes = []
        if mode == "fixed":
            active = single("fixed-long", background=True, max_new_tokens=12, pace_ms=550)
            event(folder, "long_first", **{k: v for k, v in first_fragment(active).items() if k != "event"})
        event(folder, "upgrade_A_to_B")
        helm("--set", f"probeMode={mode}", "--set", "modelVersion=B")
        if mode == "fixed":
            # Start more streams toward A in the second half of B's warmup to observe drain.
            time.sleep(12)
            for index, pod in enumerate(p for p in before if p["pod"] in old):
                proc = single(f"late-A-{index}", background=True, max_new_tokens=24, pace_ms=650,
                              url=f"http://{pod['ip']}:8080/generate", client=index)
                probes.append(proc)
            event(folder, "late_A_requests_started")
            extra = [last_record(active), *(last_record(p) for p in probes)]
            jsonfile(folder / "long-requests.json", extra)
        wait_ready()
        event(folder, "rollout_complete")
        rows = future.result()
        for pod in pod_rows():
            if not pod["deleting"]:
                (folder / (pod["pod"] + ".jsonl")).write_text(cmd("logs", pod["pod"]))
    jsonfile(folder / "result.json", {
        "warming_503": sum(r["status"] == 503 and r["pod"] not in old for r in rows),
        "http_5xx": sum(r["status"] is not None and r["status"] >= 500 for r in rows),
        "aborted_or_timeout": sum(r["outcome"] in ("aborted", "timeout", "connect_error") for r in rows),
        "versions": sorted({r["version"] for r in rows if r["version"]}),
        "processed": len(rows),
    })


def bad_release():
    folder = ROOT / "rollback"
    helm("--set", "modelVersion=A", wait=True)
    existing_uids = {p["uid"] for p in pod_rows()}
    revision = json.loads(cmd("get", "deploy/hw11-api", "-o", "json"))["metadata"]["annotations"]["deployment.kubernetes.io/revision"]
    from cluster import shell
    history = json.loads(shell("helm", "history", "hw11", "-n", "hw11", "-o", "json").stdout)
    release_revision = history[-1]["revision"]
    with watch(folder), concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(parallel_load, folder, "rollback", "plain", "uniform", 42, 1.5)
        wait_until(lambda: (folder / "schedule.json").is_file(), 10)
        time.sleep(7)
        event(folder, "invalid_B", previous_deployment_revision=revision, rollback_to=release_revision)
        helm("--set", "modelVersion=B", "--set", "artifactVersion=A")
        def bad_pod():
            return next((p for p in pod_rows() if p["requested_version"] == "B" and p["ip"]
                         and p["uid"] not in existing_uids and not p["deleting"]), None)
        bad = wait_until(bad_pod, 25)
        failure_log = wait_until(lambda: (log if "load_failed" in (log := cmd("logs", bad["pod"], check=False)) else None), 15)
        (folder / "bad-pod.jsonl").write_text(failure_log)
        direct = single("bad-B-direct", url=f"http://{bad['ip']}:8080/generate")
        jsonfile(folder / "direct-B.json", direct)
        time.sleep(8)
        event(folder, "helm_rollback", revision=release_revision)
        output = shell("helm", "rollback", "hw11", str(release_revision), "-n", "hw11", "--wait", "--timeout", "100s")
        (folder / "helm-rollback.txt").write_text(output.stdout)
        wait_ready()
        event(folder, "rollback_ready")
        future.result()
        samples = [single(f"after-rollback-{i}", client=i % 4) for i in range(12)]
        jsonfile(folder / "after-rollback.json", samples)
        jsonfile(folder / "invalid-pod.json", bad)


def forced_loss():
    folder = ROOT / "forced"
    with watch(folder):
        before = [p for p in pod_rows() if p["ready"] and not p["deleting"]]
        target = before[0]
        direct = f"http://{target['ip']}:8080/generate"
        alpha = [{"role": "user", "content": "контроль-альфа-171"}]
        beta = [{"role": "user", "content": "контроль-бета-254"}]
        alpha_before = single("alpha-before", url=direct, session_id="alpha", history=alpha)
        beta_before = single("beta-before", url=direct, session_id="beta", history=beta)
        long = single("force-interrupted", url=direct, background=True, max_new_tokens=24, pace_ms=700,
                      session_id="alpha", history=alpha)
        event(folder, "interrupted_first", **{k: v for k, v in first_fragment(long).items() if k != "event"})
        # Fill all eight waiting slots while the long stream owns the compute slot.
        waiting = [single(f"force-wait-{i}", url=direct, background=True, client=i % 4) for i in range(8)]
        time.sleep(2)
        rejection = single("force-rejected", url=direct)
        event(folder, "force_delete", pod=target["pod"])
        cmd("delete", "pod", target["pod"], "--force", "--grace-period=0", "--wait=false")
        interrupted = last_record(long)
        collateral = [last_record(p) for p in waiting]
        event(folder, "first_remaining_service_success", request=single("service-during-replacement"))
        previous_ids = {p["uid"] for p in before}
        replacement = wait_until(lambda: next((p for p in pod_rows()
                                 if p["ready"] and p["uid"] not in previous_ids), None), 85)
        event(folder, "replacement_ready", pod=replacement["pod"])
        replaced = f"http://{replacement['ip']}:8080/generate"
        alpha_after = single("alpha-after", url=replaced, session_id="alpha", history=alpha)
        beta_after = single("beta-after", url=replaced, session_id="beta", history=beta)
        jsonfile(folder / "requests.json", {"alpha_before": alpha_before, "beta_before": beta_before,
            "interrupted": interrupted, "collateral": collateral, "rejection": rejection,
            "replacement": replacement, "alpha_after": alpha_after, "beta_after": beta_after})


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("phases", nargs="*")
    args = parser.parse_args()
    for phase in args.phases or ("broken", "fixed", "rollback", "forced"):
        print("Starting", phase, flush=True)
        if phase in ("broken", "fixed"):
            warmup_trial(phase)
        elif phase == "rollback":
            bad_release()
        else:
            forced_loss()
        print("Saved", phase, flush=True)


if __name__ == "__main__":
    main()
