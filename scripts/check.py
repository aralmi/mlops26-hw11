"""Acceptance checks for the five Kubernetes exercises.

--live replays the Kubernetes requests before checking the recorded observations.
Without it, the checks read the retained HTTP results and endpoint timelines.
"""
import argparse
import hashlib
import json
import sys
import unittest

from cluster import RESULTS, ROOT as PROJECT, lines

ROOT = RESULTS / "rollout"


def read(path):
    return json.loads((ROOT / path).read_text())


def records(path):
    return lines((ROOT / path).read_text())


def actual_a(row):
    artifact_sha = hashlib.sha256((PROJECT / "chart" / "model-A.json").read_bytes()).hexdigest()
    return (row["outcome"], row["version"], row["backend_mode"], row["artifact_sha256"]) == (
        "ok", "A", "simulated", artifact_sha)


class KubernetesAcceptance(unittest.TestCase):
    def test_1_warmup_and_graceful_streams(self):
        """HTTP under A→B with both probe variants, correlated to EndpointSlice."""
        broken, fixed = read("broken/result.json"), read("fixed/result.json")
        self.assertGreater(broken["warming_503"], 0)
        self.assertEqual(fixed["http_5xx"], 0)
        self.assertEqual(fixed["aborted_or_timeout"], 0)
        self.assertEqual(fixed["versions"], ["A", "B"])
        trace = records("fixed/timeline.jsonl")
        self.assertTrue(any(any(p["requested_version"] == "B" and not p["ready"] for p in sample["pods"])
                            and all(not e["ready"] for e in sample["endpoints"] if e["pod"] in
                                    {p["pod"] for p in sample["pods"] if p["requested_version"] == "B" and not p["ready"]})
                            for sample in trace))
        events = records("fixed/events.jsonl")
        update = next(r["unix"] for r in events if r["event"] == "upgrade_A_to_B")
        streams = read("fixed/long-requests.json")
        initial = streams[0]
        self.assertLess(initial["sent_unix"], update)
        self.assertGreater(initial["end_unix"], update)
        self.assertTrue(all(row["done"] and row["outcome"] == "ok" and row["version"] == "A" for row in streams))
        self.assertTrue(any(sample["unix"] < row["end_unix"] and any(
            endpoint["pod"] == row["pod"] and endpoint.get("terminating") and not endpoint["ready"]
            for endpoint in sample["endpoints"]) for sample in trace for row in streams))

    def test_2_invalid_b_and_actual_rollback_version(self):
        direct = read("rollback/direct-B.json")
        self.assertEqual(direct["status"], 503)
        bad = read("rollback/invalid-pod.json")["pod"]
        trace = records("rollback/timeline.jsonl")
        self.assertTrue(any(p["pod"] == bad for sample in trace for p in sample["pods"]))
        self.assertFalse(any(e["pod"] == bad and e["ready"] for sample in trace for e in sample["endpoints"]))
        self.assertTrue(all(actual_a(row) for row in records("rollback/requests.jsonl")))
        self.assertTrue(all(actual_a(row) for row in read("rollback/after-rollback.json")))

    def test_3_replacement_reloads_verified_version(self):
        rows = read("forced/requests.json")
        old, new = rows["alpha_before"], rows["alpha_after"]
        self.assertTrue(actual_a(old) and actual_a(new))
        self.assertNotEqual(old["pod"], new["pod"])
        self.assertEqual(rows["replacement"]["pod"], new["pod"])
        self.assertTrue(rows["replacement"]["ready"])

    def test_4_distinguish_rejection_from_interrupted_stream(self):
        rows = read("forced/requests.json")
        rejected, stopped = rows["rejection"], rows["interrupted"]
        self.assertEqual((rejected["status"], rejected["outcome"], rejected["chunks"], rejected["done"]),
                         (429, "rejected", 0, False))
        self.assertEqual((stopped["status"], stopped["outcome"], stopped["done"]), (200, "aborted", False))
        self.assertLess(stopped["chunks"], 24)
        self.assertGreater(stopped["chunks"], 0)

    def test_5_isolated_dialogues_with_explicit_history(self):
        rows = read("forced/requests.json")
        for prefix, marker in (("alpha", "контроль-альфа-171"), ("beta", "контроль-бета-254")):
            before, after = rows[f"{prefix}_before"], rows[f"{prefix}_after"]
            self.assertEqual(before["context_marker"], marker)
            self.assertEqual(after["context_marker"], marker)
            self.assertEqual(before["history_sha256"], after["history_sha256"])
            self.assertEqual(after["outcome"], "ok")
        self.assertNotEqual(rows["alpha_after"]["history_sha256"], rows["beta_after"]["history_sha256"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    if args.live:
        from rollout import bad_release, forced_loss, warmup_trial
        for mode in ("broken", "fixed"):
            warmup_trial(mode)
        bad_release()
        forced_loss()
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(KubernetesAcceptance)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())


if __name__ == "__main__":
    main()
