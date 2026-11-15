"""Query and save PromQL against the same successful responses as the HTTP log."""
import argparse
import json
import re

from cluster import RESULTS, jsonfile, query


def expression(metric, pods, start, end):
    selector = '{job="hw11-api",pod=~"' + "|".join(re.escape(p) for p in pods).replace(r"\-", "-") + '"}'
    series = metric + selector
    window = "15s"
    return (f"sum by (le) (last_over_time({series}[{window}] @ {end:.3f}) "
            f"- last_over_time({series}[{window}] @ {start:.3f}))")


def compare(folder):
    data = json.loads((folder / "histograms.json").read_text())
    pods = [p["pod"] for p in data["pods"] if p["ready"] and not p["deleting"]]
    statements = {}
    for metric in ("hw11_ttfc_seconds_bucket", "hw11_duration_seconds_bucket"):
        before = next(v["value"][0] for v in data["before"][metric] if v["metric"]["pod"] in pods)
        after = next(v["value"][0] for v in data["after"][metric] if v["metric"]["pod"] in pods)
        buckets = expression(metric, pods, before, after)
        for name, statement in (("buckets", buckets), ("p50", f"histogram_quantile(0.5, {buckets})"),
                                ("p95", f"histogram_quantile(0.95, {buckets})")):
            statements[f"{metric}:{name}"] = {"query": statement, "result": query(statement)}
    jsonfile(folder / "promql.json", statements)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run", nargs="*", default=["plain-skew-1", "sticky-skew-1"])
    args = parser.parse_args()
    for run in args.run:
        compare(RESULTS / "balance" / run)


if __name__ == "__main__":
    main()
