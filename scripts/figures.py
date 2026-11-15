"""Plot four figures from recorded observations (matplotlib, no synthetic data)."""
from collections import Counter

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from analyze import read, stream
from cluster import RESULTS

OUT = RESULTS / "figures"
COLORS = {"blue":"#2563eb", "orange":"#d45328", "green":"#197554", "purple":"#793ab9", "grey":"#6b7280"}
plt.rcParams.update({"figure.dpi":110, "savefig.dpi":160, "font.family":"DejaVu Sans",
                     "axes.spines.top":False, "axes.spines.right":False,
                     "axes.grid":True, "grid.alpha":.18, "axes.axisbelow":True})


def save(fig, file):
    fig.savefig(OUT / file, facecolor="white", bbox_inches="tight")
    plt.close(fig)


def rollout():
    root = RESULTS/"rollout"
    fig, axes = plt.subplots(2, 1, figsize=(10,6.8), layout="constrained", sharex=True)
    for ax, phase, title in zip(axes, ("broken","fixed"), ("Исходная проверка HTTP", "Проверка после прогрева")):
        folder = root/phase
        timeline = stream(folder/"timeline.jsonl")
        update = next(r["unix"] for r in stream(folder/"events.jsonl") if r["event"] == "upgrade_A_to_B")
        times = [r["unix"]-update for r in timeline]
        b_pods = [{p["pod"] for p in r["pods"] if p["requested_version"] == "B"} for r in timeline]
        ready = [sum(p["ready"] for p in r["pods"] if p["pod"] in b) for r,b in zip(timeline,b_pods)]
        eps = [sum(e["ready"] for e in r["endpoints"] if e["pod"] in b) for r,b in zip(timeline,b_pods)]
        a_eps = [sum(e["ready"] for e in r["endpoints"] if e["pod"] not in b) for r,b in zip(timeline,b_pods)]
        ax.step(times, a_eps, where="post", label="A в EndpointSlice (ready)", color=COLORS["grey"])
        ax.step(times, ready, where="post", label="B Pod Ready", color=COLORS["blue"], linewidth=2)
        ax.step(times, eps, where="post", label="B в EndpointSlice (ready)", color=COLORS["green"], linestyle="--")
        failed = [r["sent_unix"]-update for r in stream(folder/"requests.jsonl") if r["status"] == 503]
        if failed:
            ax.scatter(failed, [.15]*len(failed), marker="x", s=24, color=COLORS["orange"], label="HTTP 503")
        if phase == "fixed":
            for i,r in enumerate(read(folder/"long-requests.json")):
                ax.plot([r["sent_unix"]-update,r["end_unix"]-update], [.1+i*.12]*2,
                        color=COLORS["purple"], lw=3, alpha=.8, label="Длительный поток A" if i==0 else None)
        ax.axvline(0, color="#111827", linewidth=1, linestyle=":")
        ax.set(title=title, ylabel="Реплики, шт.", yticks=[0,1,2], ylim=(-.13,2.45))
        ax.legend(loc="upper left", ncol=2, fontsize=8)
    axes[-1].set_xlabel("Время от команды обновления A → B, с")
    fig.suptitle("Опыт A. Прогрев 20 с, Service без привязки, 2 запроса/с")
    save(fig, "01-rollout.png")


def replicas(data):
    skew = [run for run in data if run["schedule"]["traffic"]=="skew"]
    hot = max((run for run in skew if run["schedule"]["policy"]=="sticky"),
              key=lambda r:max(row["planned"] for row in r["pod"].values()))
    index = hot["schedule"]["run"].rsplit("-",1)[-1]
    plain = next(r for r in skew if r["schedule"]["run"] == f"plain-skew-{index}")
    fig, axes = plt.subplots(1, 2, figsize=(11,4.8), layout="constrained")
    labels, totals, rejected, queue, ttfc = [],[],[],[],[]
    for run in (hot, plain):
        policy = "ClientIP" if run is hot else "Без привязки"
        for name,r in sorted(run["pod"].items(), key=lambda item:-item[1]["planned"]):
            labels.append(policy+"\n"+name.rsplit("-",1)[-1])
            totals.append(r["successful"])
            rejected.append(r["rejected"])
            queue.append(r["queue"]["p95"] if r["queue"]["p95"] is not None else float("nan"))
            ttfc.append(r["ttfc"]["p95"] if r["ttfc"]["p95"] is not None else float("nan"))
    xs = list(range(len(labels)))
    axes[0].bar(xs, totals, label="Завершено", color=COLORS["blue"])
    axes[0].bar(xs, rejected, bottom=totals, label="429 (очередь полна)", color=COLORS["orange"])
    axes[0].set(ylabel="Запросы, шт.", title="Распределение запросов")
    for i, (count, lost) in enumerate(zip(totals, rejected)):
        axes[0].text(i, count+lost, str(count+lost), ha="center", va="bottom", fontsize=8)
    axes[1].bar([x-.19 for x in xs], queue, width=.38, label="Очередь p95", color=COLORS["purple"])
    axes[1].bar([x+.19 for x in xs], ttfc, width=.38, label="TTFC p95", color=COLORS["green"])
    axes[1].set(ylabel="Задержка, с", title="Задержки успешных ответов")
    for ax in axes:
        ax.set_xticks(xs, labels=labels)
        ax.legend(fontsize=8)
    fig.suptitle(f"Опыт B. Перекос 80%, прогон {index}, 120 с, {hot['schedule']['rate']:g} запроса/с")
    save(fig, "02-replicas.png")


def policies(data):
    fig, axes = plt.subplots(1, 2, figsize=(11,4.7), layout="constrained")
    for i, traffic in enumerate(("uniform","skew")):
        for j, policy in enumerate(("plain","sticky")):
            trials = [r for r in data if r["schedule"]["policy"]==policy and r["schedule"]["traffic"]==traffic]
            trials.sort(key=lambda r:r["schedule"]["run"])
            x = i*3+j
            color = COLORS["blue" if policy=="plain" else "orange"]
            for trial, scatter in zip(trials,(-.12,0,.12)):
                r = trial["stats"]
                axes[0].scatter(x+scatter, r["ttfc"]["p95"], s=45, color=color)
                axes[1].scatter(x+scatter, r["errors"]*100/r["planned"], s=45, color=color)
    for ax in axes:
        ax.set_xticks([0,1,3,4], ["Обычный\nравномерно","ClientIP\nравномерно","Обычный\n80%","ClientIP\n80%"])
    axes[0].set(ylabel="TTFC p95, с", title="Успешные ответы")
    axes[1].set(ylabel="Доля ошибок, %", title="Все запланированные запросы")
    fig.suptitle("Опыт B. Каждая точка — отдельный двухминутный прогон")
    save(fig, "03-policies.png")


def autoscale(hpa):
    folder = RESULTS/"autoscale"
    start, low = hpa["high_start"], hpa["low_start"]
    data = stream(folder/"timeline.jsonl")
    rps = Counter(int(r["sent_unix"]-start) for phase in ("high","low")
                  for r in stream(folder/phase/"requests.jsonl") if r["sent_unix"])
    ts = [r["unix"]-start for r in data]
    xs = list(range(int(ts[0]), int(ts[-1])+1))
    ready = [sum(p["ready"] and not p["deleting"] for p in r["pods"]) for r in data]
    desired = [r["hpa"].get("desiredReplicas") for r in data]
    cpu = [next((m["resource"]["current"].get("averageUtilization") for m in r["hpa"].get("currentMetrics", [])
                 if m.get("resource",{}).get("name")=="cpu"), None) for r in data]
    fig, axes = plt.subplots(3, 1, figsize=(10,7.2), sharex=True, layout="constrained")
    axes[0].step(ts, desired, label="HPA desired", where="post", color=COLORS["purple"])
    axes[0].step(ts, ready, label="Ready", where="post", color=COLORS["green"], linestyle="--")
    axes[0].set(ylabel="Реплики, шт.", yticks=[1,2,3])
    axes[0].legend(loc="lower right", ncol=2)
    axes[1].plot(ts, cpu, color=COLORS["orange"], label="CPU по HPA")
    axes[1].axhline(50, color=COLORS["grey"], linestyle=":", label="Цель 50% от request")
    axes[1].set(ylabel="CPU, % request")
    axes[1].legend(loc="upper right")
    axes[2].fill_between(xs, [rps[x] for x in xs], color=COLORS["blue"], step="post", alpha=.8)
    axes[2].set(ylabel="Отправлено, запр./с", xlabel="Время от начала высокой нагрузки, с")
    for ax in axes:
        ax.axvline(low-start, color="#111827", linestyle=":", linewidth=1)
    fig.suptitle("Опыт C. CPU, число реплик и фактическая нагрузка ClientIP")
    save(fig, "04-hpa.png")


def main():
    OUT.mkdir(exist_ok=True)
    summary = read(RESULTS/"analysis.json")
    if "fixed" in summary["rollout"] and "broken" in summary["rollout"]:
        rollout()
    bal = summary["balance"]
    if any(r["schedule"]["policy"]=="sticky" and r["schedule"]["traffic"]=="skew" for r in bal) and len(bal)>=4:
        replicas(bal)
        policies(bal)
    if summary["autoscale"]:
        autoscale(summary["autoscale"])


if __name__ == "__main__":
    main()
