import json
import math
import sys
from pathlib import Path

import matplotlib.pyplot as plt


def nearest_rank(values, p):
    return sorted(values)[math.ceil(p * len(values)) - 1]


def summarize(data):
    pairs = data["latency_pairs"]
    percentiles = {"p50": 0.50, "p95": 0.95, "p99": 0.99}
    latency = {
        path: {name: nearest_rank([pair[f"{path}_us"] for pair in pairs], p) / 1000
               for name, p in percentiles.items()}
        for path in ("miss", "hit")
    }
    memory = {
        snapshot["phase"]: {name: sum(proc[name] for proc in snapshot["processes"])
                            for name in ("rss_kib", "pss_kib", "private_kib")}
        for snapshot in data["memory_phases"]
    }
    return {"pairs": len(pairs), "latency_ms": latency, "memory_kib": memory}


def draw_latency(ax, summary):
    ticks = range(3)
    for path, color, label, offset in (
        ("miss", "#B15026", "Function template miss", -0.2),
        ("hit", "#166AA6", "Function template hit", 0.2),
    ):
        values = list(summary["latency_ms"][path].values())
        bars = ax.bar([i + offset for i in ticks], values, 0.36, label=label, color=color)
        ax.bar_label(bars, fmt="%.2f", padding=3, fontsize=9)
    ax.set_xticks(ticks, ["p50", "p95", "p99"])
    ax.set_title(f"Local preparation latency · {summary['pairs']} paired samples", loc="left")
    ax.set_ylabel("Milliseconds")
    ax.set_ylim(0, ax.get_ylim()[1] * 1.35)
    ax.legend(loc="upper left", frameon=False)


def draw_memory(ax, phases, summary):
    ticks = list(range(len(phases)))
    totals = [0.0] * len(phases)
    for role, color in (("Host", "#91A0AD"), ("Base-Zygote", "#5663A6"),
                        ("Function-Zygote", "#267659"), ("Worker", "#2293B4")):
        values = [sum(proc["pss_kib"] for proc in phase["processes"] if proc["role"] == role) / 1024
                  for phase in phases]
        ax.bar(ticks, values, 0.62, bottom=totals, label=role, color=color)
        totals = [total + value for total, value in zip(totals, values)]
    for i, pss in enumerate(totals):
        ax.annotate(f"{pss:.2f}", (i, pss), xytext=(0, 4), textcoords="offset points",
                    ha="center", fontsize=9)
    ax.set_xticks(ticks, ["Base only", "Template\nidle", "+1 parked\nWorker", "+2 parked\nWorkers", "After both\nWorkers exit"])
    ax.set_title("Proportional memory by process role", loc="left")
    ax.set_ylabel("PSS (MiB)")
    ax.set_ylim(0, max(item["pss_kib"] for item in summary["memory_kib"].values()) / 1024 * 1.40)
    ax.legend(loc="upper left", ncols=2, frameon=False)


def main():
    directory = Path(sys.argv[1])
    data = json.loads((directory / "measurements.json").read_text(encoding="utf-8-sig"))
    summary = summarize(data)
    (directory / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    fig, axes = plt.subplots(1, 2, figsize=(12, 5), layout="constrained")
    fig.suptitle("FluxForge Zygote · 8 MiB function fixture", fontsize=17, fontweight="bold")
    draw_latency(axes[0], summary)
    draw_memory(axes[1], data["memory_phases"], summary)
    for ax in axes:
        ax.spines[["right", "top"]].set_visible(False)
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#E1E6EA", linewidth=0.8)
    fig.savefig(directory / "zygote-bars.png", dpi=180)
    fig.savefig(directory / "zygote-bars.svg")
    plt.close(fig)


if __name__ == "__main__":
    main()
