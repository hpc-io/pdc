import csv
import numpy as np
import matplotlib.pyplot as plt
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

STATIC_COLOR = "#d62728"
DYNAMIC_COLOR = "#1f77b4"
SPEEDUP_COLOR = "#2ca02c"

with open(f"{BASE}/total_time_vpicio.csv", newline="") as f:
    rows = list(csv.DictReader(f))

ranks = [int(r["n_ranks"]) for r in rows]
static_s = [float(r["static_s"]) for r in rows]
dynamic_s = [float(r["dynamic_s"]) for r in rows]
speedup_pct = [float(r["speedup_pct"]) for r in rows]

x = np.arange(len(ranks))

fig, ax = plt.subplots(figsize=(8.0, 4.0), constrained_layout=True)
fig.set_facecolor("#fcfcfb")
ax.set_facecolor("#fcfcfb")

ax.plot(x, static_s, marker="o", markersize=7, linewidth=1.8, color=STATIC_COLOR, label="Static", zorder=3)
ax.plot(x, dynamic_s, marker="s", markersize=7, linewidth=1.8, color=DYNAMIC_COLOR, label="Dynamic", zorder=3)

y_max = max(static_s)
for xi, s, d, pct in zip(x, static_s, dynamic_s, speedup_pct):
    ax.text(xi, max(s, d) + y_max * 0.025, f"-{pct:.0f}\\%", ha="center", va="bottom",
            fontsize=11, fontweight="bold", color=SPEEDUP_COLOR, clip_on=False)

ax.set_xticks(x)
ax.set_xticklabels([str(r) for r in ranks], fontweight="bold")
ax.set_xlabel("Number of Processes", fontsize=15, labelpad=8)
ax.set_ylabel("Total Transformation Time (s)", fontsize=15)
ax.tick_params(axis="y", labelsize=13)
ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)
ax.set_ylim(0, y_max * 1.15)

ax.legend(loc="upper left", fontsize=16, handlelength=2.5)

fig.savefig(f"{BASE}/total_time_vpicio.pdf", dpi=200, bbox_inches="tight", pad_inches=0.05)
