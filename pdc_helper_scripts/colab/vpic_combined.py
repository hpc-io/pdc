import csv
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

SERIES_ORDER = ["HDF5 Async + ZFP", "DF ZFP (CPU, 40s)", "DF ZFP (CPU, 100s)", "DF ZFP (GPU)", "DF ZFP + Encryption"]
SERIES_COLOR = {
    "HDF5 Async + ZFP": "#FF8F00",
    "DF ZFP (CPU, 40s)": "#66BB6A",
    "DF ZFP (CPU, 100s)": "#5C9BD6",
    "DF ZFP (GPU)": "#AB47BC",
    "DF ZFP + Encryption": "#EF5350",
}

data = {}
ranks_set = set()
with open(f"{BASE}/vpic_combined.csv", newline="") as f:
    for row in csv.DictReader(f):
        n = int(row["n_ranks"])
        data[(row["method"], n)] = (float(row["four_step_s"]), float(row["final_step_s"]))
        ranks_set.add(n)

all_ranks = sorted(ranks_set)
n_groups = len(all_ranks)
n_bars = len(SERIES_ORDER)
group_width = 0.84
slot_width = group_width / n_bars
bar_width = slot_width * 0.88
x = np.arange(n_groups)

fig, ax = plt.subplots(figsize=(max(9.0, 1.6 * n_groups), 6.2), constrained_layout=True)
fig.set_facecolor("#fcfcfb")
ax.set_facecolor("#fcfcfb")

for bi, method in enumerate(SERIES_ORDER):
    color = SERIES_COLOR[method]
    offset = (bi - (n_bars - 1) / 2) * slot_width
    four_step = np.array([data.get((method, n), (0.0, 0.0))[0] for n in all_ranks])
    final_step = np.array([data.get((method, n), (0.0, 0.0))[1] for n in all_ranks])
    ax.bar(x + offset, four_step, bar_width, color=color, edgecolor="black", linewidth=0.6, zorder=3)
    ax.bar(x + offset, final_step, bar_width, bottom=four_step, color=color, hatch="///",
           edgecolor="black", linewidth=0.6, zorder=3)

ax.set_xticks(x)
ax.set_xticklabels([str(r) for r in all_ranks], fontweight="bold")
ax.set_xlabel("Number of Processes", fontsize=15, labelpad=8)
ax.set_ylabel("Observed I/O Time (s)", fontsize=15)
ax.tick_params(axis="y", labelsize=13)
ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

series_handles = [mpatches.Patch(facecolor=SERIES_COLOR[m], edgecolor="black") for m in SERIES_ORDER]
leg1 = ax.legend(series_handles, SERIES_ORDER, loc="upper left", fontsize=12, handlelength=2, ncol=2)
ax.add_artist(leg1)

seg_handles = [
    mpatches.Patch(facecolor="white", edgecolor="black"),
    mpatches.Patch(facecolor="white", hatch="///", edgecolor="black"),
]
ax.legend(seg_handles, ["4-step", "Final-step"], loc="upper right", fontsize=12, handlelength=2)

fig.savefig(f"{BASE}/vpic_combined.pdf", dpi=200, bbox_inches="tight", pad_inches=0.05)
