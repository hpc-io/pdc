import csv
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

METHOD_ORDER = ["ADIOS2", "PDC Post-hoc", "DF-Eager+ZFP", "DF-Eager"]
SEGMENT_ORDER = [
    "server write", "server compute curl", "server compute mag", "server compress",
    "server close", "analysis read", "analysis compute", "analysis write",
]
SEGMENT_COLOR = {
    "server write": "#2a78d6", "server close": "#e87ba4", "analysis read": "#eb6834",
    "analysis compute": "#1baf7a", "analysis write": "#eda100",
    "server compute curl": "#00897B", "server compute mag": "#43A047", "server compress": "#8E24AA",
}
SEGMENT_HATCH = {
    "server write": "....", "server close": "||||", "analysis read": "////",
    "analysis compute": "xxxx", "analysis write": "\\\\\\\\",
    "server compute curl": "----", "server compute mag": "oooo", "server compress": "++++",
}

data = {}
ranks_set = set()
with open(f"{BASE}/curl_vorticity_scaling_breakdown.csv", newline="") as f:
    for row in csv.DictReader(f):
        key = (row["method"], int(row["n_ranks"]))
        data.setdefault(key, {})[row["segment"]] = float(row["value_s"])
        ranks_set.add(int(row["n_ranks"]))

all_ranks = sorted(ranks_set)
n_groups = len(all_ranks)
n_bars = len(METHOD_ORDER)
group_width = 0.86
slot_width = group_width / n_bars
bar_width = slot_width * 0.84
x = np.arange(n_groups)

fig, ax = plt.subplots(figsize=(7.0, 3.6), constrained_layout=True)
fig.set_facecolor("#fcfcfb")
ax.set_facecolor("#fcfcfb")

bar_centers = {}
for bi, method in enumerate(METHOD_ORDER):
    offset = (bi - (n_bars - 1) / 2) * slot_width
    bottoms = np.zeros(n_groups)
    for seg in SEGMENT_ORDER:
        heights = np.array([data.get((method, n), {}).get(seg, 0.0) for n in all_ranks])
        if not np.any(heights > 0):
            continue
        ax.bar(x + offset, heights, bar_width, bottom=bottoms,
               color=SEGMENT_COLOR[seg], hatch=SEGMENT_HATCH[seg],
               edgecolor="black", linewidth=0.3, zorder=3)
        bottoms += heights
    for gi, n in enumerate(all_ranks):
        bar_centers[(method, gi)] = x[gi] + offset

for gi, n in enumerate(all_ranks):
    for m in METHOD_ORDER:
        ax.text(bar_centers[(m, gi)], -0.012, m,
                transform=matplotlib.transforms.offset_copy(ax.get_xaxis_transform(), fig=fig, x=1.0, units="points"),
                ha="center", va="top", fontsize=10, rotation=90, clip_on=False)

ax.tick_params(axis="x", length=0)
ax.set_xticks(x)
ax.set_xticklabels([])
for gi, n in enumerate(all_ranks):
    ax.text(x[gi], -0.46, str(n), transform=ax.get_xaxis_transform(), ha="center", va="top", fontsize=10, clip_on=False)
ax.text(0.5, -0.62, "Number of Processes", transform=ax.transAxes, ha="center", va="top", fontsize=11, clip_on=False)
ax.set_ylabel("Total Workload Time (s)", fontsize=15)
ax.tick_params(axis="y", labelsize=13)
ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

seg_handles = [mpatches.Patch(facecolor=SEGMENT_COLOR[s], hatch=SEGMENT_HATCH[s], edgecolor="black") for s in SEGMENT_ORDER]
ax.legend(seg_handles, SEGMENT_ORDER, loc="upper left", ncol=4, fontsize=9,
          handlelength=2, handleheight=1.2, frameon=True, edgecolor="#222222", fancybox=False)

ax.set_ylim(0, 33)

fig.savefig(f"{BASE}/curl_vorticity_scaling_breakdown.pdf", dpi=200, bbox_inches="tight", pad_inches=0.05)
