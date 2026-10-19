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

METHOD_ORDER = [
    "HDF5", "ADIOS2",
    "PDC Post-hoc [2]", "PDC Post-hoc [4]", "PDC Post-hoc [8]",
    "DF-Eager [2]", "DF-Eager [4]", "DF-Eager [8]",
    "DF-View [2]", "DF-View [4]", "DF-View [8]",
]
SEGMENT_ORDER = ["write", "close", "analysis read", "analysis compute", "analysis write"]
SEGMENT_COLOR = {
    "write": "#2a78d6", "close": "#e87ba4", "analysis read": "#eb6834",
    "analysis compute": "#1baf7a", "analysis write": "#eda100",
}
SEGMENT_HATCH = {
    "write": "..", "close": "||", "analysis read": "//",
    "analysis compute": "xx", "analysis write": "\\\\",
}

data = {}
ranks_set = set()
with open(f"{BASE}/vector_magnitude_scaling.csv", newline="") as f:
    for row in csv.DictReader(f):
        key = (row["method"], int(row["n_ranks"]))
        data.setdefault(key, {})[row["segment"]] = float(row["value_s"])
        ranks_set.add(int(row["n_ranks"]))

all_ranks = sorted(ranks_set)
n_groups = len(all_ranks)
n_bars = len(METHOD_ORDER)
group_width = 0.92
slot_width = group_width / n_bars
bar_width = slot_width * 0.88
x = np.arange(n_groups)

fig, ax = plt.subplots(figsize=(max(9.0, 2.6 * n_groups), 6.0), constrained_layout=True)
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
               edgecolor="black", linewidth=0.6, zorder=3)
        bottoms += heights
    for gi, n in enumerate(all_ranks):
        bar_centers[(method, gi)] = x[gi] + offset

label_trans = matplotlib.transforms.blended_transform_factory(ax.transData, ax.transAxes)
for method in METHOD_ORDER:
    for gi, n in enumerate(all_ranks):
        if (method, n) not in data:
            continue
        ax.text(bar_centers[(method, gi)], -0.02, method, transform=label_trans,
                rotation=90, ha="center", va="top", fontsize=11, fontweight="bold",
                color="black", clip_on=False)

ax.set_xticks(x)
ax.set_xticklabels([str(n) for n in all_ranks], fontweight="bold")
ax.tick_params(axis="x", pad=110, labelsize=10.5, labelcolor="#111111")
for label in ax.get_xticklabels():
    label.set_transform(matplotlib.transforms.offset_copy(label.get_transform(), fig=fig, x=12, units="points"))

ax.set_xlabel("Number of Processes", fontsize=15, labelpad=8)
ax.set_ylabel("Total Workload Time (s)", fontsize=15)
ax.tick_params(axis="y", labelsize=13)
ax.yaxis.grid(True, which="major", linestyle="-", linewidth=0.8, color="#888888", alpha=0.7, zorder=0)
ax.set_axisbelow(True)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

edge_pad = slot_width * 0.6
ax.set_xlim(x[0] - group_width / 2 - edge_pad, x[-1] + group_width / 2 + edge_pad)
ax.set_ylim(0, 175)

seg_handles = [mpatches.Patch(facecolor=SEGMENT_COLOR[s], hatch=SEGMENT_HATCH[s], edgecolor="black") for s in SEGMENT_ORDER]
ax.legend(seg_handles, SEGMENT_ORDER, loc="upper left", fontsize=14, handlelength=3,
          handleheight=2.2, ncol=len(SEGMENT_ORDER))

fig.savefig(f"{BASE}/vector_magnitude_scaling.pdf", dpi=200, bbox_inches="tight")
