import csv
import matplotlib.pyplot as plt
import numpy as np
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

COLOR_PDC = "#5C9BD6"
COLOR_HDF5 = "#EF5350"
COLOR_PCT = "#2ca02c"
BAR_WIDTH = 0.35
HDF5_SHIFT = -0.175
PDC_SHIFT = 0.175
LABEL_PAD = 0.005

rows = list(csv.DictReader(open(f"{BASE}/dlio.csv", newline="")))
node_counts = [int(r["node_count"]) for r in rows]
hdf5_gb = np.array([float(r["hdf5_gbs"]) for r in rows])
hdf5_e = np.array([float(r["hdf5_err_gbs"]) for r in rows])
pdc_gb = np.array([float(r["pdc_gbs"]) for r in rows])
pdc_e = np.array([float(r["pdc_err_gbs"]) for r in rows])
pct_lift = [float(r["pct_lift"]) for r in rows]

n = len(node_counts)
x = np.arange(1, n + 1)

fig, ax = plt.subplots(figsize=(16, 10))

ax.bar(x + HDF5_SHIFT, hdf5_gb, BAR_WIDTH, color=COLOR_HDF5, edgecolor=COLOR_HDF5, linewidth=0.5, label="HDF5", zorder=3)
ax.bar(x + PDC_SHIFT, pdc_gb, BAR_WIDTH, color=COLOR_PDC, edgecolor=COLOR_PDC, linewidth=0.5, label="PDC", zorder=3)
ax.bar(x + HDF5_SHIFT, hdf5_gb, BAR_WIDTH, color='none', edgecolor='black', linewidth=2.0, zorder=4)
ax.bar(x + PDC_SHIFT, pdc_gb, BAR_WIDTH, color='none', edgecolor='black', linewidth=2.0, zorder=4)

ax.errorbar(x + HDF5_SHIFT, hdf5_gb, yerr=hdf5_e, fmt="none", ecolor="black", elinewidth=2, capsize=0, zorder=5)
ax.errorbar(x + PDC_SHIFT, pdc_gb, yerr=pdc_e, fmt="none", ecolor="black", elinewidth=2, capsize=0, zorder=5)

for i in range(n):
    label_y = pdc_gb[i] + pdc_e[i] + LABEL_PAD
    ax.text(x[i] + PDC_SHIFT, label_y, f"+{pct_lift[i]:.0f}\\%", ha="center", va="bottom",
            fontsize=22, color=COLOR_PCT, zorder=6)

ax.set_xlabel("Number of GPU Processes", fontsize=28)
ax.set_ylabel("GB/sec", fontsize=28)
ax.set_xlim(0.3, n + 0.7)
ax.set_ylim(bottom=0)
ax.set_xticks(x)
ax.set_xticklabels([str(nc) for nc in node_counts], fontsize=24)
ax.tick_params(axis="y", labelsize=24)
ax.tick_params(axis="y", which="minor", left=False)
ax.grid(axis="y", which="major", linewidth=1.5, color="black", alpha=0.25)
ax.spines["top"].set_visible(False)
ax.spines["right"].set_visible(False)

ax.legend(loc="upper center", bbox_to_anchor=(0.5, 1.12), ncol=2, fontsize=22, frameon=True,
          edgecolor="#aaaaaa", columnspacing=1.0)

plt.tight_layout()
fig.savefig(f"{BASE}/dlio.pdf", dpi=150, bbox_inches="tight")
