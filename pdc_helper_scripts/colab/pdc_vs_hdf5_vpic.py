import csv
import numpy as np
import matplotlib.pyplot as plt
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

C_PDC = "#F44336"
C_HDF5 = "#2196F3"
BAR = 0.35
rank_order = [32, 64, 128, 256, 512, 1024, 2048, 4096]
rank_to_x = {r: i + 1 for i, r in enumerate(rank_order)}

data = {}
with open(f"{BASE}/pdc_vs_hdf5_vpic.csv", newline="") as f:
    for row in csv.DictReader(f):
        data.setdefault(row["method"], []).append((int(row["n_ranks"]), float(row["four_step_s"]), float(row["final_step_s"])))

def xi(rows, offset):
    return np.array([rank_to_x[r] + offset for r, _, _ in rows])

def draw_bar(ax, x, base_vals, out_vals, color):
    ax.bar(x, base_vals, width=BAR, color=color, edgecolor=color, linewidth=0.5, zorder=3)
    ax.bar(x, out_vals, bottom=base_vals, width=BAR, facecolor=color, edgecolor='black',
           linewidth=0.5, hatch="/", zorder=3)
    ax.bar(x, base_vals + out_vals, width=BAR, color='none', edgecolor='black', linewidth=2.0, zorder=4)

fig, ax = plt.subplots(figsize=(10, 6))

hdf5_rows = data["HDF5"]
pdc_rows = data["PDC"]
draw_bar(ax, xi(hdf5_rows, -BAR / 2), np.array([r[1] for r in hdf5_rows]), np.array([r[2] for r in hdf5_rows]), C_HDF5)
draw_bar(ax, xi(pdc_rows, BAR / 2), np.array([r[1] for r in pdc_rows]), np.array([r[2] for r in pdc_rows]), C_PDC)

ax.set_xlabel("Number of Processes", fontsize=32)
ax.set_ylabel("Observed I/O Time (s)", fontsize=32)
ax.set_xticks(range(1, len(rank_order) + 1))
ax.set_xticklabels([str(r) for r in rank_order], fontsize=24)
ax.tick_params(axis='y', labelsize=24)
ax.tick_params(axis='y', which='minor', left=False)
ax.set_xlim(0.3, len(rank_order) + 0.7)
ax.set_ylim(bottom=0)
ax.grid(axis='y', which='major', linewidth=1.5, color='black', alpha=0.25)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout()
plt.savefig(f"{BASE}/pdc_vs_hdf5_vpic.pdf", dpi=150, bbox_inches='tight')
plt.close()
