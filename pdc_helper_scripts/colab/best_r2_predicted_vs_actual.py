import csv
import numpy as np
import matplotlib.pyplot as plt
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

rows = list(csv.DictReader(open(f"{BASE}/best_r2_predicted_vs_actual.csv", newline="")))
y = np.array([float(r["actual_ms"]) for r in rows])
y_hat = np.array([float(r["predicted_ms"]) for r in rows])

fig, ax = plt.subplots(figsize=(6, 6))
ax.scatter(y, y_hat, s=50, alpha=0.5, color='steelblue')
lims = [min(y.min(), y_hat.min()), max(y.max(), y_hat.max())]

ax.set_aspect('equal')

magnitude = lims[1] - lims[0]
step = 50 if magnitude > 50 else 25

tick_min = np.ceil(lims[0] / step) * step
tick_max = np.ceil(lims[1] / step) * step

ticks = np.arange(tick_min, tick_max + step, step)
ax.set_xticks(ticks)
ax.set_yticks(ticks)

ax.set_xlim(tick_min, tick_max)
ax.set_ylim(tick_min, tick_max)

ax.plot([tick_min, tick_max], [tick_min, tick_max], 'r--', linewidth=4, label='Perfect Prediction')

ax.xaxis.set_major_formatter(plt.FormatStrFormatter('%.0f'))
ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.0f'))

ax.set_xlabel("Actual Time (ms)", fontsize=28)
ax.set_ylabel("Predicted Time (ms)", fontsize=28)
ax.tick_params(axis='both', labelsize=22)
ax.tick_params(axis='y', which='minor', left=False)
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.grid(axis='y', which='major', linewidth=1.5, color='black', alpha=0.25)
plt.tight_layout()
plt.savefig(f"{BASE}/best_r2_predicted_vs_actual.pdf", dpi=150, bbox_inches='tight')
plt.close(fig)
