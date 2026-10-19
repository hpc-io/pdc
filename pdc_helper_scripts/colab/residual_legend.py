import csv
import matplotlib.pyplot as plt
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

rows = list(csv.DictReader(open(f"{BASE}/residual_legend.csv", newline="")))
lines = [plt.Line2D([0], [0], color=r["color"], linewidth=float(r["linewidth"]),
                     linestyle=r["linestyle"], label=r["label"]) for r in rows]

fig_leg, ax_leg = plt.subplots(figsize=(4, 0.5))
ax_leg.axis('off')
ax_leg.legend(handles=lines, loc='center', ncol=1, fontsize=18, frameon=True, edgecolor='#aaaaaa')
plt.tight_layout()
fig_leg.savefig(f"{BASE}/residual_legend.pdf", dpi=150, bbox_inches='tight')
plt.close(fig_leg)
