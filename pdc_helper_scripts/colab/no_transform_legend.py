import csv
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from google.colab import drive

drive.mount('/content/drive')

plt.rcParams["text.usetex"] = True
plt.rcParams["ps.useafm"] = True
plt.rcParams["pdf.use14corefonts"] = True

BASE = "/content/drive/MyDrive/data-flyway-eval-plots/colab"

handles = []
with open(f"{BASE}/no_transform_legend.csv", newline="") as f:
    for row in csv.DictReader(f):
        handles.append(mpatches.Patch(facecolor=row["facecolor"], edgecolor=row["edgecolor"],
                                       hatch=row["hatch"] or None, label=row["label"]))

fig_leg, ax_leg = plt.subplots(figsize=(8, 1))
ax_leg.axis('off')
ax_leg.legend(handles=handles, loc='center', ncol=4, fontsize=18, frameon=True,
              edgecolor='#aaaaaa', columnspacing=1.0)
plt.tight_layout()
fig_leg.savefig(f"{BASE}/no_transform_legend.pdf", dpi=150, bbox_inches='tight')
plt.close(fig_leg)
