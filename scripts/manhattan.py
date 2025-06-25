import pandas as pd
import numpy as np
import os,sys
import seaborn as sns
sns.set(palette='Set2')
import matplotlib as mpl
from matplotlib import pyplot as plt
import pandas as pd
from qmplot import manhattanplot

DATA=sys.argv[1]
LAB=sys.argv[2]
OUT='/mnt/disks/data/kanta/analysis/gwas/plots/'
print(DATA,LAB)

fig_path= os.path.join(OUT,f"{LAB}.png")

pfile = os.path.join(OUT,f"{LAB}.pkl")
if os.path.isfile(pfile):
    print(pfile)
    df = pd.read_pickle(pfile)
else:
    print(DATA)
    df = pd.read_csv(DATA,sep=' ',usecols=['CHROM','GENPOS','ID','LOG10P'])
    df.to_pickle(pfile)

#df = df.sample(100000, replace=False).sort_index()
print(df)
f, ax = plt.subplots(figsize=(12, 4), facecolor='w', edgecolor='k')
manhattanplot(data=df,
              marker=".",
              sign_marker_color="r",
              chrom='CHROM',
              pos="GENPOS",
              pv="LOG10P",
              logp=False,
              snp="ID",
              xlabel="Chromosome",
              ylabel=r"$-log_{10}{(P)}$",
              sign_line_cols=["#D62728", "#2CA02C"],
              hline_kws={"linestyle": "--", "lw": 1.3},
              is_annotate_topsnp=True,
              ld_block_size=500000,  # 500000 bp
              text_kws={"fontsize": 12,"arrowprops": dict(arrowstyle="-", color="k", alpha=0.6)},
              ax=ax
              )


plt.xticks(fontsize=6)
plt.savefig(fig_path)


