import pandas as pd
import numpy as np
import os,sys
import seaborn as sns
sns.set(palette='Set2')
import matplotlib as mpl
from matplotlib import pyplot as plt
import pandas as pd
import dash_bio

DATA=sys.argv[1]
LAB=sys.argv[2]
OUT='/mnt/disks/data/kanta/analysis/plots/'
print(DATA,LAB)

fig_path = os.path.join(OUT,f"{LAB}.png")

df = pd.read_csv(DATA,sep='\t')
df = df[df.LOG10P>2].reset_index()
print(df)
fig =dash_bio.ManhattanPlot(
    dataframe=df,
    chrm="CHROM",
    bp="GENPOS",
    p="LOG10P",
    snp="ID",
    title='LDL kanta',
    gene="ALLELE1",
    logp=False,
    ylabel='-log10(p)'
)

#df = pd.read_csv('https://raw.githubusercontent.com/plotly/dash-bio-docs-files/master/manhattan_data.csv')
#print(df)
#fig = dash_bio.ManhattanPlot(dataframe=df)
fig.write_image(file=fig_path, format='png')


