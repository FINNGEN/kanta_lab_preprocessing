import pandas as pd
import numpy as np
import os
import seaborn as sns
sns.set(palette='Set2')
from verkko.binner import binner
import matplotlib as mpl
from matplotlib import pyplot as plt


data_path = '/mnt/disks/data/kanta/results/osuus/'
f = os.path.join(data_path,'data.txt')

df = pd.read_csv(f,sep='\t')
print(df)
df = df[df['MEASUREMENT_VALUE'].notna()]



counts= {elem:value for elem,value in  df.TEST_NAME_ABBREVIATION.dropna().value_counts().to_dict().items() if value > 1000}



out_fig = os.path.join(data_path,'osuus.pdf')
fig = plt.figure()
n_tests = len(counts)
gs = mpl.gridspec.GridSpec(1,n_tests)

for i,test in enumerate(counts):
    ax = fig.add_subplot(gs[0,i])
    print(test,counts[test])
    save_test = test.replace('/','-')
    bin_path = os.path.join(data_path,f"{save_test}_bin.npy")
    plot_path = os.path.join(data_path,f"{save_test}_data.npy")
    
    if not os.path.isfile(bin_path) or not os.path.isfile(plot_path):
        mask = df['TEST_NAME_ABBREVIATION'] == test
        test_df = df[mask]
        values = test_df['MEASUREMENT_VALUE']
        print(min(values),max(values))
        n_bins = 30 if len(values) > 1000 else 10
        bins = binner.Bins(float,min(values),max(values),'lin',n_bins)
        countNotNormalized = bins.bin_count_divide(values)
        count = np.array(binner.normalize(list(countNotNormalized)))
        binAvg = bins.bin_average(zip(values,values))
        binMask = ~np.ma.getmask(binAvg)
        plot_data = count[binMask]
        bin_data = binAvg[binMask]
        bin_data.dump(bin_path)
        plot_data.dump(plot_path)
    else:
        bin_data = np.load(bin_path,allow_pickle = True)
        plot_data = np.load(plot_path,allow_pickle = True)

    print(plot_data.mean(),plot_data.std())
    ax.plot(bin_data,plot_data, '--')
    ax.set_title(test,fontsize=4)
    plt.xticks(rotation=45,fontsize = 4)
    plt.yticks(rotation = 45,fontsize = 4)

fig.savefig(out_fig)
fig.savefig(out_fig.replace('.pdf','.png'),dpi=300)
fig.tight_layout()

plt.close()
