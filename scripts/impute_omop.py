#!/usr/bin/env python3
import pandas as pd
import os
import matplotlib.pyplot as plt
import argparse
from scipy import stats
import numpy as np
import scipy
import statsmodels.api as sm
import seaborn as sns
sns.set(palette='Set2')

def parse_args():
    parser = argparse.ArgumentParser(description='Create age vs measurement value plot for OMOP data')
    parser.add_argument('--omop', type=int, required=True,
                      help='OMOP identifier')
    parser.add_argument('--data-dir', type=str, default = '/mnt/disks/data/kanta/analysis/omop',
                      help='Directory containing the input data')
    parser.add_argument('--out-dir', type=str, default = "/mnt/disks/data/kanta/analysis/figs/",
                      help='Directory for output files')
    parser.add_argument('--test', action='store_true',
                        help='Run in test mode with only 10000 rows')
    parser.add_argument('--plot', action='store_true',
                        help='Plot dist')
    parser.add_argument('--force', action='store_true',
                        help='Recalc all')

    return parser.parse_args()

def plot_data(imp, og, df, output_file, res, args):
    # Create figure with two subplots
    fig = plt.figure(figsize=(12, 10))
    gs = fig.add_gridspec(2, 1, height_ratios=[3, 2])
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])
    
    # First subplot: Scatter plot
    ax1.scatter(og['imputed::MEASUREMENT_VALUE'], og['EVENT_AGE'], 
               alpha=0.5, label='Original Values', color='blue')
    ax1.scatter(imp['imputed::MEASUREMENT_VALUE'], imp['EVENT_AGE'], 
               alpha=0.5, label='Imputed Values', color='red')
    
    ax1.set_ylabel('Event Age')
    ax1.set_xlabel('Measurement Value')
    ax1.set_title(f'Measurement Values by Event Age: Original vs Imputed (OMOP {args.omop})')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    
    # Create stats text for textbox
    imp_median, imp_std, og_median, og_std, ks, ks_mlogp, n_imp, frac_imp, n_dup = res
    
    stats_text = (
        f"Original Median (SD): {og_median} ({og_std})\n"
        f"Imputed Median (SD): {imp_median} ({imp_std})\n"
        f"Imputed Values (n): {n_imp}\n"
        f"% Imputed:{frac_imp}\n"
        f"KS:{ks}\n"
        f"mlogp:{ks_mlogp}\n"
        f"Number of duplicates: {n_dup}"
    )
    
    # Add text box to first subplot
    props = dict(boxstyle='round', facecolor='white', alpha=0.8)
    ax1.text(0.8, 0.2, stats_text,
             transform=ax1.transAxes,
             fontsize=7,
             verticalalignment='top',
             bbox=props)
    
    # Second subplot: Density plot
    kde_og = stats.gaussian_kde(og['imputed::MEASUREMENT_VALUE'].dropna())
    kde_imp = stats.gaussian_kde(imp['imputed::MEASUREMENT_VALUE'].dropna())
    x_min = min(df['imputed::MEASUREMENT_VALUE'].min(), df['harmonization_omop::MEASUREMENT_VALUE'].min())
    x_max = max(df['imputed::MEASUREMENT_VALUE'].max(), df['harmonization_omop::MEASUREMENT_VALUE'].max())
    x_eval = np.linspace(x_min, x_max, 200)
    
    ax2.plot(x_eval, kde_og(x_eval), color='blue', label='Original Values')
    ax2.plot(x_eval, kde_imp(x_eval), color='red', label='Imputed Values')
    
    ax2.set_xlabel('Measurement Value')
    ax2.set_ylabel('Density')
    ax2.set_title('Density Distribution of Measurement Values')
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()

def read_data(args):
    # Read the data
    # Split between original values and imputed ones
    unique_dir = os.path.join(args.data_dir, f"{args.omop}_unique.tsv.gz")
    dup_dir = os.path.join(args.data_dir, f"{args.omop}_duplicates.tsv.gz")

    # check if files exist or if force flag is passed
    if not all([os.path.isfile(unique_dir),os.path.isfile(dup_dir)]) or args.force:
        #print("data missing, extracting unique/dups")
        df = pd.read_csv(
            os.path.join(args.data_dir, f"{args.omop}.tsv.gz"),
            sep='\t',
            usecols=['FINNGENID','EVENT_AGE','APPROX_EVENT_DATETIME','cleaned::TEST_NAME_ABBREVIATION', 'harmonization_omop::MEASUREMENT_VALUE', 'imputed::MEASUREMENT_VALUE']
        )

        # keep=False makes sure i keep all copies so i can just filter out the imputed one
        dup_mask =df[['FINNGENID','APPROX_EVENT_DATETIME','cleaned::TEST_NAME_ABBREVIATION','imputed::MEASUREMENT_VALUE']].duplicated(keep=False)
        imputed_mask = (df['harmonization_omop::MEASUREMENT_VALUE'].isna()) & (~df['imputed::MEASUREMENT_VALUE'].isna())

        # dump all duplicates to begin with
        df[dup_mask].to_csv(dup_dir,sep='\t',na_rep="NA",index = False)
        
        # combine masks to get how many imputed values are unique
        final_mask = (dup_mask) & (imputed_mask)
        df = df[~final_mask]
        df.to_csv(unique_dir,sep='\t',na_rep="NA",index=False)
    
    # READ in duplicates
    dups =pd.read_csv(dup_dir,sep='\t')
    # count how many duplicates are imputed
    n_dup = ((dups['harmonization_omop::MEASUREMENT_VALUE'].isna()) & (~dups['imputed::MEASUREMENT_VALUE'].isna())).sum()
    
    # read in unique data
    df = pd.read_csv(unique_dir,sep='\t',index_col=None,nrows=10000 if args.test else None,)

    # reapply imputation mask to split between imputed and original. I can't use the previous ones because I need a less stringent duplica
    imputed_mask = (df['harmonization_omop::MEASUREMENT_VALUE'].isna()) & (~df['imputed::MEASUREMENT_VALUE'].isna())
    
    imp = df[imputed_mask]
    og = df[~imputed_mask]

    return imp,og,df,n_dup


def check_pval(pval):
    mlgop = "NA"
    if pval ==0 :
        mlogp = np.inf
    else:
        mlogp = round(-np.log10(pval),2)
    return mlogp


def compare_dist(imp,og):

    col = ['imputed::MEASUREMENT_VALUE']
    # Perform Kolmogorov-Smirnov test for normality
    imp_data,og_data=imp[col].dropna().to_numpy(),og[col].dropna().to_numpy()

    def format_number(value):
        return "{:.2E}".format(value) if value < 0.001 or value > 1000  else "{:.3f}".format(value)

    imp_median = format_number(np.median(imp_data))
    og_median = format_number(np.median(og_data))
    imp_std = format_number(imp_data.std())
    og_std = format_number(og_data.std())

    imp_median = format_number(np.median(imp_data))
    og_median = format_number(np.median(og_data))
    imp_std = format_number(imp_data.std())
    og_std = format_number(og_data.std())

    ks,pval = scipy.stats.ks_2samp(imp_data,og_data)
    ks_mlogp = check_pval(pval[0])

    return imp_median,imp_std,og_median,og_std,str(round(ks[0],2)),ks_mlogp


def main():
    # Parse command line arguments
    args = parse_args()
    # read data
    imp,og,df,n_dup = read_data(args)
    #KS
    res = compare_dist(imp,og)
    i,o = imp['imputed::MEASUREMENT_VALUE'].dropna(),og['imputed::MEASUREMENT_VALUE'].dropna()
    stats = list(res)   + [len(i),round(100*len(i)/(len(i)+len(o)),2),n_dup]
    print("\t".join(map(str,[args.omop] + stats)))

    # plot
    if args.plot:
        # Save the figure
        fig_file = os.path.join(args.out_dir, f'age_measurement_comparison_{args.omop}.png')
        plot_data(imp,og,df,fig_file,stats,args)
        #print(f"Plot saved to: {fig_file}")

    
if __name__ == "__main__":
    main()

