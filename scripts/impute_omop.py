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
    parser.add_argument('--sigma', default=0, const=3, nargs='?',
                        help='Run focuses only on n sigma of distribution')
    parser.add_argument('--plot', action='store_true',
                        help='Plot dist')
    parser.add_argument('--force', action='store_true',
                        help='Recalc all')

    return parser.parse_args()

def get_sigma_filtered_data(data, sigma_n, value_col='extracted::MEASUREMENT_VALUE'):
    """Filter data to within n standard deviations of the mean."""
    mean = data[value_col].mean()
    std = data[value_col].std()
    return data[
        (data[value_col] >= mean - sigma_n * std) & 
        (data[value_col] <= mean + sigma_n * std)
    ]


def plot_data(imp, og, df, output_file, stats, args):
    # Always use sigma filtering, default to 3 if not specified
    sigma_n = args.sigma if args.sigma else 3
    
    # Create three separate figures
    base_filename = output_file.rsplit('.', 1)[0]
    
    # Original plot (all data)
    res = list(compare_dist(imp,og)) + stats
    plot_single_figure(imp, og, df, f"{base_filename}_full.png", res, args, "Full Distribution")
    
    # Filter original data by sigma
    og_filtered = get_sigma_filtered_data(og, sigma_n)
    # Filter extracted data by its own distribution
    imp_filtered = get_sigma_filtered_data(imp, sigma_n)
    # Merge filtered data
    df_filtered = pd.concat([og_filtered, imp_filtered])
    
    # Calculate new statistics for filtered data
    filtered_res = list(compare_dist(imp_filtered, og_filtered)) + stats 
    
    # Plot with both distributions filtered by their own sigma
    plot_single_figure(imp_filtered, og_filtered, df_filtered, 
                     f"{base_filename}_filtered_{sigma_n}_sigma.png", filtered_res, args,
                     f"{sigma_n}-sigma Filtering (Separate)")
    

def plot_single_figure(imp, og, df, output_file, res, args, title_suffix):
    """Create a single figure with two subplots."""
    fig = plt.figure(figsize=(12, 10))
    gs = fig.add_gridspec(2, 1, height_ratios=[3, 2])
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1])
    
    # First subplot: Scatter plot
    ax1.scatter(og['extracted::MEASUREMENT_VALUE'], og['EVENT_AGE'], 
               alpha=0.5, label='Original Values', color='blue')
    ax1.scatter(imp['extracted::MEASUREMENT_VALUE'], imp['EVENT_AGE'], 
               alpha=0.5, label='Extracted Values', color='red')
    
    ax1.set_ylabel('Event Age')
    ax1.set_xlabel('Measurement Value')
    ax1.set_title(f'Measurement Values by Event Age: Original vs Extracted (OMOP {args.omop})\n{title_suffix}')
    ax1.legend(loc='upper right')
    ax1.grid(True, alpha=0.3)
    
    # Create stats text for textbox
    imp_median, imp_std, og_median, og_std, ks, ks_mlogp, n_imp, frac_imp, n_dup = res

    stats_text = (
        f"Original Median (SD): {og_median} ({og_std})\n"
        f"Extracted Median (SD): {imp_median} ({imp_std})\n"
        f"Extracted Values (n): {n_imp}\n"
        f"% Extracted: {frac_imp}%\n"
        f"KS: {ks}\n"
        f"mlogp: {ks_mlogp}\n"
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
    kde_og = stats.gaussian_kde(og['extracted::MEASUREMENT_VALUE'].dropna())
    kde_imp = stats.gaussian_kde(imp['extracted::MEASUREMENT_VALUE'].dropna())
    kde_merged = stats.gaussian_kde(df['extracted::MEASUREMENT_VALUE'].dropna())
    x_min = min(df['extracted::MEASUREMENT_VALUE'].min(), df['harmonization_omop::MEASUREMENT_VALUE'].min())
    x_max = max(df['extracted::MEASUREMENT_VALUE'].max(), df['harmonization_omop::MEASUREMENT_VALUE'].max())
    x_eval = np.linspace(x_min, x_max, 200)
    
    ax2.plot(x_eval, kde_og(x_eval), color='blue', label='Original Values')
    ax2.plot(x_eval, kde_imp(x_eval), color='red', label='Extracted Values')
    ax2.plot(x_eval, kde_merged(x_eval), color='green', label='Merged Data', linestyle='--')
    ax2.set_xlabel('Measurement Value')
    ax2.set_ylabel('Density')
    ax2.set_title(f'Density Distribution of Measurement Values\n{title_suffix}')
    ax2.legend(loc='upper right')
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close()




def read_data(args):
    # Read the data
    # Split between original values and extracted ones
    unique_dir = os.path.join(args.data_dir, f"{args.omop}_dist_unique.tsv.gz")
    dup_dir = os.path.join(args.data_dir, f"{args.omop}_dist_duplicates.tsv.gz")

    # check if files exist or if force flag is passed
    if not all([os.path.isfile(unique_dir),os.path.isfile(dup_dir)]) or args.force:
        #print("data missing, extracting unique/dups")
        df = pd.read_csv(
            os.path.join(args.data_dir, f"{args.omop}.tsv.gz"),
            sep='\t',
            usecols=['FINNGENID','EVENT_AGE','APPROX_EVENT_DATETIME','cleaned::TEST_NAME_ABBREVIATION', 'harmonization_omop::MEASUREMENT_VALUE', 'extracted::MEASUREMENT_VALUE','extracted::IS_MEASUREMENT_EXTRACTED']
        )

        # keep=False makes sure i keep all copies so i can just filter out the extracted one
        dup_mask =df[['FINNGENID','APPROX_EVENT_DATETIME','cleaned::TEST_NAME_ABBREVIATION','extracted::MEASUREMENT_VALUE']].duplicated(keep=False)
        extracted_mask = (df['extracted::IS_MEASUREMENT_EXTRACTED'].astype(bool).astype(int)==1)

        # dump all duplicates to begin with
        df[dup_mask].to_csv(dup_dir,sep='\t',na_rep="NA",index = False)
        
        # combine masks to get how many extracted values are unique
        final_mask = (dup_mask) & (extracted_mask)
        df = df[~final_mask]
        df.to_csv(unique_dir,sep='\t',na_rep="NA",index=False)
    
    # READ in duplicates
    dups =pd.read_csv(dup_dir,sep='\t')
    # count how many duplicates are extracted
    n_dup = ((dups['harmonization_omop::MEASUREMENT_VALUE'].isna()) & (~dups['extracted::MEASUREMENT_VALUE'].isna())).sum()
    
    # read in unique data
    df = pd.read_csv(unique_dir,sep='\t',index_col=None,nrows=10000 if args.test else None,)

    # reapply imputation mask to split between extracted and original. I can't use the previous ones because I need a less stringent duplica
    extracted_mask = (df['extracted::IS_MEASUREMENT_EXTRACTED'].astype(bool).astype(int)==1)
    imp = df[extracted_mask]
    og = df[~extracted_mask]
    
    return imp,og,df,n_dup


def check_pval(pval):
    mlgop = "NA"
    mlogp = np.inf if pval ==0  else  round(-np.log10(pval),2)
    return mlogp


def compare_dist(imp,og):

    col = ['extracted::MEASUREMENT_VALUE']
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
    i,o = imp['extracted::MEASUREMENT_VALUE'].dropna(),og['extracted::MEASUREMENT_VALUE'].dropna()
    stats =  [len(i),round(100*len(i)/(len(i)+len(o)),2),n_dup]
    print("\t".join(map(str,[args.omop] +list(res)   +  stats )))

    # plot
    if args.plot:
        # Save the figure
        fig_file = os.path.join(args.out_dir, f'age_measurement_comparison_{args.omop}.png')
        plot_data(imp,og,df,fig_file, stats,args)
        #print(f"Plot saved to: {fig_file}")

    
if __name__ == "__main__":
    main()

