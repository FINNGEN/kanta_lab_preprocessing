import os
import sys
# FIX: Set writable config directory before imports to avoid Permission Denied
os.environ['MPLCONFIGDIR'] = '/tmp/matplotlib_config'

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import argparse
import duckdb
import random
from tqdm import tqdm

# Set a style for better visualization
sns.set_theme(style="whitegrid")

def format_scientific(val):
    if pd.isna(val) or isinstance(val, str): return "-"
    return f"{val:.2E}" if (abs(val) < 0.01 or abs(val) > 10000) and val != 0 else f"{val:.2f}"

def get_deciles(series):
    if series.empty or len(series) < 5:
        return "-"
    deciles = np.percentile(series, np.arange(10, 100, 10))
    return "[" + ",".join([f"{d:.1f}" for d in deciles]) + "]"

def sigma_filter(data: pd.Series, n_sigma: int = 3):
    """Returns (cleaned_series, n_removed)"""
    data = data.dropna()
    initial_count = len(data)
    if initial_count < 2:
        return data, 0
    mean, std = data.mean(), data.std()
    if std == 0 or pd.isna(std): 
        return data, 0
    
    mask = (data >= mean - n_sigma * std) & (data <= mean + n_sigma * std)
    cleaned = data[mask]
    n_removed = initial_count - len(cleaned)
    return cleaned, n_removed

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_parquet = os.path.expanduser("~/fg-3/kanta_v3/core/kanta_dev_2026_02_03_core.parquet")
    DEFAULT_IDS = [3020564, 3009542, 3003396, 3019900, 3014007]
    default_ref_path = os.path.abspath(os.path.join(script_dir, "..", "finngen_qc", "data", "LABfi_ALL.usagi.csv"))
    
    parser = argparse.ArgumentParser(description="OMOP Analysis: ID Selection from Parquet Data")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--ids', type=str, help="Comma-separated list of OMOP IDs")
    group.add_argument('--full', action='store_true', help="Process ALL unique IDs found in the Parquet file")
    group.add_argument('--random', type=int, nargs='?', const=10, help="Process N random IDs found in the Parquet file")

    parser.add_argument('--file_path', type=str, default=default_parquet, help="Input Parquet path")
    parser.add_argument('--ref_path', type=str, default=default_ref_path, help="Usagi CSV path")
    parser.add_argument('--output_dir', type=str, default='./plots', help="Output directory for PNGs")
    parser.add_argument('--name', type=str, default="analysis", help="Custom prefix for the output files")
    parser.add_argument('--test', type=int, nargs='?', const=10000, default=None, help="Limit rows per ID")
    
    args = parser.parse_args()

    if not os.path.exists(args.file_path):
        print(f"ERROR: Parquet file not found at: {args.file_path}")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs('/tmp/matplotlib_config', exist_ok=True)

    try:
        ref_df = pd.read_csv(args.ref_path).drop_duplicates(subset=['conceptId'])
        ref_df['conceptId'] = ref_df['conceptId'].astype(float).astype(int)
        ref_map = dict(zip(ref_df['conceptId'], ref_df['conceptName']))
    except Exception:
        ref_map = {}

    conn = duckdb.connect()

    # Determine IDs
    if args.full or args.random is not None:
        print(f"STATUS: Fetching unique OMOP IDs from {args.file_path}...")
        unique_query = f"SELECT DISTINCT OMOP_CONCEPT_ID FROM '{args.file_path}' WHERE OMOP_CONCEPT_ID IS NOT NULL"
        ids_in_parquet = conn.execute(unique_query).df()['OMOP_CONCEPT_ID'].astype(int).tolist()
        
        if args.full:
            omop_ids = sorted(ids_in_parquet)
            run_suffix = ""
        else:
            count = min(args.random, len(ids_in_parquet))
            omop_ids = random.sample(ids_in_parquet, count)
            run_suffix = f"_random_{count}"
    elif args.ids:
        omop_ids = [int(i.strip()) for i in args.ids.split(',')]
        run_suffix = ""
    else:
        omop_ids = DEFAULT_IDS
        run_suffix = ""

    if args.test:
        run_suffix += f"_test_{args.test}"

    final_base = f"{args.name}{run_suffix}"
    summary_file = f"{final_base}_summary.tsv"
    rejected_file = f"{final_base}_rejected.tsv"

    summary_results = []
    rejected_results = []
    limit_clause = f"LIMIT {args.test}" if args.test is not None else ""
    
    for omop_id in tqdm(omop_ids, desc=f"Analyzing {final_base}"):
        desc = ref_map.get(omop_id, "Unknown Concept")
        
        query = f"""
            SELECT MEASUREMENT_VALUE_EXTRACTED as ext, 
                   MEASUREMENT_VALUE_HARMONIZED as harm,
                   EVENT_AGE
            FROM '{args.file_path}'
            WHERE OMOP_CONCEPT_ID = {omop_id} 
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            {limit_clause}
        """
        
        try:
            df = conn.execute(query).fetchdf()
            if df.empty:
                rejected_results.append({"OMOP_ID": omop_id, "REASON": "EMPTY_OR_NON_NUMERIC", "Description": desc})
                continue
        except Exception:
            rejected_results.append({"OMOP_ID": omop_id, "REASON": "QUERY_ERROR", "Description": desc})
            continue

        ext_raw = df['ext'].dropna()
        harm_raw = df['harm'].dropna()

        ext_clean, n_ext_rem = sigma_filter(ext_raw)
        harm_clean, n_harm_rem = sigma_filter(harm_raw)

        if ext_clean.empty and harm_clean.empty:
            rejected_results.append({"OMOP_ID": omop_id, "REASON": "SIGMA_FILTERED_TO_EMPTY", "Description": desc})
            continue

        n_ext, n_harm = len(ext_clean), len(harm_clean)
        
        # Ratio of N extracted vs N harmonized reference
        n_ratio = round(n_ext / n_harm, 4) if n_harm > 0 else 0.0

        total_numeric = len(ext_raw) + len(harm_raw)
        total_removed = n_ext_rem + n_harm_rem
        sigma_reject_pct = round((total_removed / total_numeric) * 100, 2) if total_numeric > 0 else 0.0
        
        # --- KS Calculation with mlogp ---
        if n_ext > 0 and n_harm > 0:
            ks_stat, p_val = stats.ks_2samp(ext_clean, harm_clean)
            # Clip p_val to avoid log(0)
            mlogp = -np.log10(max(p_val, 1e-300))
        else:
            ks_stat, mlogp = np.nan, 0.0

        status_label = "SUCCESS" if (np.isnan(ks_stat) or ks_stat < 0.3) else "FAIL"

        summary_results.append({
            "RANK": 0, 
            "OMOP_ID": omop_id, 
            "STATUS": status_label,
            "EXT_MEDIAN": format_scientific(ext_clean.median()), 
            "HARM_REF_MEDIAN": format_scientific(harm_clean.median()), 
            "EXT_HARM_N_RATIO": n_ratio,
            "KS": round(ks_stat, 3) if not np.isnan(ks_stat) else 1.0, 
            "MLOGP": round(mlogp, 2),
            "N_EXTRACTED": n_ext, 
            "N_HARM_REF": n_harm,
            "SIGMA_REJECTED_PCT": sigma_reject_pct, 
            "Description": desc,
            "EXT_DECILES": get_deciles(ext_clean), 
            "HARM_REF_DECILES": get_deciles(harm_clean)
        })

        # --- Plotting ---
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
        
        if not ext_clean.empty:
            sns.scatterplot(x=df.loc[ext_clean.index, 'ext'], y=df.loc[ext_clean.index, 'EVENT_AGE'], 
                            color='red', alpha=0.3, label='Extracted', ax=ax1, s=25, rasterized=True)
            if ext_clean.nunique() > 1: sns.kdeplot(ext_clean, ax=ax2, color='red', label='Extracted PDF')
            else: ax2.axvline(ext_clean.iloc[0], color='red', linestyle='--')
        
        if not harm_clean.empty:
            sns.scatterplot(x=df.loc[harm_clean.index, 'harm'], y=df.loc[harm_clean.index, 'EVENT_AGE'], 
                            color='blue', alpha=0.3, label='Harmonized (Ref)', ax=ax1, s=25, rasterized=True)
            if harm_clean.nunique() > 1: sns.kdeplot(harm_clean, ax=ax2, color='blue', label='Harmonized PDF')
            else: ax2.axvline(harm_clean.iloc[0], color='blue', linestyle='--')
        
        ax1.legend(loc='upper right')
        title_str = (f"OMOP {omop_id}: {desc}\n"
                     f"N Ratio: {n_ratio} | KS: {round(ks_stat,3)} | -log10(p): {round(mlogp, 2)}")
        ax1.set_title(title_str)
        
        plot_path = os.path.join(args.output_dir, f"{final_base}_omop_{omop_id}.png")
        fig.savefig(plot_path, dpi=150, bbox_inches='tight')
        plt.close(fig)

    if summary_results:
        final_df = pd.DataFrame(summary_results).sort_values("N_EXTRACTED", ascending=False).reset_index(drop=True)
        final_df['RANK'] = final_df.index + 1
        final_df.to_csv(summary_file, sep='\t', index=False)
    
    pd.DataFrame(rejected_results).to_csv(rejected_file, sep='\t', index=False)
    print(f"\nDONE. Prefix: {final_base}")

if __name__ == "__main__":
    main()
