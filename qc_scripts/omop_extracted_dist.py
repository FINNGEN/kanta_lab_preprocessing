import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import argparse
import os
import duckdb
import random

# Set a style for better visualization
sns.set_theme(style="whitegrid")

def format_scientific(val):
    if pd.isna(val): return "-"
    return f"{val:.2E}" if (abs(val) < 0.01 or abs(val) > 10000) and val != 0 else f"{val:.2f}"

def get_deciles(series):
    if series.empty or len(series) < 5:
        return "-"
    deciles = np.percentile(series, np.arange(10, 100, 10))
    return "[" + ",".join([f"{d:.1f}" for d in deciles]) + "]"

def sigma_filter(data: pd.Series, n_sigma: int = 3) -> pd.Series:
    data = data.dropna()
    if data.empty:
        return pd.Series([], dtype=float)
    mean, std = data.mean(), data.std()
    if std == 0: return data
    return data[(data >= mean - n_sigma * std) & (data <= mean + n_sigma * std)]

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
    parser.add_argument('--test', type=int, nargs='?', const=10000, default=None, help="Limit rows per ID")
    
    args = parser.parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    # --- 1. Determine Suffix for Unique Filenames ---
    if args.full:
        run_suffix = "full"
    elif args.random is not None:
        run_suffix = f"random_{args.random}"
    elif args.ids:
        run_suffix = f"ids_{args.ids.replace(',', '_')[:30]}"
    else:
        run_suffix = "default"
    
    if args.test:
        run_suffix += f"_test_{args.test}"

    summary_file = f"analysis_summary_{run_suffix}.tsv"
    rejected_file = f"rejected_ids_{run_suffix}.tsv"

    # 2. Load Reference Metadata
    try:
        ref_df = pd.read_csv(args.ref_path).drop_duplicates(subset=['conceptId'])
        ref_df['conceptId'] = ref_df['conceptId'].astype(float).astype(int)
        ref_map = dict(zip(ref_df['conceptId'], ref_df['conceptName']))
    except Exception:
        ref_map = {}

    conn = duckdb.connect()

    # 3. Determine OMOP IDs
    if args.full or args.random is not None:
        print(f"STATUS: Fetching unique OMOP IDs from {args.file_path}...")
        unique_query = f"SELECT DISTINCT OMOP_CONCEPT_ID FROM '{args.file_path}' WHERE OMOP_CONCEPT_ID IS NOT NULL"
        ids_in_parquet = conn.execute(unique_query).df()['OMOP_CONCEPT_ID'].astype(int).tolist()
        
        if args.full:
            omop_ids = sorted(ids_in_parquet)
        else:
            count = min(args.random, len(ids_in_parquet))
            omop_ids = random.sample(ids_in_parquet, count)
    elif args.ids:
        omop_ids = [int(i.strip()) for i in args.ids.split(',')]
    else:
        omop_ids = DEFAULT_IDS

    print(f"STATUS: Mode [{run_suffix}] | Processing {len(omop_ids)} IDs.")

    summary_results = []
    rejected_results = []
    limit_clause = f"LIMIT {args.test}" if args.test is not None else ""
    total_ids = len(omop_ids)

    # 4. Processing Loop
    for idx, omop_id in enumerate(omop_ids, 1):
        desc = ref_map.get(omop_id, "Unknown Concept")
        print(f"STATUS: [{idx}/{total_ids}] Analyzing OMOP {omop_id}...", end=" ", flush=True)
        
        query = f"""
            SELECT MEASUREMENT_VALUE_EXTRACTED as ext, 
                   MEASUREMENT_VALUE_HARMONIZED as har,
                   EVENT_AGE
            FROM '{args.file_path}'
            WHERE OMOP_CONCEPT_ID = {omop_id} 
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            {limit_clause}
        """
        
        try:
            df = conn.execute(query).fetchdf()
            if df.empty:
                print("SKIPPED (No numeric data)")
                rejected_results.append({"OMOP_ID": omop_id, "REASON": "EMPTY_OR_NON_NUMERIC", "N_ROWS": 0, "Description": desc})
                continue
        except Exception:
            print("SKIPPED (Query Error)")
            rejected_results.append({"OMOP_ID": omop_id, "REASON": "QUERY_ERROR", "N_ROWS": 0, "Description": desc})
            continue

        ext_clean = sigma_filter(df['ext'].dropna())
        har_clean = sigma_filter(df['har'].dropna())

        if ext_clean.empty and har_clean.empty:
            print(f"SKIPPED (Filtered to 0 rows)")
            rejected_results.append({"OMOP_ID": omop_id, "REASON": "SIGMA_FILTERED_TO_EMPTY", "N_ROWS": len(df), "Description": desc})
            continue

        print("OK")

        # Ratio calculation
        n_ext = len(ext_clean)
        n_har = len(har_clean)
        ext_ratio = round(n_ext / n_har, 3) if n_har > 0 else 0.0

        ks_stat, p_val = stats.ks_2samp(ext_clean, har_clean) if not (ext_clean.empty or har_clean.empty) else (np.nan, 1.0)
        log_p = -np.log10(p_val) if (p_val > 0 and not np.isnan(p_val)) else (50.0 if ks_stat > 0 else 0.0)
        status_label = "SUCCESS" if ks_stat < 0.3 else "FAIL"

        summary_results.append({
            "RANK": 0, "OMOP_ID": omop_id, "STATUS": status_label,
            "EXT_MEDIAN": format_scientific(ext_clean.median()), "EXT_STD": format_scientific(ext_clean.std()),
            "OG_MEDIAN": format_scientific(har_clean.median()), "OG_STD": format_scientific(har_clean.std()),
            "KS": round(ks_stat, 3) if not np.isnan(ks_stat) else 1.0, "LOGP": round(log_p, 2),
            "N_EXTRACTED": n_ext, 
            "EXT_RATIO": ext_ratio,
            "Description": desc,
            "EXT_DECILES": get_deciles(ext_clean), "HAR_DECILES": get_deciles(har_clean)
        })

        # --- Plotting ---
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
        if not ext_clean.empty:
            sns.scatterplot(x=df.loc[ext_clean.index, 'ext'], y=df.loc[ext_clean.index, 'EVENT_AGE'], color='red', alpha=0.3, label='Extracted', ax=ax1, s=25)
            if ext_clean.nunique() > 1: sns.kdeplot(ext_clean, ax=ax2, color='red', label='Extracted')
            else: ax2.axvline(ext_clean.iloc[0], color='red', linestyle='--', label='Extracted (Singular)')
        if not har_clean.empty:
            sns.scatterplot(x=df.loc[har_clean.index, 'har'], y=df.loc[har_clean.index, 'EVENT_AGE'], color='blue', alpha=0.3, label='Harmonized', ax=ax1, s=25)
            if har_clean.nunique() > 1: sns.kdeplot(har_clean, ax=ax2, color='blue', label='Harmonized')
            else: ax2.axvline(har_clean.iloc[0], color='blue', linestyle='--', label='Harmonized (Singular)')
        
        ax1.set_title(f"OMOP {omop_id}: {desc}\nRatio: {ext_ratio} | KS: {round(ks_stat,3)}")
        ax1.legend(loc='upper right')
        if ax2.get_legend_handles_labels()[0]: ax2.legend(loc='upper right')
        
        # Updated filename to include suffix but no subfolder
        plot_name = f"{run_suffix}_omop_{omop_id}.png"
        fig.savefig(os.path.join(args.output_dir, plot_name), dpi=150)
        plt.close(fig)

    # 5. Final Save
    final_summary = pd.DataFrame(summary_results)
    if not final_summary.empty:
        final_summary = final_summary.sort_values("N_EXTRACTED", ascending=False).reset_index(drop=True)
        final_summary['RANK'] = final_summary.index + 1
    
    final_summary.to_csv(summary_file, sep='\t', index=False)
    pd.DataFrame(rejected_results).to_csv(rejected_file, sep='\t', index=False)

    print(f"\n--- Finished Mode: {run_suffix} ---")
    print(f"Summary: {summary_file}")
    print(f"Plots kept in: {args.output_dir}")

if __name__ == "__main__":
    main()
