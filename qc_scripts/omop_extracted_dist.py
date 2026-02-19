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
from pathlib import Path

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

def chunk_by_count(id_count_pairs, num_chunks=10):
    """Split ID-count pairs into roughly equal-sized chunks by total count"""
    # Sort by count descending to distribute large IDs evenly
    sorted_pairs = sorted(id_count_pairs, key=lambda x: x[1], reverse=True)
    
    chunks = [[] for _ in range(num_chunks)]
    chunk_totals = [0] * num_chunks
    
    # Greedy assignment: add each ID to the chunk with smallest current total
    for omop_id, count in sorted_pairs:
        min_idx = chunk_totals.index(min(chunk_totals))
        chunks[min_idx].append(omop_id)
        chunk_totals[min_idx] += count
    
    return chunks, chunk_totals

def apply_suffix_to_filepath(filepath, suffix):
    """Insert suffix before file extension. E.g., 'summary.tsv' + '_test_50000' -> 'summary_test_50000.tsv'"""
    p = Path(filepath)
    return str(p.parent / (p.stem + suffix + p.suffix))

def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_parquet = os.path.expanduser("~/fg-3/kanta_v3/core/kanta_dev_2026_02_03_core.parquet")
    DEFAULT_IDS = [3020564, 3009542, 3003396, 3019900, 3014007]
    default_ref_path = os.path.abspath(os.path.join(script_dir, "..", "finngen_qc", "data", "LABfi_ALL.usagi.csv"))
    
    parser = argparse.ArgumentParser(description="OMOP Analysis: ID Selection from Parquet Data")
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--ids', type=str, nargs='?', const=','.join(map(str, DEFAULT_IDS)), 
                   help="Comma-separated list of OMOP IDs (if flag used without value, uses DEFAULT_IDS)")
    group.add_argument('--full', action='store_true', help="Process ALL unique IDs found in the Parquet file")
    group.add_argument('--random', type=int, nargs='?', const=10, help="Process N random IDs found in the Parquet file")

    parser.add_argument('--file_path', type=str, default=default_parquet, help="Input Parquet path")
    parser.add_argument('--ref_path', type=str, default=default_ref_path, help="Usagi CSV path")
    parser.add_argument('--output_dir', type=str, default='./plots', help="Output directory for PNGs")
    parser.add_argument('--summary-file', type=str, default='summary.tsv', help="Path to summary output file")
    parser.add_argument('--test', type=int, nargs='?', const=1000000, default=None, help="Limit rows per ID (default 1M for test)")
    
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

    # Determine IDs and their counts
    if args.full or args.random is not None:
        print(f"STATUS: Fetching unique OMOP IDs and counts from {args.file_path}...")
        count_query = f"""
            SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as OMOP_CONCEPT_ID, COUNT(*) as count
            FROM '{args.file_path}'
            WHERE OMOP_CONCEPT_ID IS NOT NULL
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            GROUP BY CAST(OMOP_CONCEPT_ID AS INTEGER)
        """
        count_df = conn.execute(count_query).df()
        id_count_pairs = list(zip(count_df['OMOP_CONCEPT_ID'].astype(int), count_df['count']))
        
        if args.full:
            # Sort by ID for deterministic order
            id_count_pairs = sorted(id_count_pairs, key=lambda x: x[0])
            run_suffix = ""
        else:
            # Random sample
            count = min(args.random, len(id_count_pairs))
            id_count_pairs = random.sample(id_count_pairs, count)
            run_suffix = f"_random_{count}"
        
        omop_ids = [x[0] for x in id_count_pairs]
    elif args.ids:
        omop_ids = [int(i.strip()) for i in args.ids.split(',')]
        # Fetch counts for these IDs
        ids_str = ','.join(map(str, omop_ids))
        count_query = f"""
            SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as OMOP_CONCEPT_ID, COUNT(*) as count
            FROM '{args.file_path}'
            WHERE CAST(OMOP_CONCEPT_ID AS INTEGER) IN ({ids_str})
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            GROUP BY CAST(OMOP_CONCEPT_ID AS INTEGER)
        """
        count_df = conn.execute(count_query).df()
        id_count_pairs = list(zip(count_df['OMOP_CONCEPT_ID'].astype(int), count_df['count']))
        run_suffix = ""
    else:
        # Use DEFAULT_IDS
        omop_ids = DEFAULT_IDS
        ids_str = ','.join(map(str, omop_ids))
        count_query = f"""
            SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as OMOP_CONCEPT_ID, COUNT(*) as count
            FROM '{args.file_path}'
            WHERE CAST(OMOP_CONCEPT_ID AS INTEGER) IN ({ids_str})
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            GROUP BY CAST(OMOP_CONCEPT_ID AS INTEGER)
        """
        count_df = conn.execute(count_query).df()
        id_count_pairs = list(zip(count_df['OMOP_CONCEPT_ID'].astype(int), count_df['count']))
        run_suffix = ""

    if args.test:
        run_suffix += f"_test_{args.test}"

    # Apply suffix to output file
    summary_file = apply_suffix_to_filepath(args.summary_file, run_suffix) if run_suffix else args.summary_file

    # --- BATCH PROCESSING: Split into 10 chunks by total count ---
    print(f"STATUS: Processing {len(omop_ids)} OMOP IDs in 10 balanced batches...")
    chunks, chunk_totals = chunk_by_count(id_count_pairs, num_chunks=10)
    limit_clause = f"LIMIT {args.test}" if args.test is not None else ""
    
    summary_results = []
    
    for chunk_idx, batch_ids in enumerate(tqdm(chunks, desc=f"Analyzing", total=10)):
        if not batch_ids:  # Skip empty chunks
            continue
        
        ids_str = ','.join(map(str, batch_ids))
        chunk_count = chunk_totals[chunk_idx]
        
        print(f"  Batch {chunk_idx + 1}/10: {len(batch_ids)} IDs, ~{chunk_count:,} rows")
        
        query = f"""
            SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as OMOP_CONCEPT_ID,
                   MEASUREMENT_VALUE_EXTRACTED as ext, 
                   MEASUREMENT_VALUE_HARMONIZED as harm,
                   EVENT_AGE
            FROM '{args.file_path}'
            WHERE CAST(OMOP_CONCEPT_ID AS INTEGER) IN ({ids_str})
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            {limit_clause}
        """
        
        try:
            batch_data = conn.execute(query).fetchdf()
        except Exception as e:
            print(f"ERROR: Failed to fetch batch {chunk_idx + 1}: {e}")
            for bid in batch_ids:
                desc = ref_map.get(bid, "Unknown Concept")
                summary_results.append({
                    "RANK": 0, 
                    "OMOP_ID": bid, 
                    "STATUS": "QUERY_ERROR",
                    "EXT_MEDIAN": "-",
                    "HARM_REF_MEDIAN": "-",
                    "EXT_HARM_N_RATIO": 0.0,
                    "KS": 1.0,
                    "MLOGP": 0.0,
                    "N_EXTRACTED": 0,
                    "N_HARM_REF": 0,
                    "SIGMA_REJECTED_PCT": 0.0,
                    "Description": desc,
                    "EXT_DECILES": "-",
                    "HARM_REF_DECILES": "-"
                })
            continue
        
        if batch_data.empty:
            for bid in batch_ids:
                desc = ref_map.get(bid, "Unknown Concept")
                summary_results.append({
                    "RANK": 0, 
                    "OMOP_ID": bid, 
                    "STATUS": "EMPTY_OR_NON_NUMERIC",
                    "EXT_MEDIAN": "-",
                    "HARM_REF_MEDIAN": "-",
                    "EXT_HARM_N_RATIO": 0.0,
                    "KS": 1.0,
                    "MLOGP": 0.0,
                    "N_EXTRACTED": 0,
                    "N_HARM_REF": 0,
                    "SIGMA_REJECTED_PCT": 0.0,
                    "Description": desc,
                    "EXT_DECILES": "-",
                    "HARM_REF_DECILES": "-"
                })
            continue
        
        grouped = batch_data.groupby('OMOP_CONCEPT_ID')
        
        for omop_id in batch_ids:
            desc = ref_map.get(omop_id, "Unknown Concept")
            
            # Get data for this ID from the grouped data
            if omop_id not in grouped.groups:
                summary_results.append({
                    "RANK": 0, 
                    "OMOP_ID": omop_id, 
                    "STATUS": "EMPTY_OR_NON_NUMERIC",
                    "EXT_MEDIAN": "-",
                    "HARM_REF_MEDIAN": "-",
                    "EXT_HARM_N_RATIO": 0.0,
                    "KS": 1.0,
                    "MLOGP": 0.0,
                    "N_EXTRACTED": 0,
                    "N_HARM_REF": 0,
                    "SIGMA_REJECTED_PCT": 0.0,
                    "Description": desc,
                    "EXT_DECILES": "-",
                    "HARM_REF_DECILES": "-"
                })
                continue
            
            df = grouped.get_group(omop_id)

            ext_raw = df['ext'].dropna()
            harm_raw = df['harm'].dropna()

            ext_clean, n_ext_rem = sigma_filter(ext_raw)
            harm_clean, n_harm_rem = sigma_filter(harm_raw)

            if ext_clean.empty and harm_clean.empty:
                summary_results.append({
                    "RANK": 0, 
                    "OMOP_ID": omop_id, 
                    "STATUS": "SIGMA_FILTERED_TO_EMPTY",
                    "EXT_MEDIAN": "-",
                    "HARM_REF_MEDIAN": "-",
                    "EXT_HARM_N_RATIO": 0.0,
                    "KS": 1.0,
                    "MLOGP": 0.0,
                    "N_EXTRACTED": 0,
                    "N_HARM_REF": 0,
                    "SIGMA_REJECTED_PCT": 0.0,
                    "Description": desc,
                    "EXT_DECILES": "-",
                    "HARM_REF_DECILES": "-"
                })
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
            
            plot_path = os.path.join(args.output_dir, f"omop_{omop_id}.png")
            fig.savefig(plot_path, dpi=150, bbox_inches='tight')
            plt.close(fig)

    if summary_results:
        final_df = pd.DataFrame(summary_results).sort_values("N_EXTRACTED", ascending=False).reset_index(drop=True)
        final_df['RANK'] = final_df.index + 1
        final_df.to_csv(summary_file, sep='\t', index=False)
        print(f"Summary saved to: {summary_file}")
    
    print(f"\nDONE")

if __name__ == "__main__":
    main()
