import os
import sys

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
    sorted_pairs = sorted(id_count_pairs, key=lambda x: x[1], reverse=True)
    chunks = [[] for _ in range(num_chunks)]
    chunk_totals = [0] * num_chunks
    for omop_id, count in sorted_pairs:
        min_idx = chunk_totals.index(min(chunk_totals))
        chunks[min_idx].append(omop_id)
        chunk_totals[min_idx] += count
    return chunks, chunk_totals

def apply_suffix_to_filepath(filepath, suffix):
    p = Path(filepath)
    return str(p.parent / (p.stem + suffix + p.suffix))

def get_plot_limits(series_list):
    # Outlier-resistant axis range
    full = pd.concat([s.dropna() for s in series_list if not s.empty])
    if full.empty:
        return None, None
    q1, q99 = np.percentile(full, [1, 99])
    rng = q99 - q1
    pad = 0.05 * rng if rng > 0 else 1.0
    return q1 - pad, q99 + pad

def load_ref_data(ref_path):
    try:
        ref_df = pd.read_csv(ref_path).drop_duplicates(subset=['conceptId'])
        ref_df['conceptId'] = ref_df['conceptId'].astype(float).astype(int)
        return dict(zip(ref_df['conceptId'], ref_df['conceptName']))
    except Exception:
        return {}

def get_omop_id_count_pairs(conn, args, ids=None):
    if ids is None:
        count_query = f"""
            SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as OMOP_CONCEPT_ID, COUNT(*) as count
            FROM '{args.file_path}'
            WHERE OMOP_CONCEPT_ID IS NOT NULL
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            GROUP BY CAST(OMOP_CONCEPT_ID AS INTEGER)
        """
    else:
        ids_str = ','.join(map(str, ids))
        count_query = f"""
            SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as OMOP_CONCEPT_ID, COUNT(*) as count
            FROM '{args.file_path}'
            WHERE CAST(OMOP_CONCEPT_ID AS INTEGER) IN ({ids_str})
            AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
            GROUP BY CAST(OMOP_CONCEPT_ID AS INTEGER)
        """
    count_df = conn.execute(count_query).df()
    return list(zip(count_df['OMOP_CONCEPT_ID'].astype(int), count_df['count']))

def fetch_batch_data(conn, args, batch_ids, limit_clause):
    ids_str = ','.join(map(str, batch_ids))
    query = f"""
        SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as OMOP_CONCEPT_ID,
               MEASUREMENT_VALUE_EXTRACTED as ext, 
               MEASUREMENT_VALUE_HARMONIZED as harm,
               MEASUREMENT_VALUE_MERGED,
               EVENT_AGE
        FROM '{args.file_path}'
        WHERE CAST(OMOP_CONCEPT_ID AS INTEGER) IN ({ids_str})
        AND (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_HARMONIZED IS NOT NULL)
        {limit_clause}
    """
    return conn.execute(query).fetchdf()

def collect_summary_row(omop_id, desc, status, ext_clean, harm_clean, n_ratio, ks_stat, mlogp, n_ext, n_harm, sigma_reject_pct):
    return {
        "RANK": 0, 
        "OMOP_ID": omop_id, 
        "STATUS": status,
        "EXT_MEDIAN": format_scientific(ext_clean.median()) if n_ext else "-",
        "HARM_REF_MEDIAN": format_scientific(harm_clean.median()) if n_harm else "-",
        "EXT_HARM_N_RATIO": n_ratio,
        "KS": round(ks_stat, 3) if ks_stat is not None and not np.isnan(ks_stat) else 1.0,
        "MLOGP": round(mlogp, 2),
        "N_EXTRACTED": n_ext,
        "N_HARM_REF": n_harm,
        "SIGMA_REJECTED_PCT": sigma_reject_pct,
        "Description": desc,
        "EXT_DECILES": get_deciles(ext_clean),
        "HARM_REF_DECILES": get_deciles(harm_clean)
    }

def plot_distributions(
    df, omop_id, desc,
    ext_clean, harm_clean, merged_clean,
    n_ratio, ks_stat, mlogp,
    plot_path
):
    # Defensive: empty = pd.Series([], dtype=float)
    for_val = lambda s: s if not s.empty else pd.Series([], dtype=float)

    left_min, left_max = get_plot_limits([for_val(ext_clean), for_val(harm_clean)])
    right_min, right_max = get_plot_limits([for_val(merged_clean)])

    fig, axes = plt.subplots(2, 2, figsize=(18, 10), sharex='col')
    ax1, ax2 = axes[0,0], axes[1,0]  # ext/harm
    ax3, ax4 = axes[0,1], axes[1,1]  # merged

    # -- Ext/Harm scatter and KDE --
    if not ext_clean.empty:
        mask = ext_clean.between(left_min, left_max)
        sns.scatterplot(x=df.loc[ext_clean[mask].index, 'ext'], y=df.loc[ext_clean[mask].index, 'EVENT_AGE'],
                        color='red', alpha=0.3, label='Extracted', ax=ax1, s=25, rasterized=True)
        if (~mask).any():
            sns.scatterplot(x=df.loc[ext_clean[~mask].index, 'ext'], y=df.loc[ext_clean[~mask].index, 'EVENT_AGE'],
                            color='red', alpha=0.08, label='_nolegend_', ax=ax1, s=25, rasterized=True)
        if ext_clean.nunique() > 1:
            sns.kdeplot(ext_clean.clip(lower=left_min, upper=left_max), ax=ax2, color='red', bw_adjust=1.2,
                        label='Extracted PDF', clip=(left_min, left_max))
        else:
            ax2.axvline(ext_clean.iloc[0], color='red', linestyle='--')
    if not harm_clean.empty:
        mask = harm_clean.between(left_min, left_max)
        sns.scatterplot(x=df.loc[harm_clean[mask].index, 'harm'], y=df.loc[harm_clean[mask].index, 'EVENT_AGE'],
                        color='blue', alpha=0.3, label='Harmonized (Ref)', ax=ax1, s=25, rasterized=True)
        if (~mask).any():
            sns.scatterplot(x=df.loc[harm_clean[~mask].index, 'harm'], y=df.loc[harm_clean[~mask].index, 'EVENT_AGE'],
                            color='blue', alpha=0.08, label='_nolegend_', ax=ax1, s=25, rasterized=True)
        if harm_clean.nunique() > 1:
            sns.kdeplot(harm_clean.clip(lower=left_min, upper=left_max), ax=ax2, color='blue', bw_adjust=1.2,
                        label='Harmonized PDF', clip=(left_min, left_max))
        else:
            ax2.axvline(harm_clean.iloc[0], color='blue', linestyle='--')

    ax1.legend(loc='upper right')
    title_str = (f"OMOP {omop_id}: {desc}\n"
                 f"N Ratio: {n_ratio} | KS: {round(ks_stat,3) if ks_stat is not None else '-'} | -log10(p): {round(mlogp, 2)}")
    ax1.set_title(title_str)
    ax2.set_title("Extracted/Harmonized Value Distribution")
    if left_min is not None and left_max is not None:
        ax1.set_xlim(left_min, left_max)
        ax2.set_xlim(left_min, left_max)

    # -- Merged scatter and KDE --
    if not merged_clean.empty:
        mask = merged_clean.between(right_min, right_max)
        sns.scatterplot(x=df.loc[merged_clean[mask].index, 'MEASUREMENT_VALUE_MERGED'], y=df.loc[merged_clean[mask].index, 'EVENT_AGE'],
                        color='purple', alpha=0.3, label='Merged', ax=ax3, s=25, rasterized=True)
        if (~mask).any():
            sns.scatterplot(x=df.loc[merged_clean[~mask].index, 'MEASUREMENT_VALUE_MERGED'], y=df.loc[merged_clean[~mask].index, 'EVENT_AGE'],
                            color='purple', alpha=0.08, label='_nolegend_', ax=ax3, s=25, rasterized=True)
        if merged_clean.nunique() > 1:
            sns.kdeplot(merged_clean.clip(lower=right_min, upper=right_max), ax=ax4, color='purple', bw_adjust=1.2,
                        label='Merged PDF', clip=(right_min, right_max))
        else:
            ax4.axvline(merged_clean.iloc[0], color='purple', linestyle='--')
        ax3.legend(loc='upper right')
    ax3.set_title("Merged (Value vs Event Age)")
    ax4.set_title("Merged Value Distribution")
    if right_min is not None and right_max is not None:
        ax3.set_xlim(right_min, right_max)
        ax4.set_xlim(right_min, right_max)

    for ax in [ax1, ax3]:
        ax.set_xlabel("Value")
        ax.set_ylabel("Event Age")
    for ax in [ax2, ax4]:
        ax.set_xlabel("Value")
        ax.set_ylabel("Density")

    fig.tight_layout()
    fig.savefig(plot_path, dpi=150, bbox_inches='tight')
    plt.close(fig)

def process_id_group(df, omop_id, desc):
    ext_raw = df['ext'].dropna()
    harm_raw = df['harm'].dropna()
    merged_raw = df['MEASUREMENT_VALUE_MERGED'].dropna() if 'MEASUREMENT_VALUE_MERGED' in df.columns else pd.Series(dtype=float)

    ext_clean, n_ext_rem = sigma_filter(ext_raw)
    harm_clean, n_harm_rem = sigma_filter(harm_raw)
    merged_clean, n_merged_rem = sigma_filter(merged_raw)

    n_ext, n_harm = len(ext_clean), len(harm_clean)
    n_ratio = round(n_ext / n_harm, 4) if n_harm > 0 else 0.0
    total_numeric = len(ext_raw) + len(harm_raw)
    total_removed = n_ext_rem + n_harm_rem
    sigma_reject_pct = round((total_removed / total_numeric) * 100, 2) if total_numeric > 0 else 0.0
    if n_ext > 0 and n_harm > 0:
        ks_stat, p_val = stats.ks_2samp(ext_clean, harm_clean)
        mlogp = -np.log10(max(p_val, 1e-300))
    else:
        ks_stat, mlogp = np.nan, 0.0
    status_label = "SUCCESS" if (np.isnan(ks_stat) or ks_stat < 0.3) else "FAIL"
    return (ext_clean, harm_clean, merged_clean, n_ratio, ks_stat, mlogp, n_ext, n_harm, sigma_reject_pct, status_label)

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

    ref_map = load_ref_data(args.ref_path)
    conn = duckdb.connect()

    if args.full or args.random is not None:
        print(f"STATUS: Fetching unique OMOP IDs and counts from {args.file_path}...")
        id_count_pairs = get_omop_id_count_pairs(conn, args)
        if args.full:
            id_count_pairs = sorted(id_count_pairs, key=lambda x: x[0])
            run_suffix = ""
        else:
            count = min(args.random, len(id_count_pairs))
            id_count_pairs = random.sample(id_count_pairs, count)
            run_suffix = f"_random_{count}"
        omop_ids = [x[0] for x in id_count_pairs]
    elif args.ids:
        omop_ids = [int(i.strip()) for i in args.ids.split(',')]
        id_count_pairs = get_omop_id_count_pairs(conn, args, omop_ids)
        run_suffix = ""
    else:
        omop_ids = DEFAULT_IDS
        id_count_pairs = get_omop_id_count_pairs(conn, args, omop_ids)
        run_suffix = ""

    if args.test:
        run_suffix += f"_test_{args.test}"

    summary_file = apply_suffix_to_filepath(args.summary_file, run_suffix) if run_suffix else args.summary_file

    print(f"STATUS: Processing {len(omop_ids)} OMOP IDs in 10 balanced batches...")
    chunks, chunk_totals = chunk_by_count(id_count_pairs, num_chunks=10)
    limit_clause = f"LIMIT {args.test}" if args.test is not None else ""

    summary_results = []
    for chunk_idx, batch_ids in enumerate(tqdm(chunks, desc=f"Analyzing", total=10)):
        if not batch_ids:
            continue
        chunk_count = chunk_totals[chunk_idx]
        print(f"  Batch {chunk_idx + 1}/10: {len(batch_ids)} IDs, ~{chunk_count:,} rows")
        try:
            batch_data = fetch_batch_data(conn, args, batch_ids, limit_clause)
        except Exception as e:
            print(f"ERROR: Failed to fetch batch {chunk_idx + 1}: {e}")
            for bid in batch_ids:
                desc = ref_map.get(bid, "Unknown Concept")
                summary_results.append(collect_summary_row(
                    bid, desc, "QUERY_ERROR", pd.Series(dtype=float), pd.Series(dtype=float),
                    0.0, 1.0, 0.0, 0, 0, 0.0
                ))
            continue
        if batch_data.empty:
            for bid in batch_ids:
                desc = ref_map.get(bid, "Unknown Concept")
                summary_results.append(collect_summary_row(
                    bid, desc, "EMPTY_OR_NON_NUMERIC", pd.Series(dtype=float), pd.Series(dtype=float),
                    0.0, 1.0, 0.0, 0, 0, 0.0
                ))
            continue
        grouped = batch_data.groupby('OMOP_CONCEPT_ID')
        for omop_id in batch_ids:
            desc = ref_map.get(omop_id, "Unknown Concept")
            if omop_id not in grouped.groups:
                summary_results.append(collect_summary_row(
                    omop_id, desc, "EMPTY_OR_NON_NUMERIC", pd.Series(dtype=float), pd.Series(dtype=float),
                    0.0, 1.0, 0.0, 0, 0, 0.0
                ))
                continue
            df = grouped.get_group(omop_id)
            results = process_id_group(df, omop_id, desc)
            ext_clean, harm_clean, merged_clean, n_ratio, ks_stat, mlogp, n_ext, n_harm, sigma_reject_pct, status_label = results
            summary_results.append(collect_summary_row(
                omop_id, desc, status_label, ext_clean, harm_clean,
                n_ratio, ks_stat, mlogp, n_ext, n_harm, sigma_reject_pct
            ))
            plot_path = os.path.join(args.output_dir, f"omop_{omop_id}.png")
            plot_distributions(
                df, omop_id, desc, ext_clean, harm_clean, merged_clean,
                n_ratio, ks_stat, mlogp, plot_path
            )

    if summary_results:
        final_df = pd.DataFrame(summary_results).sort_values("N_EXTRACTED", ascending=False).reset_index(drop=True)
        final_df['RANK'] = final_df.index + 1
        final_df.to_csv(summary_file, sep='\t', index=False)
        print(f"Summary saved to: {summary_file}")
    print(f"\nDONE")

if __name__ == "__main__":
    main()
