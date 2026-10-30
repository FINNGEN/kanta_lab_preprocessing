"""Compare two releases' per-OMOP counts: ratio new / old per column, histograms, appeared / disappeared OMOP IDs.

--new / --old take a counts.py TSV, or a _RELEASE.parquet: its counts table is then made first with counts.py (the
columns below), as new_/old_<parquet name>_omop_analysis.tsv in the current folder, and reused while it is newer than the
parquet (--force to recount).
"""
import argparse
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from kanta import config

sys.path.insert(0, str(Path(__file__).resolve().parent))
from counts import analyze_omop_data  # noqa: E402

# What a parquet is counted on (counts.py -c / -b / -a): values, outcomes, failed QC, rows with a reference limit
COUNT_COLUMNS = ['MEASUREMENT_VALUE_CLEANED', 'MEASUREMENT_UNIT_CLEANED', 'MEASUREMENT_VALUE_HARMONIZED', 'TEST_OUTCOME',
                 'TEST_OUTCOME_IMPUTED']
ZERO_COLUMNS = ['QC_PASS']
ANY_COLUMNS = [('REFERENCE_RANGE', ['REFERENCE_RANGE_LOW_VALUE', 'REFERENCE_RANGE_HIGH_VALUE'])]  # lower or upper (or both)


def counts_table(path, role, force=False):
    """A counts TSV as given, or the one made from a parquet (kept as <role>_<name>_omop_analysis.tsv, so two releases
    with the same file name don't share it; reused when newer)."""
    path = Path(path)
    if path.suffix != '.parquet':
        return path
    tsv = Path(f"{role}_{path.stem}_omop_analysis.tsv")
    if not force and tsv.exists() and tsv.stat().st_mtime >= path.stat().st_mtime:
        print(f"Reusing {tsv} (newer than {path.name}; --force to recount)")
    else:
        print(f"Counting {path} -> {tsv}")
        analyze_omop_data(str(path), COUNT_COLUMNS, ZERO_COLUMNS, '.', prefix=f"{role}_{path.stem}", any_columns=ANY_COLUMNS)
    return tsv


def main():
    parser = argparse.ArgumentParser(description="Analyze relative changes between two data versions.")
    # File Inputs
    parser.add_argument("--new", required=True, help="The new version: a counts.py TSV, or a _RELEASE.parquet (counted first)")
    parser.add_argument("--old", required=True, help="The old version: a counts.py TSV, or a _RELEASE.parquet (counted first)")
    parser.add_argument("--force", action="store_true", help="Recount a parquet even when its counts table is up to date")
    parser.add_argument("--names", default=config.USAGI_MAPPING_FILE,
                        help="Usagi mapping TSV providing OMOP_ID -> concept name")
    
    # Suffix/Naming Options
    parser.add_argument("--new_suffix", default='_new', help="Suffix for new version columns")
    parser.add_argument("--old_suffix", default='_old', help="Suffix for old version columns")
    
    # Output Options
    parser.add_argument("--out_tsv", default='relative_change.tsv', help="Output filename for TSV data")
    parser.add_argument("--out_img", default='relative_change_histograms.png', help="Output filename for the plots")
    parser.add_argument("--out_disappeared", default='disappeared_omop_ids.tsv', help="Output filename for disappeared IDs")
    parser.add_argument("--out_appeared", default='appeared_omop_ids.tsv', help="Output filename for appeared IDs")
    
    args = parser.parse_args()

    # 1. Load data (counting a parquet first)
    args.new, args.old = counts_table(args.new, 'new', args.force), counts_table(args.old, 'old', args.force)
    print("Loading files...")
    new_ver = pd.read_csv(args.new, sep='\t')
    old_ver = pd.read_csv(args.old, sep='\t')
    new_ver['NAME'] = new_ver['NAME'].astype(str)
    old_ver['NAME'] = old_ver['NAME'].astype(str)

    if os.path.exists(args.names):
        names = pd.read_csv(args.names, sep='\t', dtype=str)
        names = names.rename(columns={'harmonization_omop::OMOP_ID': 'NAME',
                                      'harmonization_omop::OMOP_NAME': 'conceptName'})
        names = names.dropna(subset=['NAME']).drop_duplicates('NAME')
    else:
        print(f"Warning: {args.names} not found. Proceeding without concept names.")
        names = pd.DataFrame(columns=['NAME', 'conceptName'])

    # 2. Identify disappeared and appeared OMOP IDs
    print("\nAnalyzing OMOP ID changes...")
    old_ids = set(old_ver['NAME'].astype(str))
    new_ids = set(new_ver['NAME'].astype(str))
    
    disappeared_ids = sorted(old_ids - new_ids)
    appeared_ids = sorted(new_ids - old_ids)
    
    print(f"Old version OMOP IDs: {len(old_ids):,}")
    print(f"New version OMOP IDs: {len(new_ids):,}")
    print(f"Disappeared IDs (in old, not in new): {len(disappeared_ids):,}")
    print(f"Appeared IDs (in new, not in old): {len(appeared_ids):,}")
    
    # Save disappeared IDs
    disappeared_df = pd.DataFrame({'NAME': disappeared_ids})
    disappeared_df.to_csv(args.out_disappeared, sep='\t', index=False)
    print(f"Disappeared IDs saved to: {args.out_disappeared}")
    
    # Save appeared IDs
    appeared_df = pd.DataFrame({'NAME': appeared_ids})
    appeared_df.to_csv(args.out_appeared, sep='\t', index=False)
    print(f"Appeared IDs saved to: {args.out_appeared}")

    # 3. Merging
    print("\nMerging data for analysis...")
    m = new_ver.merge(old_ver, on='NAME', how='left', suffixes=(args.new_suffix, args.old_suffix))
    m = m.merge(names[['NAME', 'conceptName']], on='NAME', how='left')
    
    # Identify numeric columns for analysis
    cols = [c for c in new_ver.columns if c != 'NAME']

    # 4. Calculate Global Growth Factor (A)
    total_new_sum = new_ver[cols].sum().sum()
    total_old_sum = old_ver[cols].sum().sum()
    A = total_new_sum / total_old_sum if total_old_sum != 0 else 1.0
    print(f"Global Growth Factor (Total New / Total Old): {A:.4f}")

    # 5. Generate Output Table
    print("Generating output table...")
    out = {'NAME': m['NAME']}
    
    for col in cols:
        new_col, old_col = f'{col}{args.new_suffix}', f'{col}{args.old_suffix}'
        
        # Keep the absolute count from the new data (aliased as column_COUNT)
        out[f'{col}_COUNT'] = m[new_col]
        
        # Calculate relative change ratio (aliased as column_RATIO)
        out[f'{col}_RATIO'] = np.where(
            (m[old_col].notna()) & (m[old_col] != 0),
            (m[new_col] / m[old_col]).round(3),
            np.nan
        )
    
    out['conceptName'] = m['conceptName']
    df_out = pd.DataFrame(out)
    df_out.to_csv(args.out_tsv, sep='\t', index=False, na_rep='NA')

    # 6. Visualization
    print("Generating plots...")
    fig, axes = plt.subplots(len(cols), 2, figsize=(16, 6 * len(cols)))
    if len(cols) == 1:
        axes = axes.reshape(1, 2)

    for i, col in enumerate(cols):
        new_vals = m[f'{col}{args.new_suffix}'].fillna(0)
        old_vals = m[f'{col}{args.old_suffix}'].fillna(0)
        # Use the ratio column we just created in the output dataframe
        rel_change = df_out[f'{col}_RATIO'].dropna().astype(float)
        
        # --- Left: Histogram (Zoomed [0, 2]) ---
        ax_hist = axes[i, 0]
        # OMOP IDs of the new release whose count in this column went from 0 (incl. new IDs) to > 0, or from > 0 to 0:
        # no ratio, so not in the histogram; in the scatter on the axes, coloured. IDs gone from the new release are
        # in the box at the top.
        omop = (m['NAME'] != 'ALL').to_numpy()
        from_zero = omop & (old_vals == 0).to_numpy() & (new_vals > 0).to_numpy()
        to_zero = omop & (old_vals > 0).to_numpy() & (new_vals == 0).to_numpy()
        stats = f'from 0: {from_zero.sum():,}\nto 0: {to_zero.sum():,}'
        if not rel_change.empty:
            zoomed_data = rel_change[(rel_change >= 0) & (rel_change <= 2)]
            outliers_count = (rel_change > 2).sum()

            ax_hist.hist(zoomed_data, bins=50, range=(0, 2), edgecolor='black', alpha=0.7)
            ax_hist.axvline(x=1.0, color='red', linestyle='--', label='No change (1.0)')
            ax_hist.set_xlabel('Relative Change Ratio')
            ax_hist.set_ylabel('Frequency')
            ax_hist.legend()

            stats = (f'Mean: {rel_change.mean():.3f}\n'
                     f'Median: {rel_change.median():.3f}\n'
                     f'N (total): {len(rel_change)}\n'
                     f'N > 2.0: {outliers_count}\n' + stats)
        ax_hist.set_title(f'{col} - Ratio Distribution [0, 2]')
        ax_hist.text(0.95, 0.95, stats, transform=ax_hist.transAxes, verticalalignment='top',
                     horizontalalignment='right', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # --- Right: Scatter Plot (Old vs New Counts) ---
        ax_scatter = axes[i, 1]
        rest = ~(from_zero | to_zero)
        ax_scatter.scatter(old_vals[rest], new_vals[rest], alpha=0.4, s=15)
        ax_scatter.scatter(old_vals[from_zero], new_vals[from_zero], alpha=0.8, s=25, color='tab:orange',
                           label=f'from 0 ({from_zero.sum():,})')
        ax_scatter.scatter(old_vals[to_zero], new_vals[to_zero], alpha=0.8, s=25, color='tab:purple',
                           label=f'to 0 ({to_zero.sum():,})')
        
        # Reference Lines
        max_val = max(old_vals.max(), new_vals.max())
        line_x = np.array([0, max_val])
        
        ax_scatter.plot(line_x, line_x, color='red', linestyle='--', label='y = x (Constant)')
        ax_scatter.plot(line_x, A * line_x, color='green', linestyle=':', linewidth=2, label=f'y = {A:.3f}x (Global Avg)')
        
        ax_scatter.set_title(f'{col} - Count Comparison')
        ax_scatter.set_xlabel(f'Old Count ({args.old_suffix})')
        ax_scatter.set_ylabel(f'New Count ({args.new_suffix})')
        ax_scatter.legend()
        
        if max_val > 1000:
            ax_scatter.set_xscale('symlog')
            ax_scatter.set_yscale('symlog')
        ax_scatter.grid(True, alpha=0.3)

    # OMOP IDs that appeared / disappeared, in a box above the plots ('ALL' is the total row, not an OMOP ID)
    ids = lambda s: {x for x in s if x != 'ALL'}
    fig.text(0.99, 0.995, f"OMOP IDs: {len(ids(old_ids)):,} old, {len(ids(new_ids)):,} new\n"
                          f"appeared: {len(ids(appeared_ids)):,}   disappeared: {len(ids(disappeared_ids)):,}",
             ha='right', va='top', fontsize=14, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    plt.tight_layout(rect=(0, 0, 1, 1 - 0.6 / fig.get_figheight()))  # room for the box
    plt.savefig(args.out_img, dpi=300, bbox_inches='tight')
    print(f"\nDone! Results saved to:")
    print(f"  - {args.out_tsv}")
    print(f"  - {args.out_img}")
    print(f"  - {args.out_disappeared}")
    print(f"  - {args.out_appeared}")

if __name__ == "__main__":
    main()
