import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
import argparse

def main():
    parser = argparse.ArgumentParser(description="Analyze relative changes between two data versions.")
    # File Inputs
    parser.add_argument("--new", required=True, help="Path to the new version TSV file")
    parser.add_argument("--old", required=True, help="Path to the old version TSV file")
    parser.add_argument("--names", default='/mnt/disks/data/kanta/meta/omop_name_table.tsv', 
                        help="Path to the OMOP name table TSV")
    
    # Suffix/Naming Options
    parser.add_argument("--new_suffix", default='_new', help="Suffix for new version columns")
    parser.add_argument("--old_suffix", default='_old', help="Suffix for old version columns")
    
    # Output Options
    parser.add_argument("--out_tsv", default='relative_change.tsv', help="Output filename for TSV data")
    parser.add_argument("--out_img", default='relative_change_histograms.png', help="Output filename for the plots")
    
    args = parser.parse_args()

    # 1. Load data
    print("Loading files...")
    new_ver = pd.read_csv(args.new, sep='\t')
    old_ver = pd.read_csv(args.old, sep='\t')

    if os.path.exists(args.names):
        names = pd.read_csv(args.names, sep='\t')
        names['NAME'] = names['conceptId'].astype(str)
    else:
        print(f"Warning: {args.names} not found. Proceeding without concept names.")
        names = pd.DataFrame(columns=['NAME', 'conceptName'])

    # 2. Merging
    m = new_ver.merge(old_ver, on='NAME', how='left', suffixes=(args.new_suffix, args.old_suffix))
    m = m.merge(names[['NAME', 'conceptName']], on='NAME', how='left')
    
    # Identify numeric columns for analysis
    cols = [c for c in new_ver.columns if c != 'NAME']

    # 3. Calculate Global Growth Factor (A)
    total_new_sum = new_ver[cols].sum().sum()
    total_old_sum = old_ver[cols].sum().sum()
    A = total_new_sum / total_old_sum if total_old_sum != 0 else 1.0
    print(f"Global Growth Factor (Total New / Total Old): {A:.4f}")

    # 4. Generate Output Table
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

    # 5. Visualization
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
        if not rel_change.empty:
            zoomed_data = rel_change[(rel_change >= 0) & (rel_change <= 2)]
            outliers_count = (rel_change > 2).sum()
            
            ax_hist.hist(zoomed_data, bins=50, range=(0, 2), edgecolor='black', alpha=0.7)
            ax_hist.axvline(x=1.0, color='red', linestyle='--', label='No change (1.0)')
            ax_hist.set_title(f'{col} - Ratio Distribution [0, 2]')
            ax_hist.set_xlabel('Relative Change Ratio')
            ax_hist.set_ylabel('Frequency')
            ax_hist.legend()
            
            stats = (f'Mean: {rel_change.mean():.3f}\n'
                     f'Median: {rel_change.median():.3f}\n'
                     f'N (total): {len(rel_change)}\n'
                     f'N > 2.0: {outliers_count}')
            
            ax_hist.text(0.95, 0.95, stats, transform=ax_hist.transAxes, verticalalignment='top', 
                        horizontalalignment='right', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # --- Right: Scatter Plot (Old vs New Counts) ---
        ax_scatter = axes[i, 1]
        ax_scatter.scatter(old_vals, new_vals, alpha=0.4, s=15)
        
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

    plt.tight_layout()
    plt.savefig(args.out_img, dpi=300, bbox_inches='tight')
    print(f"Done! Results saved to {args.out_tsv} and {args.out_img}")

if __name__ == "__main__":
    main()
