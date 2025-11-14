import pandas as pd
import os
import argparse
import matplotlib.pyplot as plt
import seaborn as sns
import sys # Added for robustness, as it was in previous versions

# Configuration
TXT_FILEPATH = '/mnt/disks/data/kanta/units/unharmonized_values.txt'
PARQUET_FILEPATH = '/home/pete/fg-3/kanta_v2/core/kanta_core_2025_05_22.parquet'
DELIMITER = '\t'

# Columns needed from the TXT file
TXT_COLS = [
    'source::MEASUREMENT_VALUE',
    'harmonization_omop::OMOP_ID',
    'cleaned::TEST_NAME_ABBREVIATION',
    'cleaned::MEASUREMENT_UNIT'
]

# Columns needed from the Parquet file
PARQUET_COLS = [
    'OMOP_CONCEPT_ID',
    'MEASUREMENT_VALUE_HARMONIZED',
    'MEASUREMENT_UNIT_HARMONIZED'
]

# Keys for identifying unique groups
GROUPING_KEYS = [
    'harmonization_omop::OMOP_ID',
    'cleaned::TEST_NAME_ABBREVIATION',
    'cleaned::MEASUREMENT_UNIT'
]


def load_or_create_cache(cache_path, txt_filepath):
    """Load cached groups or create from TXT file."""
    if os.path.exists(cache_path):
        print(f"Loading groups from cache: {cache_path}")
        groups = pd.read_csv(cache_path, keep_default_na=False, na_values=[''])
        print(f"Loaded {len(groups)} groups from cache")
        
        print("Loading TXT data for plotting...")
        df = pd.read_csv(txt_filepath, sep=DELIMITER, usecols=TXT_COLS, 
                         dtype=str, keep_default_na=False, na_values=[''])
        df['cleaned::MEASUREMENT_UNIT'] = df['cleaned::MEASUREMENT_UNIT'].fillna("NA")
        return groups, df
        
    print("Loading and grouping TXT data...")
    df = pd.read_csv(txt_filepath, sep=DELIMITER, usecols=TXT_COLS,
                     dtype=str, keep_default_na=False, na_values=[''])
    
    # --- Critical Step: Fill NaN units with "NA" string ---
    df['cleaned::MEASUREMENT_UNIT'] = df['cleaned::MEASUREMENT_UNIT'].fillna("NA")
    
    groups = df.groupby(GROUPING_KEYS, dropna=False).size().reset_index(name='Total_Count')
    groups = groups.sort_values(by='Total_Count', ascending=False)
    
    groups.to_csv(cache_path, index=False)
    print(f"Saved {len(groups)} groups to cache: {cache_path}")
    
    return groups, df


def query_parquet_data(parquet_path, omop_id, row_limit=None):
    """Query parquet file for specific OMOP ID using predicate pushdown."""
    # Note: Using filters ensures PyArrow only reads necessary row groups.
    filters = [('OMOP_CONCEPT_ID', '==', omop_id)]
    
    df = pd.read_parquet(parquet_path, columns=PARQUET_COLS, filters=filters)
    
    if row_limit:
        df = df.head(row_limit)
    
    return df


def generate_plot(data, output_path):
    """Generate side-by-side comparison distribution plots using KDE."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    n_unharm = len(data['unharmonized_values'])
    n_harm = len(data['harmonized_values'])
    
    title = f"{data['test_abbr']} (OMOP:{data['omop_id']}) - Total Count: {data['count']:,}"
    fig.suptitle(title, fontsize=13, fontweight='bold')
    
    # Left plot: Unharmonized only
    if n_unharm > 1:
        sns.kdeplot(data['unharmonized_values'], ax=ax1, 
                    color='skyblue', linewidth=2, fill=True, alpha=0.4)
    ax1.set_title(f'Unharmonized Distribution (n={n_unharm:,})', fontsize=11)
    ax1.set_xlabel(f"Unit: {data['unharmonized_unit']}", fontsize=10)
    ax1.set_ylabel('Density', fontsize=10)
    ax1.grid(axis='y', linestyle='--', alpha=0.7)
    ax1.tick_params(labelsize=8)
    
    # Right plot: Harmonized with unharmonized overlay
    if n_harm > 1:
        # Get harmonized range
        harm_min = data['harmonized_values'].min()
        harm_max = data['harmonized_values'].max()
        
        # Count unharmonized values in harmonized range
        unharm_in_range = data['unharmonized_values'][
            (data['unharmonized_values'] >= harm_min) & 
            (data['unharmonized_values'] <= harm_max)
        ]
        n_in_range = len(unharm_in_range)
        fraction_in_range = n_in_range / n_unharm if n_unharm > 0 else 0
        
        # Plot harmonized on primary axis
        sns.kdeplot(data['harmonized_values'], ax=ax2,
                    color='lightcoral', linewidth=2.5, fill=True, alpha=0.4,
                    label=f'Harmonized (n={n_harm:,})')
        
        # Plot ALL unharmonized on secondary axis (not just in-range)
        ax2_twin = ax2.twinx()
        if n_unharm > 1:
            sns.kdeplot(data['unharmonized_values'], ax=ax2_twin,
                        color='skyblue', linewidth=2, fill=False, alpha=0.7, linestyle='--',
                        label=f'Unharmonized (all, {fraction_in_range:.1%} in range)')
        
        # Set harmonized range for x-axis
        ax2.set_xlim(harm_min, harm_max)
        ax2_twin.set_xlim(harm_min, harm_max)
        
        # Add text annotation for out-of-range values
        n_out_range = n_unharm - n_in_range
        ax2.text(0.02, 0.98, f'Values outside range: {n_out_range:,} ({100-fraction_in_range*100:.1f}%)',
                 transform=ax2.transAxes, fontsize=9, verticalalignment='top',
                 bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # Labels and legend
        ax2.set_ylabel('Density (Harmonized)', fontsize=10, color='lightcoral')
        ax2_twin.set_ylabel('Density (Unharmonized)', fontsize=10, color='skyblue')
        ax2.tick_params(axis='y', labelcolor='lightcoral', labelsize=8)
        ax2_twin.tick_params(axis='y', labelcolor='skyblue', labelsize=8)
        
        # Combine legends
        lines1, labels1 = ax2.get_legend_handles_labels()
        lines2, labels2 = ax2_twin.get_legend_handles_labels()
        ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=8)
        
    ax2.set_title('Harmonized Distribution with Unharmonized Overlay', fontsize=11)
    ax2.set_xlabel(f"Unit: {data['harmonized_unit']}", fontsize=10)
    ax2.grid(axis='y', linestyle='--', alpha=0.7)
    ax2.tick_params(axis='x', labelsize=8)
    
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close(fig)
    
    # Return statistics for summary table
    return {
        'omop_id': data['omop_id'],
        'test_abbr': data['test_abbr'],
        'unharmonized_unit': data['unharmonized_unit'],
        'harmonized_unit': data['harmonized_unit'],
        'unharmonized_count': n_unharm,
        'harmonized_count': n_harm,
        'unharmonized_mean': data['unharmonized_values'].mean() if n_unharm > 0 else None,
        'unharmonized_median': data['unharmonized_values'].median() if n_unharm > 0 else None,
        'unharmonized_std': data['unharmonized_values'].std() if n_unharm > 0 else None,
        'harmonized_mean': data['harmonized_values'].mean() if n_harm > 0 else None,
        'harmonized_median': data['harmonized_values'].median() if n_harm > 0 else None,
        'harmonized_std': data['harmonized_values'].std() if n_harm > 0 else None,
    }


def analyze_data(test_mode, output_path):
    """Main analysis function."""
    top_n = 3 if test_mode else 30
    parquet_limit = 10000 if test_mode else None
    
    print(f"--- {'TEST' if test_mode else 'FULL'} MODE ---")
    print(f"Top {top_n} groups, Parquet limit: {parquet_limit or 'None'}")
    
    output_dir = os.path.dirname(output_path) or '.'
    cache_path = os.path.join(output_dir, 'all_problem_groups_cache.csv')
    
    # Load data
    all_groups, analysis_df = load_or_create_cache(cache_path, TXT_FILEPATH)
    top_groups = all_groups.head(top_n)
    
    print(f"Processing {len(top_groups)} groups...")
    
    plot_paths = []
    summary_stats = []
    
    for i, group in top_groups.iterrows():
        omop_id_str = str(group['harmonization_omop::OMOP_ID'])
        test_abbr = group['cleaned::TEST_NAME_ABBREVIATION']
        unit = str(group['cleaned::MEASUREMENT_UNIT'])
        count = group['Total_Count']
        
        # Convert OMOP ID for filtering
        try:
            omop_id_filter = str(int(float(omop_id_str)))
        except ValueError:
            print(f"Warning: Skipping group {i+1} - invalid OMOP ID: '{omop_id_str}'")
            continue
        
        print(f"Processing {i+1}/{len(top_groups)}: {test_abbr} ({omop_id_filter}), unit '{unit}'")
        
        # Filter TXT data
        txt_mask = (
            (analysis_df['harmonization_omop::OMOP_ID'] == omop_id_str) &
            (analysis_df['cleaned::TEST_NAME_ABBREVIATION'] == test_abbr) &
            (analysis_df['cleaned::MEASUREMENT_UNIT'] == unit)
        )
        txt_subset = analysis_df[txt_mask]
        
        # Query Parquet data
        parquet_subset = query_parquet_data(PARQUET_FILEPATH, omop_id_filter, parquet_limit)
        
        # Convert to numeric (use source::MEASUREMENT_VALUE for unharmonized)
        unharmonized = pd.to_numeric(txt_subset['source::MEASUREMENT_VALUE'], errors='coerce').dropna()
        harmonized = pd.to_numeric(parquet_subset['MEASUREMENT_VALUE_HARMONIZED'], errors='coerce').dropna()
        
        # Get harmonized unit
        if not parquet_subset.empty and not parquet_subset['MEASUREMENT_UNIT_HARMONIZED'].mode().empty:
            harmonized_unit = parquet_subset['MEASUREMENT_UNIT_HARMONIZED'].mode().iloc[0]
        else:
            harmonized_unit = "N/A"
        
        # Generate plot
        safe_abbr = test_abbr.replace(' ', '_').replace('/', '_')
        safe_unit = unit.replace(' ', '_').replace('/', '_')
        plot_filename = f"{omop_id_filter}_{safe_abbr}_{safe_unit}_comparison.png"
        plot_path = os.path.join(output_dir, plot_filename)
        
        plot_data = {
            'omop_id': omop_id_str,
            'test_abbr': test_abbr,
            'unharmonized_unit': unit,
            'harmonized_unit': harmonized_unit,
            'unharmonized_values': unharmonized,
            'harmonized_values': harmonized,
            'count': int(count)
        }
        
        stats = generate_plot(plot_data, plot_path)
        plot_paths.append(plot_path)
        summary_stats.append(stats)
        
    # Create summary table
    summary_df = pd.DataFrame(summary_stats)
    
    # --- CHANGE 1: Round numeric statistics to 4 decimal places ---
    numeric_cols = [
        'unharmonized_mean', 'unharmonized_median', 'unharmonized_std',
        'harmonized_mean', 'harmonized_median', 'harmonized_std'
    ]
    summary_df[numeric_cols] = summary_df[numeric_cols].round(4)
    
    # --- CHANGE 2: Save as TSV file with tab separator ---
    summary_path = os.path.join(output_dir, 'summary_statistics.tsv')
    summary_df.to_csv(summary_path, index=False, sep='\t')
    
    print(f"\n--- SUCCESS ---")
    print(f"Generated {len(plot_paths)} plots:")
    for p in plot_paths:
        print(f"  - {os.path.abspath(p)}")
    print(f"\nSummary statistics saved to: {os.path.abspath(summary_path)}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze distribution shift between unharmonized and harmonized measurements"
    )
    parser.add_argument("--test-mode", action="store_true",
                        help="Test mode: top 3 groups, 10k rows per query")
    parser.add_argument("--output-path", type=str, default="distribution_comparison.png",
                        help="Output directory path for plots")
    
    args = parser.parse_args()
    analyze_data(args.test_mode, args.output_path)


if __name__ == "__main__":
    main()
