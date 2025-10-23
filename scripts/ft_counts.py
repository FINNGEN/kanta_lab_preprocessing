import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import argparse
import os
import duckdb # Import duckdb for running SQL against Parquet files

# Set a style for better visualization
sns.set_theme(style="whitegrid")

def sigma_filter(data: pd.Series, n_sigma: int = 3) -> pd.Series:
    """Filters a pandas Series to keep only values within n_sigma of the mean."""
    # Ensure data is numeric and non-empty
    data = data.dropna()
    if data.empty:
        return pd.Series([], dtype=float)
        
    mean = data.mean()
    std = data.std()
    lower_bound = mean - n_sigma * std
    upper_bound = upper_bound = mean + n_sigma * std
    return data[(data >= lower_bound) & (data <= upper_bound)]

def analyze_and_plot_omop_data(data: pd.DataFrame, omop_id: int, output_dir: str):
    """
    Takes loaded data, performs 3-sigma filtering, and generates a two-panel plot 
    (Scatter and Density) comparing Extracted and Harmonized values. The plot is 
    saved to the specified output directory.
    """
    
    print(f"--- Starting analysis for OMOP ID: {omop_id} (n={len(data):,}) ---")
    
    # Filter the data for the specific OMOP ID
    filtered_data = data[data['OMOP_CONCEPT_ID'] == omop_id].copy()

    if filtered_data.empty:
        print(f"No data found for OMOP ID {omop_id}. Skipping plot generation.")
        return

    # Prepare the two primary columns
    df = filtered_data.rename(columns={
        'MEASUREMENT_VALUE_EXTRACTED': 'Original Values',
        'MEASUREMENT_VALUE_HARMONIZED': 'Imputed Values'
    })
    
    # --- 2. SCATTER PLOT (AGE vs VALUE) ---
    fig, axes = plt.subplots(2, 1, figsize=(10, 10), sharex=True)
    plt.subplots_adjust(hspace=0.3)
    
    # Scatter Plot Setup
    ax1 = axes[0]
    
    # 1. Plot Original Values (Extracted) - Blue
    sns.scatterplot(
        x='Original Values', 
        y='EVENT_AGE', 
        data=df, 
        color='blue', 
        alpha=0.3, 
        label='Original Values', 
        ax=ax1,
        edgecolor=None,
        s=30
    )
    
    # 2. Plot Imputed Values (Harmonized) - Red (non-null only)
    df_imputed = df.dropna(subset=['Imputed Values'])
    sns.scatterplot(
        x='Imputed Values', 
        y='EVENT_AGE', 
        data=df_imputed, 
        color='red', 
        alpha=0.5, 
        label=f'Imputed Values (n={len(df_imputed):,})',
        ax=ax1,
        edgecolor=None,
        s=30
    )
    
    # Finalize Scatter Plot
    ax1.set_title(f"Measurement Values by Event Age: Original vs Imputed (OMOP {omop_id})\n3-sigma Filtering (Separate)", fontsize=14)
    ax1.set_xlabel("Measurement Value", fontsize=12)
    ax1.set_ylabel("Event Age", fontsize=12)
    ax1.legend(loc='upper right')
    
    # --- 3. DENSITY PLOT (3-SIGMA FILTERED) ---
    ax2 = axes[1]

    # Apply 3-sigma filtering separately
    original_filtered = sigma_filter(df['Original Values'], n_sigma=3)
    imputed_filtered = sigma_filter(df['Imputed Values'], n_sigma=3)
    
    # Create the merged series
    merged_data = pd.concat([original_filtered, imputed_filtered]).dropna()
    
    # Plot Kernel Density Estimates
    if not original_filtered.empty:
        sns.kdeplot(original_filtered, ax=ax2, color='blue', linewidth=2, label='Original Values')
    if not imputed_filtered.empty:
        sns.kdeplot(imputed_filtered, ax=ax2, color='red', linewidth=2, label='Imputed Values')
    if not merged_data.empty:
        sns.kdeplot(merged_data, ax=ax2, color='darkgreen', linewidth=1.5, linestyle='--', label='Merged Data')

    # Finalize Density Plot
    ax2.set_title("Density Distribution of Measurement Values\n3-sigma Filtering (Separate)", fontsize=14)
    ax2.set_xlabel("Measurement Value", fontsize=12)
    ax2.set_ylabel("Density", fontsize=12)
    ax2.legend(loc='upper right')
    
    # --- 4. SAVE PLOT ---
    output_filename = f"omop_{omop_id}_analysis.png"
    output_filepath = os.path.join(output_dir, output_filename)
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        plt.savefig(output_filepath, dpi=300)
        print(f"Successfully saved plot to: {output_filepath}")
    except Exception as e:
        print(f"Warning: Could not save file to {output_filepath}. Error: {e}")
        # Fallback to show plot if saving failed (e.g., permission error)
        plt.show()

def main():
    """Handles command-line arguments and data loading/analysis execution."""
    DEFAULT_FILE_PATH = "/home/pete/fg-3/kanta_v2/core/kanta_core_2025_05_22.parquet"
    
    parser = argparse.ArgumentParser(
        description="Analyze and plot extracted vs harmonized measurement values from a local Parquet file for a specific OMOP Concept ID."
    )
    
    # --- Parquet File Arguments (Optional) ---
    parser.add_argument(
        '--file_path', 
        type=str, 
        default=DEFAULT_FILE_PATH,
        help=f"Path to the input Parquet file. Defaults to '{DEFAULT_FILE_PATH}'."
    )
    
    # --- Analysis & Output Arguments ---
    parser.add_argument(
        '--omop_id', 
        type=int, 
        required=True, 
        help="OMOP Concept ID (integer) to filter the data by (e.g., 3020564)."
    )
    parser.add_argument(
        '--output_dir', 
        type=str, 
        default='.', 
        help="Directory to save the output plot (PNG file). Defaults to current directory (.)."
    )
    parser.add_argument(
        '--test_mode',
        action='store_true',
        help="If set, only the first 1000 rows of data will be loaded (via pandas.head()) for fast testing."
    )
    
    args = parser.parse_args()
    
    data = None
    row_limit = 1000 # Define the limit for test mode
    
    # 1. Load data from the Parquet file using a SQL query via DuckDB.
    input_file_path = args.file_path 
    
    # Construct the base SQL query to select required columns
    sql_query = f"""
        SELECT 
            MEASUREMENT_VALUE_HARMONIZED,
            MEASUREMENT_VALUE_EXTRACTED,
            OMOP_CONCEPT_ID,
            EVENT_AGE
        FROM '{input_file_path}'
    """
    
    # Apply test mode limit if requested
    if args.test_mode:
        sql_query += f" LIMIT {row_limit}"
        print(f"TEST MODE: Limiting data query to {row_limit} rows.")

    print(f"Attempting to load data from Parquet file using SQL: {input_file_path}")
    
    # Execute the query using DuckDB. Errors (like FileNotFoundError) will propagate here.
    conn = duckdb.connect()
    data = conn.execute(sql_query).fetchdf()
    conn.close()
            
    # 2. Execute analysis
    if data is not None:
        analyze_and_plot_omop_data(data, args.omop_id, args.output_dir)

if __name__ == "__main__":
    main()
