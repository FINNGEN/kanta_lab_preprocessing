#!/bin/python3
import argparse
import os
import psutil
import pandas as pd
import numpy as np
import duckdb
from tqdm import tqdm

def get_high_low_percentiles(df, percentile=5):
    high_keys, low_keys = ['H', 'HH'], ['L', 'LL']
    h_vals = df.loc[df['abnorm'].isin(high_keys), 'value'].values
    l_vals = df.loc[df['abnorm'].isin(low_keys), 'value'].values
    hp = np.percentile(h_vals, percentile) if h_vals.size > 0 else "NA"
    lp = np.percentile(l_vals, 100 - percentile) if l_vals.size > 0 else "NA"
    return str(lp), str(hp)

def return_bound_vectorized(df_sorted, t_hold, n_lines, num_keys, den_keys):
    subset = df_sorted.head(n_lines)
    if subset.empty: return "NA"
    vals, abnorm = subset['value'].values, subset['abnorm'].values
    is_num = np.isin(abnorm, num_keys)
    is_den = np.isin(abnorm, den_keys)
    run_num = np.cumsum(is_num)
    run_den = np.cumsum(is_den) + 0.0001
    ratios = run_num / run_den
    meaningful_mask = (abnorm != 'NA')
    bad_indices = np.where((ratios > t_hold) & meaningful_mask)[0]
    meaningful_indices = np.where(meaningful_mask)[0]
    
    is_valid, res_val = False, "NA"
    if meaningful_indices.size > 0:
        is_valid = (ratios[meaningful_indices[-1]] <= t_hold)
        if bad_indices.size > 0: res_val = vals[bad_indices[-1]]

    return str(res_val) if is_valid else f"{vals[-1]}*"

def process_group(omop_id, group, args):
    group['abnorm'] = group['abnorm'].fillna('NA')
    total_count = len(group)
    
    if total_count < args.min_count and not args.test: return None

    n_walk = int(total_count * args.max_walk)
    l_keys, l_den = ['A', 'AA', 'L', 'LL'], ['A', 'AA', 'L', 'LL', 'N', 'H', 'HH']
    h_keys, h_den = ['A', 'AA', 'H', 'HH'], ['A', 'AA', 'H', 'HH', 'N', 'L', 'LL']

    l_sort = group.sort_values(['value', 'abnorm'], ascending=[True, True])
    h_sort = group.sort_values(['value', 'abnorm'], ascending=[False, True])
    
    low_ests = {t: return_bound_vectorized(l_sort, t, n_walk, l_keys, l_den) for t in args.thresholds}
    high_ests = {t: return_bound_vectorized(h_sort, t, n_walk, h_keys, h_den) for t in args.thresholds}
    lp, hp = get_high_low_percentiles(group, args.percentile)
    
    return {
        'ID': str(omop_id), 'ENTRIES': total_count, 
        'LOW_EST': low_ests, 'HIGH_EST': high_ests, 
        'LOW_P': lp, 'HIGH_P': hp, 'COUNTS': group['abnorm'].value_counts().to_dict()
    }

def get_system_config():
    """Dynamically allocate resources based on system capabilities"""
    cpu_count = psutil.cpu_count(logical=True)  # Use all logical CPUs
    total_memory = psutil.virtual_memory().total
    
    # Use 75% of available CPUs (leave headroom for OS)
    threads = max(1, int(cpu_count * 0.75))
    
    # Use 50% of available RAM for DuckDB (leave headroom for OS and other processes)
    memory_limit_gb = int(total_memory * 0.5 / (1024**3))
    memory_limit_gb = max(2, min(memory_limit_gb, 64))  # Clamp between 2GB and 64GB
    
    return threads, memory_limit_gb

def select_threshold(entries, thresholds):
    """Select appropriate threshold based on entry count"""
    if int(entries) > 100000:
        return max(thresholds)  # Use highest threshold (.99)
    else:
        return 0.95  # Use .95 as starter

def main(args):
    # Get dynamic system configuration
    threads, memory_limit_gb = get_system_config()
    print(f"System detected: {psutil.cpu_count()} CPUs, {psutil.virtual_memory().total / (1024**3):.1f}GB RAM")
    print(f"Allocating: {threads} threads, {memory_limit_gb}GB memory")
    print(f"Batch size: {args.batch_size}M rows per batch")
    
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute(f"PRAGMA memory_limit='{memory_limit_gb}GB'")
    
    # Set temp directory size to available disk space
    temp_dir = os.getcwd()
    disk_usage = psutil.disk_usage(temp_dir)
    available_gb = int(disk_usage.free / (1024**3))
    con.execute(f"SET max_temp_directory_size='{available_gb}GB'")
    con.execute(f"SET temp_directory='{temp_dir}'")
    
    print(f"Scanning Parquet to identify IDs with at least {args.min_count} entries...")
    
    t_filt = f"AND OMOP_CONCEPT_ID IN (3008486, 3009201, 3027238, 3032333, 3023199, 3020460, 3018572)" if args.test else ""
    
    # Get IDs WITH their row counts for dynamic batching
    count_query = f"""
        SELECT CAST(OMOP_CONCEPT_ID AS INTEGER) as omop_id, COUNT(*) as row_count
        FROM read_parquet('{args.parquet_file}')
        WHERE OMOP_CONCEPT_ID IS NOT NULL
          AND MEASUREMENT_VALUE_HARMONIZED IS NOT NULL
          {t_filt}
        GROUP BY OMOP_CONCEPT_ID
        HAVING COUNT(*) >= {args.min_count}
        ORDER BY OMOP_CONCEPT_ID
    """
    
    all_ids_with_counts = con.execute(count_query).fetchall()
    num_ids = len(all_ids_with_counts)
    
    if num_ids == 0:
        print("No IDs found matching the criteria. Exiting.")
        return

    print(f"Found {num_ids} IDs to process (meeting min_count={args.min_count}).")
    
    # Dynamic batching: group IDs until total row count threshold
    max_rows_per_batch = args.batch_size * 1_000_000  # Convert M to rows
    batches = []
    current_batch = []
    current_row_count = 0
    
    for omop_id, row_count in all_ids_with_counts:
        if current_row_count + row_count > max_rows_per_batch and current_batch:
            # Start new batch
            batches.append(current_batch)
            current_batch = [omop_id]
            current_row_count = row_count
        else:
            current_batch.append(omop_id)
            current_row_count += row_count
    
    if current_batch:
        batches.append(current_batch)
    
    print(f"Created {len(batches)} dynamic batches")
    
    results = []
    
    for batch_idx, batch_ids in tqdm(enumerate(batches), desc="Processing ID batches", total=len(batches), unit="batch"):
        id_list = ','.join(map(str, batch_ids))
        
        # Fetch only this batch of IDs
        batch_query = f"""
            SELECT 
                CAST(OMOP_CONCEPT_ID AS INTEGER) as omop_id,
                CAST(MEASUREMENT_VALUE_HARMONIZED AS DOUBLE) as value,
                CAST(TEST_OUTCOME AS VARCHAR) as abnorm
            FROM read_parquet('{args.parquet_file}')
            WHERE OMOP_CONCEPT_ID IN ({id_list})
              AND MEASUREMENT_VALUE_HARMONIZED IS NOT NULL
            ORDER BY OMOP_CONCEPT_ID, value
        """
        
        df_batch = con.execute(batch_query).df()
        
        # Process this batch's groups
        for omop_id, group in df_batch.groupby('omop_id'):
            res = process_group(omop_id, group.drop(columns=['omop_id']), args)
            if res:
                results.append(res)
    
    # --- SAVE DETAILED TSV (--ab_ranges) ---
    with open(args.ab_ranges, 'wt') as f:
        # Build header dynamically based on thresholds
        threshold_cols = []
        for t in sorted(args.thresholds):
            threshold_cols.append(f'LOWER_{t}')
        for t in sorted(args.thresholds):
            threshold_cols.append(f'UPPER_{t}')
        
        header = ['ID'] + threshold_cols + ['LOW_5', 'HIGH_95', 'ENTRIES', 'COUNTS']
        f.write('\t'.join(header) + '\n')
        
        for r in results:
            row_data = [r['ID']]
            
            # Add LOWER estimates for each threshold
            for t in sorted(args.thresholds):
                val = r['LOW_EST'][t]
                # Convert NA to "NA", remove asterisks
                if val == "NA":
                    row_data.append("NA")
                else:
                    row_data.append(str(float(val.replace('*', ''))))
            
            # Add UPPER estimates for each threshold
            for t in sorted(args.thresholds):
                val = r['HIGH_EST'][t]
                # Convert NA to "NA", remove asterisks
                if val == "NA":
                    row_data.append("NA")
                else:
                    row_data.append(str(float(val.replace('*', ''))))
            
            # Add percentile bounds
            row_data.append(r['LOW_P'])
            row_data.append(r['HIGH_P'])
            
            # Add entries count
            row_data.append(str(r['ENTRIES']))
            
            # Add counts dictionary as string
            row_data.append(str(r['COUNTS']))
            
            f.write('\t'.join(row_data) + '\n')
    
    # --- SAVE SIMPLIFIED TSV (--ab_table) ---
    with open(args.ab_table, 'wt') as f:
        # Write header
        f.write('\t'.join(['ID', 'LOW_LIMIT', 'HIGH_LIMIT', 'LOW_PROBLEM', 'HIGH_PROBLEM']) + '\n')
        
        for r in results:
            entries = r['ENTRIES']
            
            # Select threshold based on entry count
            if entries > 100000:
                selected_threshold = max(args.thresholds)  # Use highest (.99)
            else:
                selected_threshold = 0.95  # Use .95 as starter
            
            # Get the estimates for selected threshold
            low_col = r['LOW_EST'][selected_threshold]
            high_col = r['HIGH_EST'][selected_threshold]
            
            # For smaller datasets, try .99 if .95 is problematic
            if entries <= 100000:
                if '*' in low_col:
                    new_low = r['LOW_EST'].get(max(args.thresholds), low_col)
                    low_col = new_low if '*' not in new_low else low_col
                if '*' in high_col:
                    new_high = r['HIGH_EST'].get(max(args.thresholds), high_col)
                    high_col = new_high if '*' not in new_high else high_col
            
            # Check if problematic (has asterisk)
            low_problem = 1 if '*' in low_col else 0
            high_problem = 1 if '*' in high_col else 0
            
            # Map NAs to +/- inf and remove asterisks
            low_res = -np.inf if low_col == "NA" else float(low_col.replace('*', ''))
            high_res = np.inf if high_col == "NA" else float(high_col.replace('*', ''))
            
            # Write results
            row = [r['ID'], low_res, high_res, low_problem, high_problem]
            f.write('\t'.join(map(str, row)) + '\n')
            
    print(f"\nDone. {len(results)} IDs processed.")
    print(f"Simplified results saved to {args.ab_table}.")
    print(f"Detailed results saved to {args.ab_ranges}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Estimate abnormality bounds from medical measurement data")
    parser.add_argument('--parquet_file', required=True, help="Path to input Parquet file")
    parser.add_argument('--ab_table', default="abnormality_estimation.table.tsv",
                        help="Filename for simplified results (default: abnormality_estimation.table.tsv)")
    parser.add_argument('--ab_ranges', default="abnormality_estimation.txt",
                        help="Filename for detailed results (default: abnormality_estimation.txt)")
    parser.add_argument('--min-count', default=1, type=int, 
                        help="Minimum number of entries per ID (default: 1)")
    parser.add_argument('--percentile', default=5, type=int,
                        help="Percentile for bounds calculation (default: 5)")
    parser.add_argument('--max-walk', default=.5, type=float,
                        help="Maximum walk parameter (default: 0.5)")
    parser.add_argument('--batch-size', default=10, type=int, 
                        help="Max rows per batch in millions (default: 10M)")
    parser.add_argument('--thresholds', default=[0.9, 0.95, 0.99], nargs='*', type=float,
                        help="Thresholds for estimation (default: 0.9 0.95 0.99)")
    parser.add_argument("--test", action='store_true', help="Run in test mode with limited IDs")
    main(parser.parse_args())
