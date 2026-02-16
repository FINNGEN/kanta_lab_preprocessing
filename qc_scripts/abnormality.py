#!/bin/python3
import argparse
import os
import pandas as pd
import numpy as np
import duckdb
from tqdm import tqdm

def get_high_low_percentiles(df, percentile=5):
    """
    lp = 95th percentile of Lows (the 'highest' low)
    hp = 5th percentile of Highs (the 'lowest' high)
    """
    high_keys, low_keys = ['H', 'HH'], ['L', 'LL']
    h_vals = df.loc[df['abnorm'].isin(high_keys), 'value'].values
    l_vals = df.loc[df['abnorm'].isin(low_keys), 'value'].values
    
    hp = np.percentile(h_vals, percentile) if h_vals.size > 0 else "NA"
    lp = np.percentile(l_vals, 100 - percentile) if l_vals.size > 0 else "NA"
    return str(lp), str(hp)


def return_bound_vectorized(df_sorted, t_hold, n_lines, num_keys, den_keys):
    # 1. Take the budget from the TOP of the file (including NA rows)
    # This 'subset' represents the lines the loop would have visited.
    subset = df_sorted.head(n_lines)
    if subset.empty: 
        return "NA"
    
    vals = subset['value'].values
    abnorm = subset['abnorm'].values
    
    # 2. Identify Numerators and Denominators
    is_num = np.isin(abnorm, num_keys)
    is_den = np.isin(abnorm, den_keys)
    
    # 3. Calculate running counts and ratios
    # NAs don't increase num or den, but they occupy a row index (a line)
    run_num = np.cumsum(is_num)
    run_den = np.cumsum(is_den) + 0.0001
    ratios = run_num / run_den
    
    # 4. Filter for rows where status != 'NA'
    # This matches your 'if status != "NA"' block
    meaningful_mask = (abnorm != 'NA')
    
    # Identify indices where ratio > threshold AND it's a meaningful row
    # This updates your 'res' candidate
    bad_indices = np.where((ratios > t_hold) & meaningful_mask)[0]
    
    # Determine 'is_valid' based on the VERY LAST meaningful row seen in the budget
    meaningful_indices = np.where(meaningful_mask)[0]
    is_valid = False
    res_val = "NA"
    
    if meaningful_indices.size > 0:
        last_meaningful_idx = meaningful_indices[-1]
        is_valid = (ratios[last_meaningful_idx] <= t_hold)
        
        # If there were any bad rows, res_val is the value of the last one
        if bad_indices.size > 0:
            res_val = vals[bad_indices[-1]]

    # 5. The Return Logic
    if is_valid:
        return str(res_val)
    else:
        # Match the iterative 'else: return value + "*"'
        # 'value' in the loop is the last row processed (the budget limit)
        final_value_in_budget = vals[-1]
        return f"{final_value_in_budget}*"


def main(args):
    con = duckdb.connect()
    t_filt = f"AND OMOP_CONCEPT_ID IN (3008486, 3009201, 3027238, 3032333, 3023199, 3020460, 3018572)" if args.test else ""
    
    query = f"""
        SELECT 
            CAST(OMOP_CONCEPT_ID AS INTEGER) as omop,
            CAST(MEASUREMENT_VALUE_HARMONIZED AS DOUBLE) as value,
            CAST(TEST_OUTCOME AS VARCHAR) as abnorm
        FROM read_parquet('{args.parquet_file}')
        WHERE OMOP_CONCEPT_ID IS NOT NULL 
          AND MEASUREMENT_VALUE_HARMONIZED IS NOT NULL
        {t_filt}
    """
    
    print("Loading Numeric Data...")
    df = con.execute(query).df()
    df['abnorm'] = df['abnorm'].fillna('NA').astype('category')
    
    results = []
    l_keys, l_den = ['A', 'AA', 'L', 'LL'], ['A', 'AA', 'L', 'LL', 'N', 'H', 'HH']
    h_keys, h_den = ['A', 'AA', 'H', 'HH'], ['A', 'AA', 'H', 'HH', 'N', 'L', 'LL']

    print("Processing...")
    for omop_id, group in tqdm(df.groupby('omop')):
        # Total numeric entries (includes rows where abnorm is 'NA')
        total_count = len(group)
        
        if total_count < args.min_count and not args.test:
            continue
            
        # IMPORTANT: To match your old script, the walk limit must be based 
        # on total lines, so that 'NA' rows "waste" the budget.
        n_walk = int(total_count * args.max_walk)
        
        # Stabilized sorting
        l_sort = group.sort_values(['value', 'abnorm'], ascending=[True, True])
        h_sort = group.sort_values(['value', 'abnorm'], ascending=[False, True])
        
        low_ests = {t: return_bound_vectorized(l_sort, t, n_walk, l_keys, l_den) for t in args.thresholds}
        high_ests = {t: return_bound_vectorized(h_sort, t, n_walk, h_keys, h_den) for t in args.thresholds}
        lp, hp = get_high_low_percentiles(group, args.percentile)
        
        counts_dict = group['abnorm'].value_counts().to_dict()
        
        results.append({
            'ID': str(omop_id), 'ENTRIES': total_count, 
            'LOW_EST': low_ests, 'HIGH_EST': high_ests, 
            'LOW_P': lp, 'HIGH_P': hp, 'COUNTS': counts_dict
        })

    os.makedirs(args.out, exist_ok=True)

    txt_path = os.path.join(args.out, 'abnormality_estimation.txt')
    with open(txt_path, 'wt') as f:
        header = ['ID']
        for t in args.thresholds: header += [f"LOWER_{t}", f"UPPER_{t}"]
        header += [f"LOW_{args.percentile}", f"HIGH_{100-args.percentile}", 'ENTRIES', 'COUNTS']
        f.write('\t'.join(header) + '\n')
        for r in results:
            row = [r['ID']]
            for t in args.thresholds: row += [r['LOW_EST'][t], r['HIGH_EST'][t]]
            row += [r['LOW_P'], r['HIGH_P'], r['ENTRIES'], str(r['COUNTS'])]
            f.write('\t'.join(map(str, row)) + '\n')

    table_path = os.path.join(args.out, 'abnormality_estimation.table.tsv')
    with open(table_path, 'wt') as o:
        o.write('\t'.join(["ID", 'LOW_LIMIT', 'HIGH_LIMIT', 'LOW_PROBLEM', 'HIGH_PROBLEM']) + '\n')
        for res in results:
            t_pick = 0.99 if res['ENTRIES'] > 100000 else 0.95
            l_str = res['LOW_EST'][t_pick]
            h_str = res['HIGH_EST'][t_pick]
            
            if res['ENTRIES'] <= 100000:
                if '*' in l_str:
                    alt = res['LOW_EST'][0.99]
                    l_str = alt if '*' not in alt else l_str
                if '*' in h_str:
                    alt = res['HIGH_EST'][0.99]
                    h_str = alt if '*' not in alt else h_str

            low_val = -np.inf if l_str == "NA" else l_str.replace('*', '')
            high_val = np.inf if h_str == "NA" else h_str.replace('*', '')
            
            o.write('\t'.join(map(str, [
                res['ID'], low_val, high_val, 
                1 if '*' in l_str else 0, 1 if '*' in h_str else 0
            ])) + '\n')

    print(f"Done. Outputs saved to {args.out}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--parquet_file', required=True)
    parser.add_argument('--out', default="/mnt/disks/data/kanta/abnorm/")
    parser.add_argument('--min-count', default=1, type=int) 
    parser.add_argument('--percentile', default=5, type=int)
    parser.add_argument('--max-walk', default=.5, type=float)
    parser.add_argument('--thresholds', default=[0.9, 0.95, 0.99], nargs='*', type=float)
    parser.add_argument("--test", action='store_true')
    main(parser.parse_args())
