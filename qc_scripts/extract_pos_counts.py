#!/usr/bin/env python3
import pandas as pd
import sys
import os
import argparse

# Constants
DEFAULT_TEST_LINES = 1_000_000

def escape_sheets(val):
    s = str(val)
    if s.startswith(('+', '-', '=')):
        return f"'{s}"
    return s

def apply_reconciliation(new_df, ref_path, join_cols):
    """
    Standardizes join logic and calculates numerical ratios (New/Old).
    """
    # Initialize default columns
    new_df['ratio_COUNT'] = "NA"
    new_df['ratio_Npeople'] = "NA"
    new_df['NOTES'] = "NA"

    if not ref_path or not os.path.exists(ref_path):
        return new_df
    
    try:
        # Load ref - force string for keys
        ref_df = pd.read_csv(ref_path, sep='\t', dtype=str).fillna("NA")
        
        # Identify relevant columns in reference
        notes_col = next((c for c in ref_df.columns if c.upper() == "NOTES"), None)
        count_col = next((c for c in ref_df.columns if c.upper() == "COUNT"), None)
        nppl_col = next((c for c in ref_df.columns if c.upper() == "NPEOPLE"), None)
        
        # Create shadow keys for matching
        def make_shadow_key(df, cols):
            return df[cols].astype(str).apply(lambda x: "_".join(x.str.strip()), axis=1)

        new_df['_match_key'] = make_shadow_key(new_df, join_cols)
        ref_df['_match_key'] = make_shadow_key(ref_df, join_cols)

        # Drop duplicates in ref to prevent row explosion
        ref_df = ref_df.drop_duplicates('_match_key').set_index('_match_key')

        def get_ratio(new_val, old_series, key):
            try:
                if key not in old_series.index:
                    return "NA"
                old_val = float(old_series.get(key, 0))
                if old_val == 0:
                    return "NA"
                # Return as a float rounded to 3 decimal places for clean sorting
                return round(float(new_val) / old_val, 3)
            except:
                return "NA"

        # 1. Calculate Ratios
        if count_col:
            ref_counts = pd.to_numeric(ref_df[count_col], errors='coerce').fillna(0)
            new_df['ratio_COUNT'] = new_df.apply(
                lambda row: get_ratio(row['COUNT'], ref_counts, row['_match_key']), axis=1
            )
        
        if nppl_col:
            ref_nppl = pd.to_numeric(ref_df[nppl_col], errors='coerce').fillna(0)
            new_df['ratio_Npeople'] = new_df.apply(
                lambda row: get_ratio(row['Npeople'], ref_nppl, row['_match_key']), axis=1
            )

        # 2. Map Notes
        ref_keys = set(ref_df.index)
        def finalize_note(key):
            if key not in ref_keys:
                return "!! WARNING: NEW ENTRY !!"
            if notes_col:
                val = str(ref_df.loc[key, notes_col]).strip()
                return val if val not in ["NA", "nan", "None", ""] else "NA"
            return "NA"

        new_df['NOTES'] = new_df['_match_key'].apply(finalize_note)
        new_df.drop(columns=['_match_key'], inplace=True)
        
    except Exception as e:
        print(f"  Warning: Reconciliation failed: {e}")
        
    return new_df

def process_data(input_file, map_file, pn_orig=None, plus_orig=None, test_lines=None):
    # 1. Load OMOP mapping
    omop_map = {}
    if map_file and os.path.exists(map_file):
        try:
            m_df = pd.read_csv(map_file, dtype=str)
            omop_map = dict(zip(m_df['conceptId'], m_df['conceptName']))
        except Exception as e:
            print(f"Warning: Could not read map file: {e}")

    # 2. Identify Headers
    try:
        sample = pd.read_csv(input_file, sep='\t', nrows=0)
        actual_cols = sample.columns.tolist()
        col_map = {
            'id': next(c for c in actual_cols if c.upper() == 'FINNGENID'),
            'text': next(c for c in actual_cols if c.upper() == 'MEASUREMENT_FREE_TEXT'),
            'pos': next(c for c in actual_cols if c.upper() == 'EXTRACTED::IS_POS'),
            'omop': next(c for c in actual_cols if c.upper() == 'HARMONIZATION_OMOP::OMOP_ID')
        }
    except Exception as e:
        print(f"Error analyzing input headers: {e}")
        return

    # 3. Process Chunks
    chunksize = 500_000
    pn_frames = []
    plus_frames = []

    print(f"Reading {input_file}...")
    reader = pd.read_csv(input_file, sep='\t', usecols=list(col_map.values()), 
                         dtype=str, chunksize=chunksize, nrows=test_lines)

    for i, chunk in enumerate(reader):
        chunk = chunk.rename(columns={v: k for k, v in col_map.items()})
        chunk['id'] = chunk['id'].fillna("NA")
        chunk['text'] = chunk['text'].fillna("NA")
        chunk['pos'] = chunk['pos'].fillna("NA")
        chunk['omop'] = chunk['omop'].fillna("NA")
        
        pn_frames.append(chunk[chunk['text'].str.contains('pos|neg', case=False, na=False)].copy())
        plus_frames.append(chunk[(chunk['text'].str.contains(r'\+', na=False)) & (chunk['omop'] != "-1")].copy())
        print(f"  Processed { (i+1) * chunksize / 1_000_000 :.1f}M lines...", end='\r')

    print("\nAggregating...")

    # Column layout
    pn_cols = ['MEASUREMENT_FREE_TEXT', 'extracted::IS_POS', 'COUNT', 'ratio_COUNT', 'Npeople', 'ratio_Npeople', 'NOTES']
    pl_cols = ['harmonization_omop::OMOP_ID', 'MEASUREMENT_FREE_TEXT', 'extracted::IS_POS', 'DESC', 'COUNT', 'ratio_COUNT', 'Npeople', 'ratio_Npeople', 'NOTES']

    if pn_frames:
        pn_res = pd.concat(pn_frames).groupby(['text', 'pos']).agg(
            COUNT=('id', 'count'), Npeople=('id', 'nunique')).reset_index()
        pn_res = pn_res[pn_res['Npeople'] >= 5].sort_values('COUNT', ascending=False)
        pn_res = pn_res.rename(columns={'text': 'MEASUREMENT_FREE_TEXT', 'pos': 'extracted::IS_POS'})
        pn_res = apply_reconciliation(pn_res, pn_orig, ['MEASUREMENT_FREE_TEXT', 'extracted::IS_POS'])
        
        pn_res[pn_cols].to_csv("pos_neg_summary.tsv", sep='\t', index=False)
        pn_res[pn_cols].map(escape_sheets).to_csv("pos_neg_summary_pasteable.tsv", sep='\t', index=False)

    if plus_frames:
        pl_res = pd.concat(plus_frames).groupby(['omop', 'text', 'pos']).agg(
            COUNT=('id', 'count'), Npeople=('id', 'nunique')).reset_index()
        pl_res = pl_res[pl_res['Npeople'] >= 5].sort_values('COUNT', ascending=False)
        pl_res = pl_res.rename(columns={
            'omop': 'harmonization_omop::OMOP_ID',
            'text': 'MEASUREMENT_FREE_TEXT',
            'pos': 'extracted::IS_POS'
        })
        pl_res['DESC'] = pl_res['harmonization_omop::OMOP_ID'].map(lambda x: omop_map.get(x, "NOT_IN_MAP"))
        pl_res = apply_reconciliation(pl_res, plus_orig, 
                                      ['harmonization_omop::OMOP_ID', 'MEASUREMENT_FREE_TEXT', 'extracted::IS_POS'])
        
        pl_res[pl_cols].to_csv("plusplus_summary.tsv", sep='\t', index=False)
        pl_res[pl_cols].map(escape_sheets).to_csv("plusplus_summary_pasteable.tsv", sep='\t', index=False)

    print("Done.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("--map")
    parser.add_argument("--pn_orig")
    parser.add_argument("--plus_orig")
    parser.add_argument("--test", nargs='?', const=DEFAULT_TEST_LINES, type=int)
    args = parser.parse_args()
    process_data(args.input, args.map, args.pn_orig, args.plus_orig, args.test)
