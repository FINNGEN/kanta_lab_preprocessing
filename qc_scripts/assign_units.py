import pandas as pd
import numpy as np
import argparse
import os
from scipy.stats import ks_2samp
from tqdm import tqdm
from collections import Counter

def get_deciles(series):
    if series is None or len(series) == 0:
        return "NA"
    clean_series = pd.to_numeric(pd.Series(series), errors='coerce').dropna()
    if clean_series.empty:
        return "NA"
    return clean_series.quantile(np.linspace(0, 1, 11)).round(4).tolist()

def main():
    parser = argparse.ArgumentParser(description="Vectorized Audit Harmonization.")
    parser.add_argument("--data", required=True, help="Path to the large .txt.gz data file")
    parser.add_argument("--audit_list", required=True, help="The unharmonized table")
    parser.add_argument("--output", default="harmonization_audit_results.tsv")
    parser.add_argument("--min_count", type=int, default=1000)
    parser.add_argument("--test", nargs='?', const=1000000, type=int)
    args = parser.parse_args()

    # Column Constants
    COL_OMOP_ID = "harmonization_omop::OMOP_ID"
    COL_HARM_VAL = "harmonization_omop::MEASUREMENT_VALUE"
    COL_HARM_UNIT = "harmonization_omop::MEASUREMENT_UNIT"
    COL_SRC_VAL = "cleaned::MEASUREMENT_VALUE"
    COL_SRC_ABBR = "cleaned::TEST_NAME_ABBREVIATION"
    COL_SRC_UNIT = "cleaned::MEASUREMENT_UNIT"
    COL_COUNT = "COUNT"

    # 1. Load and Filter Audit List
    targets = pd.read_csv(args.audit_list, sep='\t', keep_default_na=False)
    actual_count_col = next((c for c in [COL_COUNT, 'n', 'count'] if c in targets.columns), None)
    
    if actual_count_col:
        targets = targets[pd.to_numeric(targets[actual_count_col], errors='coerce').fillna(0) >= args.min_count].copy()

    # Pre-calculate target sets for fast filtering
    target_omops = set(targets[COL_OMOP_ID].astype(str))
    # Map to help us identify which (OMOP, ABBR) combinations we care about
    valid_combinations = set(zip(targets[COL_OMOP_ID].astype(str), targets[COL_SRC_ABBR]))

    # Data structures for results
    src_samples = {f"{r[COL_OMOP_ID]}_{r[COL_SRC_ABBR]}": [] for _, r in targets.iterrows()}
    harm_ref_samples = {oid: [] for oid in target_omops}
    harm_unit_counts = {oid: Counter() for oid in target_omops}

    # 2. Vectorized Streaming Pass
    reader = pd.read_csv(
        args.data, sep='\t', 
        usecols=[COL_OMOP_ID, COL_HARM_VAL, COL_HARM_UNIT, COL_SRC_VAL, COL_SRC_ABBR],
        chunksize=500_000, # Increased chunksize for better vectorization
        nrows=args.test, 
        engine='c', 
        low_memory=False
    )

    SAMPLE_CAP = 100_000

    with tqdm(total=args.test, desc="Vectorized Streaming") as pbar:
        for chunk in reader:
            chunk[COL_OMOP_ID] = chunk[COL_OMOP_ID].astype(str)
            
            # --- PASS A: Vectorized Harmonized Reference ---
            h_mask = chunk[COL_HARM_VAL].notna() & chunk[COL_OMOP_ID].isin(target_omops)
            if h_mask.any():
                h_groups = chunk[h_mask].groupby(COL_OMOP_ID)
                for oid, group in h_groups:
                    # Update unit counts
                    harm_unit_counts[oid].update(group[COL_HARM_UNIT].astype(str).tolist())
                    # Update samples
                    if len(harm_ref_samples[oid]) < SAMPLE_CAP:
                        vals = pd.to_numeric(group[COL_HARM_VAL], errors='coerce').dropna().tolist()
                        harm_ref_samples[oid].extend(vals[:10000])

            # --- PASS B: Vectorized Source Audit ---
            s_mask = chunk[COL_HARM_VAL].isna() & chunk[COL_SRC_VAL].notna() & chunk[COL_OMOP_ID].isin(target_omops)
            if s_mask.any():
                # Group by both columns to find matches in one operation
                s_groups = chunk[s_mask].groupby([COL_OMOP_ID, COL_SRC_ABBR])
                for (oid, abbr), group in s_groups:
                    key = f"{oid}_{abbr}"
                    if key in src_samples and len(src_samples[key]) < SAMPLE_CAP:
                        vals = pd.to_numeric(group[COL_SRC_VAL], errors='coerce').dropna().tolist()
                        src_samples[key].extend(vals[:10000])
            
            pbar.update(len(chunk))

    # 3. Final Calculations (remains the same)
    final_results = []
    for _, row in targets.iterrows():
        oid, abbr = str(row[COL_OMOP_ID]), str(row[COL_SRC_ABBR])
        key = f"{oid}_{abbr}"
        s_vals, h_vals = pd.Series(src_samples.get(key, [])), pd.Series(harm_ref_samples.get(oid, []))
        
        u_counts = harm_unit_counts.get(oid)
        clean_u = {k: v for k, v in u_counts.items() if str(k).lower() not in ['nan', 'none', 'na', '']}
        most_common_unit = max(clean_u, key=clean_u.get) if clean_u else "NA"
        
        res_row = {
            "harmonization_omop::OMOP_ID": oid,
            "cleaned::TEST_NAME_ABBREVIATION": abbr,
            "cleaned::MEASUREMENT_UNIT": row.get(COL_SRC_UNIT, "NA"),
            "COUNT": int(row.get(actual_count_col, 0)),
            "NEW_UNIT": "", "NOTES": "",
            "N_SOURCE": len(s_vals), "N_HARM": len(h_vals),
            "SOURCE_DECILES": get_deciles(s_vals), "HARM_DECILES": get_deciles(h_vals),
            "KS_STAT": "NA"
        }
        
        if not h_vals.empty and not s_vals.empty:
            ks_stat, _ = ks_2samp(s_vals, h_vals)
            res_row["KS_STAT"] = f"{ks_stat:.4f}"
            if ks_stat < 0.3:
                res_row["NEW_UNIT"], res_row["NOTES"] = most_common_unit, "success"
            else:
                res_row["NOTES"] = "Distributions differ (KS >= 0.3)"
        elif h_vals.empty:
            res_row["NOTES"] = "No harmonized reference data"
        else:
            res_row["NOTES"] = "Insufficient source data"

        final_results.append(res_row)

    pd.DataFrame(final_results).to_csv(args.output, sep='\t', index=False)
    print(f"Audit complete. Results saved to: {args.output}")

if __name__ == "__main__":
    main()
