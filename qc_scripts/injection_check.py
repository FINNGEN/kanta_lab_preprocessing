import gzip
import argparse
import sys
import pandas as pd
import numpy as np
import gc
import os
from scipy.stats import ks_2samp
from tqdm import tqdm

def get_deciles(series):
    if series is None or len(series) == 0:
        return "NA"
    arr = np.array(series, dtype=np.float32)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return "NA"
    return np.percentile(arr, np.linspace(0, 100, 11)).round(4).tolist()

def main():
    parser = argparse.ArgumentParser(description="Audit Unit Injection Success - Clean Reference Comparison")
    
    default_input = "~/fg-3/kanta_v3/munged/kanta_v3_harmonized_2026_02_02.txt.gz"
    default_output = "injection_check.tsv"
    
    parser.add_argument("input", nargs='?', default=default_input, help=f"Path to file (default: {default_input})")
    parser.add_argument("-o", "--output", default=default_output, help=f"Output TSV (default: {default_output})")
    parser.add_argument("--min_count", type=int, default=100, help="Min count to include in audit")
    parser.add_argument(
        "--test", 
        type=int, 
        nargs='?', 
        const=1000000, 
        default=None, # Changed to None so it runs full file unless --test is typed
        help="Limit lines. Use --test for 1M or --test <N>. If omitted, runs full file."
    )
    args = parser.parse_args()

    # Column Mapping
    COL_OMOP = "harmonization_omop::OMOP_ID"
    COL_TEST = "cleaned::TEST_NAME_ABBREVIATION"
    COL_UNIT_PRE = "cleaned-pre-fix::MEASUREMENT_UNIT"
    COL_UNIT_CLEAN = "cleaned::MEASUREMENT_UNIT"
    COL_VAL = "source::MEASUREMENT_VALUE"
    COL_HARM_VAL = "harmonization_omop::MEASUREMENT_VALUE"

    pre_samples = {}
    clean_ref_samples = {}

    input_path = os.path.expanduser(args.input)

    if not os.path.exists(input_path):
        print(f"Error: Input file not found at {input_path}")
        sys.exit(1)

    # If args.test is None, it reads the whole file
    reader = pd.read_csv(
        input_path, sep='\t', compression='gzip',
        usecols=[COL_OMOP, COL_TEST, COL_UNIT_PRE, COL_UNIT_CLEAN, COL_VAL, COL_HARM_VAL],
        chunksize=250_000, nrows=args.test, engine='c', low_memory=False,
        keep_default_na=False
    )

    # Use a descriptive pbar total
    total_val = args.test if args.test else None
    pbar = tqdm(total=total_val, desc="Auditing Injections")

    for chunk in reader:
        src_num = pd.to_numeric(chunk[COL_VAL], errors='coerce').astype(np.float32)
        harm_num = pd.to_numeric(chunk[COL_HARM_VAL], errors='coerce').astype(np.float32)
        
        chunk[COL_OMOP] = chunk[COL_OMOP].astype(str).replace(['nan', 'None', '', 'NA'], '-1')
        
        for col in [COL_UNIT_PRE, COL_UNIT_CLEAN]:
            chunk[col] = chunk[col].astype(str).replace(['nan', 'None', 'NA'], '')

        mask_changed = (chunk[COL_UNIT_PRE] != chunk[COL_UNIT_CLEAN]) & \
                       (chunk[COL_UNIT_PRE] != "") & \
                       (chunk[COL_UNIT_CLEAN] != "") & \
                       (src_num.notna()) & \
                       (chunk[COL_OMOP] != "-1")

        if mask_changed.any():
            c_data = chunk[mask_changed]
            c_vals = src_num[mask_changed]
            for (oid, abbr, u_pre, u_clean), idx in c_data.groupby([COL_OMOP, COL_TEST, COL_UNIT_PRE, COL_UNIT_CLEAN]).groups.items():
                key = (oid, abbr, u_pre, u_clean)
                if key not in pre_samples: pre_samples[key] = []
                pre_samples[key].append(c_vals.loc[idx].values)

        mask_ref = (~mask_changed) & (harm_num.notna()) & (chunk[COL_OMOP] != "-1")
        
        if mask_ref.any():
            r_data = chunk[mask_ref]
            r_vals = harm_num[mask_ref]
            for oid, idx in r_data.groupby(COL_OMOP).groups.items():
                if oid not in clean_ref_samples: clean_ref_samples[oid] = []
                clean_ref_samples[oid].append(r_vals.loc[idx].values)

        pbar.update(len(chunk))
        del chunk
        gc.collect()

    pbar.close()

    final_audit = []
    for (oid, abbr, u_pre, u_clean), list_of_arrays in pre_samples.items():
        all_injected = np.concatenate(list_of_arrays)
        if len(all_injected) < args.min_count: continue

        ref_arrays = clean_ref_samples.get(oid, [])
        all_clean_ref = np.concatenate(ref_arrays) if ref_arrays else np.array([], dtype=np.float32)

        res_row = {
            "OMOP_ID": oid,
            "TEST_ABBR": abbr,
            "PRE_UNIT": u_pre,
            "CLEAN_UNIT": u_clean,
            "N_INJECTED": len(all_injected),
            "N_PURE_REF": len(all_clean_ref),
            "INJECTED_DECILES": get_deciles(all_injected),
            "PURE_REF_DECILES": get_deciles(all_clean_ref),
            "KS_STAT": "NA",
            "STATUS": "No Ref Data"
        }

        if len(all_clean_ref) >= 20:
            ks_stat, _ = ks_2samp(all_injected, all_clean_ref)
            res_row["KS_STAT"] = f"{ks_stat:.4f}"
            
            if ks_stat < 0.2:
                res_row["STATUS"] = "SUCCESS"
            elif ks_stat < 0.4:
                res_row["STATUS"] = "WARNING"
            else:
                res_row["STATUS"] = "FAIL"

        final_audit.append(res_row)

    if final_audit:
        df_final = pd.DataFrame(final_audit).sort_values("N_INJECTED", ascending=False)
        df_final.to_csv(args.output, sep='\t', index=False)
        print(f"\nAudit complete. Processed {'full' if args.test is None else args.test} lines.")
        print(f"Results saved to: {os.path.abspath(args.output)}")
    else:
        print("\nNo unit injections found with sufficient data for auditing.")

if __name__ == "__main__":
    main()
