import gzip
import argparse
import sys
import pandas as pd
import numpy as np
import gc
from collections import Counter
from scipy.stats import ks_2samp
from tqdm import tqdm

def get_deciles(series):
    if series is None or len(series) == 0:
        return "NA"
    # Using numpy for speed and memory efficiency
    arr = np.array(series, dtype=np.float32)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return "NA"
    # np.percentile takes 0-100
    return np.percentile(arr, np.linspace(0, 100, 11)).round(4).tolist()

def main():
    parser = argparse.ArgumentParser(description="Full Audit - Memory Optimized - Keeps All Samples")
    parser.add_argument("input", help="Path to the kanta_..._munged.txt.gz file")
    parser.add_argument("-u", "--unmapped-output", required=True, help="Output TSV for unmapped tests")
    parser.add_argument("-a", "--audit-output", required=True, help="Output TSV for the audited mismatches")
    parser.add_argument("--min_count", type=int, default=1000, help="Min count to include in audit")
    parser.add_argument(
        "--test", 
        type=int, 
        nargs='?', 
        const=1000000, 
        default=None, 
        help="Limit lines. Use --test for 1M or --test <N>."
    )
    args = parser.parse_args()

    COL_OMOP = "harmonization_omop::OMOP_ID"
    COL_TEST = "cleaned::TEST_NAME_ABBREVIATION"
    COL_UNIT = "cleaned::MEASUREMENT_UNIT"
    COL_SRC_VAL = "source::MEASUREMENT_VALUE"
    COL_HARM_VAL = "harmonization_omop::MEASUREMENT_VALUE"
    COL_HARM_UNIT = "harmonization_omop::MEASUREMENT_UNIT"

    unmapped_counts = Counter()
    mapped_test_names = set()
    
    src_samples = {}
    harm_ref_samples = {}
    harm_unit_counts = {}

    # Chunksize 250k is usually the "sweet spot" for 32GB VMs
    reader = pd.read_csv(
        args.input, sep='\t', compression='gzip',
        usecols=[COL_OMOP, COL_TEST, COL_UNIT, COL_SRC_VAL, COL_HARM_VAL, COL_HARM_UNIT],
        chunksize=250_000, nrows=args.test, engine='c', low_memory=False,
        keep_default_na=False
    )

    pbar = tqdm(total=args.test, desc="Lines Processed")

    for chunk in reader:
        # Pre-convert for memory efficiency (float32 saves 50% vs float64)
        s_num = pd.to_numeric(chunk[COL_SRC_VAL], errors='coerce').astype(np.float32)
        h_num = pd.to_numeric(chunk[COL_HARM_VAL], errors='coerce').astype(np.float32)
        chunk[COL_OMOP] = chunk[COL_OMOP].astype(str).replace(['nan', 'None', '', 'NA'], '-1')
        
        # 1. Unmapped Logic
        u_mask = chunk[COL_OMOP] == "-1"
        if u_mask.any():
            u_batch = chunk[u_mask].groupby([COL_TEST, COL_UNIT]).size()
            for (t_name, t_unit), count in u_batch.items():
                unmapped_counts[(t_name, t_unit)] += count
        
        # 2. Mismatch Logic
        mapped_mask = ~u_mask
        mapped_test_names.update(chunk.loc[mapped_mask, COL_TEST].unique())
        
        # Mismatch = Mapped, Source is numeric, but Harmonized is NaN
        m_mask = mapped_mask & s_num.notna() & h_num.isna()
        if m_mask.any():
            m_data = chunk[m_mask]
            m_vals = s_num[m_mask]
            # Use index groups for fast slicing
            for (oid, abbr, unit), idx in m_data.groupby([COL_OMOP, COL_TEST, COL_UNIT]).groups.items():
                key = (oid, abbr, unit)
                if key not in src_samples: src_samples[key] = []
                # Store as numpy arrays inside the list to reduce Python object overhead
                src_samples[key].append(m_vals.loc[idx].values)

        # 3. Reference Logic (Successes)
        h_mask = h_num.notna()
        if h_mask.any():
            h_data = chunk[h_mask]
            h_vals = h_num[h_mask]
            for oid, idx in h_data.groupby(COL_OMOP).groups.items():
                if oid not in harm_ref_samples:
                    harm_ref_samples[oid] = []
                    harm_unit_counts[oid] = Counter()
                harm_unit_counts[oid].update(h_data.loc[idx, COL_HARM_UNIT].astype(str).tolist())
                harm_ref_samples[oid].append(h_vals.loc[idx].values)

        pbar.update(len(chunk))
        del chunk
        gc.collect()

    pbar.close()

    # --- Write Unmapped ---
    unmapped_data = [
        {COL_TEST: k[0], COL_UNIT: k[1], 'COUNT': v, 'HAS_ANY_MAPPING': k[0] in mapped_test_names}
        for k, v in unmapped_counts.items()
    ]
    if unmapped_data:
        pd.DataFrame(unmapped_data).sort_values('COUNT', ascending=False).to_csv(args.unmapped_output, sep='\t', index=False)

    # --- Write Audit ---
    final_audit = []
    for (oid, abbr, unit), list_of_arrays in src_samples.items():
        # Flatten all chunks back into a single array
        all_src = np.concatenate(list_of_arrays)
        n_src = len(all_src)
        if n_src < args.min_count: continue
        
        ref_arrays = harm_ref_samples.get(oid, [])
        all_harm = np.concatenate(ref_arrays) if ref_arrays else np.array([], dtype=np.float32)
        
        u_counts = harm_unit_counts.get(oid, Counter())
        clean_u = {k: v for k, v in u_counts.items() if str(k).lower() not in ['nan', 'none', 'na', '', '-1']}
        most_common_unit = max(clean_u, key=clean_u.get) if clean_u else "NA"

        res_row = {
            "OMOP_ID": oid,
            "TEST_ABBR": abbr,
            "ORIG_UNIT": unit,
            "N_SOURCE": n_src,
            "N_HARM": len(all_harm),
            "SUGGESTED_UNIT": "",
            "NOTES": "",
            "SOURCE_DECILES": get_deciles(all_src),
            "HARM_DECILES": get_deciles(all_harm),
            "KS_STAT": "NA"
        }

        if len(all_harm) >= 5: 
            # Drop NaNs just in case for KS test
            s_clean = all_src[~np.isnan(all_src)]
            if s_clean.size > 0:
                ks_stat, _ = ks_2samp(s_clean, all_harm)
                res_row["KS_STAT"] = f"{ks_stat:.4f}"
                if ks_stat < 0.3:
                    res_row["SUGGESTED_UNIT"], res_row["NOTES"] = most_common_unit, "SUCCESS"
                else:
                    res_row["NOTES"] = "Distributions differ"
            else:
                res_row["NOTES"] = "No valid numeric source values"
        else:
            res_row["NOTES"] = "No harmonized reference data"

        final_audit.append(res_row)

    if final_audit:
        df_audit = pd.DataFrame(final_audit).sort_values("N_SOURCE", ascending=False)
        df_audit.to_csv(args.audit_output, sep='\t', index=False, na_rep='NA')
    else:
        with open(args.audit_output, 'w') as f:
            f.write("OMOP_ID\tTEST_ABBR\tORIG_UNIT\tN_SOURCE\tN_HARM\tSUGGESTED_UNIT\tNOTES\tSOURCE_DECILES\tHARM_DECILES\tKS_STAT\n")

    print(f"\nAudit complete. All samples preserved. Processed {args.test if args.test else 'full'} file.")

if __name__ == "__main__":
    main()
