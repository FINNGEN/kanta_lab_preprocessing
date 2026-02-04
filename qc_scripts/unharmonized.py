import gzip
import argparse
import sys
import pandas as pd
import numpy as np
from collections import Counter
from scipy.stats import ks_2samp
from tqdm import tqdm

def get_deciles(series):
    if series is None or len(series) == 0:
        return "NA"
    clean_series = pd.to_numeric(pd.Series(series), errors='coerce').dropna()
    if clean_series.empty:
        return "NA"
    return clean_series.quantile(np.linspace(0, 1, 11)).round(4).tolist()

def main():
    parser = argparse.ArgumentParser(description="Vectorized Audit - Sorted by N_SOURCE.")
    parser.add_argument("input", help="Path to the kanta_..._munged.txt.gz file")
    parser.add_argument("-u", "--unmapped-output", required=True, help="Output TSV for unmapped tests")
    parser.add_argument("-a", "--audit-output", required=True, help="Output TSV for the audited mismatches")
    parser.add_argument("--min_count", type=int, default=1000, help="Min count to include in audit (default: 100)")
    parser.add_argument(
    "--test", 
    type=int, 
    nargs='?',       # Allows 0 or 1 arguments
    const=1000000,   # Value if --test is present but no number is given
    default=None,    # Value if --test is not present at all (or use sys.maxsize)
    help="Limit number of lines to process. Use --test for default 1M or --test <N>."
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
    
    # Audit storage
    src_samples = {}
    harm_ref_samples = {}
    harm_unit_counts = {}

    reader = pd.read_csv(
        args.input, sep='\t', compression='gzip',
        usecols=[COL_OMOP, COL_TEST, COL_UNIT, COL_SRC_VAL, COL_HARM_VAL, COL_HARM_UNIT],
        chunksize=250_000, nrows=args.test, engine='c', low_memory=False,
        keep_default_na=False
    )

    # Progress bar tracking lines
    pbar = tqdm(total=args.test, desc="Lines Processed")

    for chunk in reader:
        # Pre-convert to numeric for identification
        s_num = pd.to_numeric(chunk[COL_SRC_VAL], errors='coerce')
        h_num = pd.to_numeric(chunk[COL_HARM_VAL], errors='coerce')
        chunk[COL_OMOP] = chunk[COL_OMOP].astype(str).replace(['nan', 'None', '', 'NA'], '-1')
        
        # 1. Unmapped Logic
        u_mask = chunk[COL_OMOP] == "-1"
        if u_mask.any():
            u_batch = chunk[u_mask].groupby([COL_TEST, COL_UNIT]).size()
            for (t_name, t_unit), count in u_batch.items():
                unmapped_counts[(t_name, t_unit)] += count
        
        # 2. Mismatch/Audit Logic
        mapped_mask = ~u_mask
        mapped_test_names.update(chunk.loc[mapped_mask, COL_TEST].unique())
        
        # Mismatch = Mapped, Source is numeric, but Harmonized is NaN
        m_mask = mapped_mask & s_num.notna() & h_num.isna()
        
        if m_mask.any():
            m_groups = chunk[m_mask].groupby([COL_OMOP, COL_TEST, COL_UNIT])
            for (oid, abbr, unit), group in m_groups:
                key = (oid, abbr, unit)
                if key not in src_samples: src_samples[key] = []
                # Keep all samples for the count, even if NaN in numeric context later
                src_samples[key].extend(s_num[group.index].tolist())

        # 3. Reference Logic (Successes)
        h_mask = h_num.notna()
        if h_mask.any():
            h_groups = chunk[h_mask].groupby(COL_OMOP)
            for oid, group in h_groups:
                if oid not in harm_ref_samples:
                    harm_ref_samples[oid] = []
                    harm_unit_counts[oid] = Counter()
                harm_unit_counts[oid].update(group[COL_HARM_UNIT].astype(str).tolist())
                harm_ref_samples[oid].extend(h_num[group.index].tolist())

        pbar.update(len(chunk))

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
    for (oid, abbr, unit), vals in src_samples.items():
        n_src = len(vals)
        # Apply strict count threshold
        if n_src < args.min_count: continue
        
        s_vals = pd.Series(vals)
        h_vals = pd.Series(harm_ref_samples.get(oid, []))
        
        u_counts = harm_unit_counts.get(oid, Counter())
        clean_u = {k: v for k, v in u_counts.items() if str(k).lower() not in ['nan', 'none', 'na', '', '-1']}
        most_common_unit = max(clean_u, key=clean_u.get) if clean_u else "NA"

        res_row = {
            "OMOP_ID": oid,
            "TEST_ABBR": abbr,
            "ORIG_UNIT": unit,
            "N_SOURCE": n_src,
            "N_HARM": len(h_vals),
            "SUGGESTED_UNIT": "",
            "NOTES": "",
            "SOURCE_DECILES": get_deciles(s_vals),
            "HARM_DECILES": get_deciles(h_vals),
            "KS_STAT": "NA"
        }

        # Numeric comparison only if enough reference data exists
        if len(h_vals) >= 5: 
            # Drop NaNs for the actual KS test calculation
            s_clean = s_vals.dropna()
            if not s_clean.empty:
                ks_stat, _ = ks_2samp(s_clean, h_vals)
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

    print(f"\nAudit complete. Threshold set to {args.min_count}. Processed {args.test} lines.")

if __name__ == "__main__":
    main()
