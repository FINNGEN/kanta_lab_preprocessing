import gzip
import argparse
import sys
import pandas as pd
import numpy as np
import gc
import os
import matplotlib.pyplot as plt
from scipy.stats import ks_2samp
from tqdm import tqdm

def get_deciles(series):
    if series is None or len(series) == 0:
        return "NA"
    arr = np.array(series, dtype=np.float32)
    arr = arr[~np.isnan(arr)]
    if arr.size == 0:
        return "NA"
    deciles = np.percentile(arr, np.linspace(0, 100, 11)).round(4).tolist()
    return "[" + ",".join(str(d) for d in deciles) + "]"

def main():
    parser = argparse.ArgumentParser(description="Audit Unit Injection Success - Clean Reference Comparison")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_ref = os.path.join(script_dir, "../finngen_qc/data/fix_unit_based_in_abbreviation.tsv")
    default_input = "~/fg-3/kanta_v3/munged/kanta_v3_harmonized_2026_02_02.txt.gz"
    default_output = "injection_check.tsv"
    
    parser.add_argument("input", nargs='?', default=default_input, help=f"Path to file (default: {default_input})")
    parser.add_argument("-o", "--output", default=default_output, help=f"Output TSV (default: {default_output})")
    parser.add_argument("--ref", default=default_ref, help=f"Injection mapping file (default: {default_ref})")
    parser.add_argument("--min_count", type=int, default=100, help="Min count to include in audit")
    parser.add_argument("--test", type=int, nargs='?', const=1000000, default=None)
    args = parser.parse_args()

    COL_OMOP = "harmonization_omop::OMOP_ID"
    COL_TEST = "cleaned::TEST_NAME_ABBREVIATION"
    COL_UNIT_PRE = "cleaned-pre-fix::MEASUREMENT_UNIT"
    COL_UNIT_CLEAN = "cleaned::MEASUREMENT_UNIT"
    COL_VAL = "source::MEASUREMENT_VALUE"
    COL_HARM_VAL = "harmonization_omop::MEASUREMENT_VALUE"

    # 1. Load Reference File
    ref_path = os.path.expanduser(args.ref)
    if os.path.exists(ref_path):
        df_ref = pd.read_csv(ref_path, sep='\t', keep_default_na=False)
        df_ref['source_unit_clean'] = df_ref['source_unit_clean'].replace('', 'NA')
    else:
        df_ref = pd.DataFrame(columns=['TEST_NAME_ABBREVIATION', 'source_unit_clean', 'source_unit_clean_fix'])

    pre_samples = {}
    clean_ref_samples = {}

    input_path = os.path.expanduser(args.input)
    reader = pd.read_csv(
        input_path, sep='\t', compression='gzip',
        usecols=[COL_OMOP, COL_TEST, COL_UNIT_PRE, COL_UNIT_CLEAN, COL_VAL, COL_HARM_VAL],
        chunksize=250_000, nrows=args.test, engine='c', low_memory=False, keep_default_na=False
    )

    pbar = tqdm(total=args.test, desc="Auditing Injections")
    for chunk in reader:
        src_num = pd.to_numeric(chunk[COL_VAL], errors='coerce').astype(np.float32)
        harm_num = pd.to_numeric(chunk[COL_HARM_VAL], errors='coerce').astype(np.float32)
        chunk[COL_OMOP] = chunk[COL_OMOP].astype(str).replace(['nan', 'None', '', 'NA'], '-1')
        for col in [COL_UNIT_PRE, COL_UNIT_CLEAN]:
            chunk[col] = chunk[col].astype(str).replace(['nan', 'None', 'NA'], '')

        mask_changed = (chunk[COL_UNIT_PRE] != chunk[COL_UNIT_CLEAN]) & (chunk[COL_UNIT_CLEAN] != "") & (harm_num.notna()) & (chunk[COL_OMOP] != "-1")

        if mask_changed.any():
            c_data = chunk[mask_changed]
            c_harm = harm_num[mask_changed]
            c_src = src_num[mask_changed]
            for (oid, abbr, u_pre, u_clean), idx in c_data.groupby([COL_OMOP, COL_TEST, COL_UNIT_PRE, COL_UNIT_CLEAN]).groups.items():
                key = (oid, abbr, u_pre, u_clean)
                if key not in pre_samples: pre_samples[key] = {"harm": [], "src": []}
                pre_samples[key]["harm"].append(c_harm.loc[idx].values)
                pre_samples[key]["src"].append(c_src.loc[idx].values)

        mask_ref = (~mask_changed) & (harm_num.notna()) & (chunk[COL_OMOP] != "-1")
        if mask_ref.any():
            r_data = chunk[mask_ref]
            r_vals = harm_num[mask_ref]
            for oid, idx in r_data.groupby(COL_OMOP).groups.items():
                if oid not in clean_ref_samples: clean_ref_samples[oid] = []
                clean_ref_samples[oid].append(r_vals.loc[idx].values)
        pbar.update(len(chunk))
    pbar.close()

    audit_rows = []
    for (oid, abbr, u_pre, u_clean), data in pre_samples.items():
        all_harm_injected = np.concatenate(data["harm"])
        if len(all_harm_injected) < args.min_count: continue
        
        all_src_injected = np.concatenate(data["src"])
        ref_arrays = clean_ref_samples.get(oid, [])
        all_clean_ref = np.concatenate(ref_arrays) if ref_arrays else np.array([], dtype=np.float32)

        res_row = {
            "OMOP_ID": oid, 
            "TEST_NAME_ABBREVIATION": abbr, 
            "source_unit_clean": u_pre if u_pre != "" else "NA", 
            "CLEANED_UNIT": u_clean,
            "N_INJECTED": len(all_harm_injected), 
            "N_PURE_REF": len(all_clean_ref),
            "SOURCE_DECILES": get_deciles(all_src_injected), 
            "HARM_INJECTED_DECILES": get_deciles(all_harm_injected),
            "HARM_REF_DECILES": get_deciles(all_clean_ref), 
            "KS_STAT": np.nan, 
            "KS_mlogp": np.nan,
            "STATUS": "No Ref Data"
        }

        if len(all_clean_ref) >= 20:
            ks_stat, p_val = ks_2samp(all_harm_injected, all_clean_ref)
            res_row["KS_STAT"] = round(ks_stat, 4)
            res_row["KS_mlogp"] = round(-np.log10(p_val + 1e-300), 4)
            
            if ks_stat < 0.15: res_row["STATUS"] = "EXCELLENT"
            elif ks_stat < 0.3: res_row["STATUS"] = "SUCCESS"
            elif ks_stat < 0.5: res_row["STATUS"] = "WARNING"
            else: res_row["STATUS"] = "FAIL"
        audit_rows.append(res_row)

    df_audit = pd.DataFrame(audit_rows)
    final_df = pd.merge(df_ref, df_audit, on=['TEST_NAME_ABBREVIATION', 'source_unit_clean'], how='outer')

    def get_flag(row):
        if pd.isna(row['STATUS']) and pd.notna(row['source_unit_clean_fix']): return "RULE_NOT_FOUND"
        if pd.isna(row['source_unit_clean_fix']) and pd.notna(row['STATUS']): return "NEW_INJECTION"
        return "OK"

    final_df['AUDIT_FLAG'] = final_df.apply(get_flag, axis=1)
    final_df['target_unit'] = final_df['CLEANED_UNIT'].fillna(final_df['source_unit_clean_fix'])
    final_df.drop(columns=['CLEANED_UNIT', 'source_unit_clean_fix'], inplace=True)

    # Sort helper
    final_df['sort_helper'] = pd.to_numeric(final_df['N_INJECTED'], errors='coerce').fillna(-1)
    final_df = final_df.sort_values(by='sort_helper', ascending=False).drop(columns=['sort_helper'])

    # Format integers
    for col in ["N_INJECTED", "N_PURE_REF"]:
        if col in final_df.columns:
            final_df[col] = final_df[col].apply(lambda x: str(int(x)) if pd.notnull(x) and not isinstance(x, str) else x)

    final_df = final_df.astype(object).fillna("NO_DATA")
    
    # Updated column order: OMOP ID moved forward, AUDIT_FLAG moved to end
    cols = [
        'TEST_NAME_ABBREVIATION', 'source_unit_clean', 'target_unit', 'STATUS', 
        'KS_STAT', 'KS_mlogp', 'OMOP_ID', 'N_INJECTED', 
        'SOURCE_DECILES', 'HARM_INJECTED_DECILES', 'N_PURE_REF', 'HARM_REF_DECILES', 'AUDIT_FLAG'
    ]
    final_df = final_df[cols]

    final_df.to_csv(args.output, sep='\t', index=False)
    
    if not df_audit.empty:
        plt.figure(figsize=(10, 6))
        plot_df = final_df[final_df['STATUS'] != "NO_DATA"]
        if not plot_df.empty:
            plot_df['STATUS'].value_counts().plot(kind='bar', color='royalblue')
            plt.title("Injection Audit Results")
            plt.ylabel("Frequency")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.savefig("audit_summary.png")
            print("Generated audit_summary.png")

    print(f"\nAudit complete. Results saved to: {os.path.abspath(args.output)}")

if __name__ == "__main__":
    main()
