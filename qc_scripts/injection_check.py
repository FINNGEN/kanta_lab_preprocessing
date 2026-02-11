import gzip
import argparse
import sys
import pandas as pd
import numpy as np
import os
import re
from scipy.stats import ks_2samp
from tqdm import tqdm

# Set up relative paths based on script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REF = os.path.join(SCRIPT_DIR, "..", "finngen_qc", "data", "fix_unit_based_in_abbreviation.tsv")
DEFAULT_USAGI = os.path.join(SCRIPT_DIR, "..", "finngen_qc", "data", "LABfi_ALL.usagi.csv")

def get_deciles(series):
    if series is None or len(series) == 0: return "NA"
    arr = pd.to_numeric(pd.Series(series), errors='coerce').dropna()
    if arr.empty: return "NA"
    deciles = np.percentile(arr, np.linspace(0, 100, 11)).round(4).tolist()
    return "[" + ",".join(str(d) for d in deciles) + "]"

def get_usagi_linkage(usagi_path, rules_df):
    target_combos = {}
    for _, row in rules_df.iterrows():
        t_unit = str(row['source_unit_clean_fix']).strip().lower()
        if t_unit in ["", "nan", "none", "na"]: t_unit = "na"
        t_name = str(row['TEST_NAME_ABBREVIATION']).strip().lower()
        target_combos[(t_name, t_unit)] = set()

    if os.path.exists(usagi_path):
        usagi = pd.read_csv(usagi_path, low_memory=False, dtype=str).fillna("NA")
        for _, row in usagi.iterrows():
            code = str(row.get('sourceCode', 'NA')).strip().lower()
            tid = str(row.get('conceptId', 'NA')).strip()
            if "[" in code and code.endswith("]"):
                u_test = code.split("[")[0].strip()
                u_unit = code.split("[")[1].replace("]", "").strip()
                if u_unit == "": u_unit = "na"
                if (u_test, u_unit) in target_combos and tid not in ["NA", "0", "-1"]:
                    target_combos[(u_test, u_unit)].add(tid.split('.')[0])

    linkage_rows = []
    all_target_omops = set()
    for _, row in rules_df.iterrows():
        abbr = str(row['TEST_NAME_ABBREVIATION']).strip()
        u_pre = str(row['source_unit_clean']).strip()
        u_fix = str(row['source_unit_clean_fix']).strip()
        lookup_unit = u_fix.lower()
        if lookup_unit in ["", "nan", "na"]: lookup_unit = "na"
        found_ids = sorted(list(target_combos.get((abbr.lower(), lookup_unit), [])))
        all_target_omops.update(found_ids)
        linkage_rows.append({
            "TEST_NAME_ABBREVIATION": abbr,
            "source_unit_clean": u_pre,
            "source_unit_clean_fix": u_fix,
            "LINKED_OMOP_IDS": ",".join(found_ids) if found_ids else "MISSING",
            "MATCH_STATUS": "FOUND" if found_ids else "MISSING"
        })
    return pd.DataFrame(linkage_rows).astype(str), all_target_omops

def main():
    parser = argparse.ArgumentParser(description="Audit v5.2 - Fixed Missing NA Counts")
    parser.add_argument("input", nargs='?', default="~/fg-3/kanta_v3/munged/kanta_v3_harmonized_2026_02_02.txt.gz")
    parser.add_argument("-o", "--output", default="injection_check.tsv")
    parser.add_argument("--ref", default=DEFAULT_REF)
    parser.add_argument("--usagi", default=DEFAULT_USAGI)
    parser.add_argument("--test", type=int, nargs='?', const=1000000, default=None)
    args = parser.parse_args()

    df_rules = pd.read_csv(args.ref, sep='\t', dtype=str, keep_default_na=False).fillna("NA").replace("", "NA")
    df_sanity, target_omops = get_usagi_linkage(args.usagi, df_rules)

    COL_TEST, COL_UNIT_PRE, COL_UNIT_CLEAN, COL_VAL, COL_HARM_VAL = \
        "cleaned::TEST_NAME_ABBREVIATION", "cleaned-pre-fix::MEASUREMENT_UNIT", \
        "cleaned::MEASUREMENT_UNIT", "source::MEASUREMENT_VALUE", "harmonization_omop::MEASUREMENT_VALUE"

    cache_name = "cache_audit_data.tsv.gz"
    if args.test: cache_name = f"test_{args.test}_{cache_name}"

    # 2. Heavy Data Caching
    if not os.path.exists(cache_name):
        print(f"Building cache {cache_name}...")
        input_path = os.path.expanduser(args.input)
        with gzip.open(input_path, 'rt') as f:
            full_header = f.readline().strip().split('\t')
        col_omop = [h for h in full_header if "OMOP_ID" in h][0]
        
        reader = pd.read_csv(input_path, sep='\t', compression='gzip',
            usecols=[col_omop, COL_TEST, COL_UNIT_PRE, COL_UNIT_CLEAN, COL_VAL, COL_HARM_VAL],
            chunksize=500_000, nrows=args.test, engine='c', dtype=str, keep_default_na=False)

        first_chunk = True
        with tqdm(total=args.test, desc="Caching") as pbar:
            with gzip.open(cache_name, 'wt') as f_out:
                for chunk in reader:
                    chunk = chunk.fillna("NA").replace(["", "nan"], "NA")
                    mask_target = (chunk[COL_TEST].isin(df_rules['TEST_NAME_ABBREVIATION'].unique())) | (chunk[col_omop].isin(target_omops))
                    chunk[mask_target].to_csv(f_out, sep='\t', index=False, header=first_chunk)
                    first_chunk = False
                    pbar.update(len(chunk))

    # 3. Analysis
    samples_injected, samples_baseline = {}, {}
    count_injected, count_baseline = {}, {}
    
    cache_reader = pd.read_csv(cache_name, sep='\t', dtype=str, keep_default_na=False, chunksize=500_000)
    col_omop_cache = None

    with tqdm(desc="Analyzing") as pbar:
        for chunk in cache_reader:
            if col_omop_cache is None: col_omop_cache = [h for h in chunk.columns if "OMOP_ID" in h][0]
            chunk = chunk.fillna("NA").replace(["", "nan"], "NA")
            
            mask_inj = (chunk[COL_UNIT_PRE] != chunk[COL_UNIT_CLEAN])
            for (oid, abbr, u_pre), group in chunk[mask_inj].groupby([col_omop_cache, COL_TEST, COL_UNIT_PRE]):
                key = (oid, abbr, u_pre)
                count_injected[key] = count_injected.get(key, 0) + len(group)
                
                numeric_group = group[group[COL_HARM_VAL] != "NA"]
                if not numeric_group.empty:
                    if key not in samples_injected: samples_injected[key] = {"harm": [], "src": []}
                    if len(samples_injected[key]["harm"]) < 20000:
                        needed = 20000 - len(samples_injected[key]["harm"])
                        samples_injected[key]["harm"].extend(numeric_group[COL_HARM_VAL].head(needed).tolist())
                        samples_injected[key]["src"].extend(numeric_group[COL_VAL].head(needed).tolist())

            mask_base = (chunk[COL_UNIT_PRE] == chunk[COL_UNIT_CLEAN]) & (chunk[col_omop_cache].isin(target_omops))
            for oid, group in chunk[mask_base].groupby(col_omop_cache):
                count_baseline[oid] = count_baseline.get(oid, 0) + len(group)
                numeric_base = group[group[COL_HARM_VAL] != "NA"]
                if not numeric_base.empty:
                    if oid not in samples_baseline: samples_baseline[oid] = []
                    if len(samples_baseline[oid]) < 20000:
                        needed = 20000 - len(samples_baseline[oid])
                        samples_baseline[oid].extend(numeric_base[COL_HARM_VAL].head(needed).tolist())
            pbar.update(len(chunk))

    # 4. Results Construction - ITERATE OVER COUNTS, NOT SAMPLES
    audit_results = []
    for (oid, abbr, u_pre), total_count in count_injected.items():
        key = (oid, abbr, u_pre)
        
        # Get samples if they exist, else empty
        data = samples_injected.get(key, {"harm": [], "src": []})
        base_vals = samples_baseline.get(oid, [])
        
        h_inj = pd.to_numeric(pd.Series(data["harm"]), errors='coerce').dropna()
        h_ref = pd.to_numeric(pd.Series(base_vals), errors='coerce').dropna()
        
        status, ks_stat, ks_mlogp = "OK", np.nan, np.nan
        if h_ref.empty: 
            status = "NO_BASELINE"
        elif h_inj.empty:
            status = "NON_NUMERIC_ONLY"
        else:
            ks, p = ks_2samp(h_inj, h_ref)
            ks_stat, ks_mlogp = round(ks, 4), round(-np.log10(p + 1e-300), 4)
            status = "EXCELLENT" if ks < 0.15 else "SUCCESS" if ks < 0.3 else "WARNING" if ks < 0.5 else "FAIL"

        audit_results.append({
            "TEST_NAME_ABBREVIATION": abbr, "source_unit_clean": u_pre,
            "KS_STAT": ks_stat, "KS_mlogp": ks_mlogp, "STATUS": status,
            "N_INJECTED": int(total_count), 
            "N_BASELINE": int(count_baseline.get(oid, 0)),
            "SOURCE_DECILES": get_deciles(data["src"]), 
            "HARM_INJECTED_DECILES": get_deciles(data["harm"]), 
            "BASELINE_DECILES": get_deciles(base_vals)
        })

    inj_df = pd.DataFrame(audit_results)
    final_df = pd.merge(df_sanity, inj_df, on=['TEST_NAME_ABBREVIATION', 'source_unit_clean'], how='left')
    
    for col in ['N_INJECTED', 'N_BASELINE']:
        final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0).astype(int)

    final_df = final_df.sort_values(['N_INJECTED', 'N_BASELINE'], ascending=False).fillna("NA")
    final_df.to_csv(args.output, sep='\t', index=False)
    print(f"Audit complete. Results saved to {args.output}")

if __name__ == "__main__": main()
