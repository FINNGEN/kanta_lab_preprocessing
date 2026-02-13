import gzip
import argparse
import sys
import pandas as pd
import numpy as np
import os
import csv
import logging
from scipy.stats import ks_2samp
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Set up relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REF = os.path.join(SCRIPT_DIR, "..", "finngen_qc", "data", "fix_unit_based_in_abbreviation.tsv")
DEFAULT_USAGI = os.path.join(SCRIPT_DIR, "..", "finngen_qc", "data", "LABfi_ALL.usagi.csv")

def get_deciles(series):
    """Returns a string representation of deciles or 'NA' if insufficient data."""
    if series is None or len(series) < 10:
        return "NA"
    arr = pd.to_numeric(pd.Series(series), errors='coerce').dropna()
    if len(arr) < 10:
        return "NA"
    deciles = np.percentile(arr, np.linspace(0, 100, 11)).round(4).tolist()
    return "[" + ",".join(str(d) for d in deciles) + "]"

def get_usagi_linkage(usagi_path, rules_df):
    """Loads Usagi, sanitizes hidden spaces, and links to rules."""
    target_combos = {}
    combo_status = {}
    for _, row in rules_df.iterrows():
        t_unit = str(row['source_unit_clean_fix']).strip().upper()
        if t_unit in ["", "NAN", "NONE", "NA"]: t_unit = "NA"
        t_name = str(row['TEST_NAME_ABBREVIATION']).strip().lower()
        target_combos[(t_name, t_unit)] = set()

    if os.path.exists(usagi_path):
        cols_to_load = ['conceptId', 'ADD_INFO:testNameAbbreviation', 'ADD_INFO:measurementUnit']
        try:
            temp_head = pd.read_csv(usagi_path, nrows=0)
            status_col = 'mappingStatus' if 'mappingStatus' in temp_head.columns else 'statusSetBy'
            if status_col in temp_head.columns: cols_to_load.append(status_col)
        except: status_col = None

        usagi = pd.read_csv(usagi_path, usecols=cols_to_load, low_memory=False, dtype=str).fillna("NA")
        
        # SANITIZATION: Strip hidden spaces and warn
        col_abbr = 'ADD_INFO:testNameAbbreviation'
        mask = usagi[col_abbr].str.strip() != usagi[col_abbr]
        if mask.any():
            offenders = usagi.loc[mask, col_abbr].unique().tolist()
            logger.warning(f"Sanitizing hidden spaces in Usagi: {offenders}")
            usagi[col_abbr] = usagi[col_abbr].str.strip()
            usagi['ADD_INFO:measurementUnit'] = usagi['ADD_INFO:measurementUnit'].str.strip()

        for _, row in usagi.iterrows():
            u_test = str(row.get(col_abbr, 'NA')).lower()
            u_unit = str(row.get('ADD_INFO:measurementUnit', 'NA')).upper()
            tid = str(row.get('conceptId', 'NA')).strip()
            curr_status = str(row.get(status_col, 'UNREVIEWED')).strip() if status_col else "UNREVIEWED"
            if tid in ["NA", "0", "-1"]: continue
            if u_unit in ["", "NAN", "NONE"]: u_unit = "NA"
            key = (u_test, u_unit)
            if key in target_combos:
                target_combos[key].add(tid.split('.')[0])
                if key not in combo_status or curr_status == "APPROVED":
                    combo_status[key] = curr_status

    linkage_rows = []
    all_target_omops = set()
    for _, row in rules_df.iterrows():
        abbr, u_pre, u_fix = str(row['TEST_NAME_ABBREVIATION']).strip(), str(row['source_unit_clean']).strip(), str(row['source_unit_clean_fix']).strip()
        lookup_unit = u_fix.upper() if u_fix.upper() not in ["", "NAN", "NA"] else "NA"
        combo_key = (abbr.lower(), lookup_unit)
        found_ids = sorted(list(target_combos.get(combo_key, [])))
        all_target_omops.update(found_ids)
        linkage_rows.append({
            "TEST_NAME_ABBREVIATION": abbr, "source_unit_clean": u_pre, "source_unit_clean_fix": u_fix,
            "LINKED_OMOP_IDS": ",".join(found_ids) if found_ids else "MISSING", "MATCH_STATUS": combo_status.get(combo_key, "MISSING")
        })
    return pd.DataFrame(linkage_rows).astype(str), all_target_omops

def main():
    parser = argparse.ArgumentParser(description="Audit v7.2 - Sanitized Output")
    parser.add_argument("input", nargs='?', default="kanta_v3_harmonized_2026_02_13.txt.gz")
    parser.add_argument("-o", "--output", default="injection_check.tsv")
    parser.add_argument("--ref", default=DEFAULT_REF)
    parser.add_argument("--usagi", default=DEFAULT_USAGI)
    parser.add_argument("--test", type=int, default=None)
    args = parser.parse_args()

    df_rules = pd.read_csv(args.ref, sep='\t', dtype=str, keep_default_na=False).fillna("NA")
    df_sanity, target_omops = get_usagi_linkage(os.path.expanduser(args.usagi), df_rules)

    cache_name = f"test_{args.test}_cache_audit_data.tsv.gz" if args.test else "cache_audit_data.tsv.gz"
    
    if not os.path.exists(cache_name):
        logger.info(f"Building cache {cache_name}...")
        COL_TEST, COL_UNIT_CLEAN, COL_UNIT_PRE, COL_HARM_VAL, COL_VAL = \
            "cleaned::TEST_NAME_ABBREVIATION", "cleaned::MEASUREMENT_UNIT", \
            "cleaned-pre-fix::MEASUREMENT_UNIT", "harmonization_omop::MEASUREMENT_VALUE", \
            "source::MEASUREMENT_VALUE"
        
        reader = pd.read_csv(os.path.expanduser(args.input), sep='\t', compression='gzip', chunksize=500_000, 
                             nrows=args.test, engine='c', dtype=str, keep_default_na=False, quoting=csv.QUOTE_NONE)
        first_chunk = True
        with gzip.open(cache_name, 'wt') as f_out:
            for chunk in tqdm(reader, desc="Caching", unit_scale=True):
                col_omop = [h for h in chunk.columns if "OMOP_ID" in h][0]
                mask = (chunk[COL_TEST].isin(df_rules['TEST_NAME_ABBREVIATION'].unique())) | (chunk[col_omop].isin(target_omops))
                audit_cols = [COL_TEST, COL_UNIT_CLEAN, COL_UNIT_PRE, COL_HARM_VAL, col_omop, COL_VAL]
                chunk.loc[mask, audit_cols].to_csv(f_out, sep='\t', index=False, header=first_chunk)
                first_chunk = False

    samples_inj_harm, samples_inj_src, samples_ref_harm = {}, {}, {}
    count_inj_row, count_inj_num, count_ref_row, count_ref_num = {}, {}, {}, {}

    MAX_SAMPLES = 20000
    cache_reader = pd.read_csv(cache_name, sep='\t', dtype=str, keep_default_na=False, quoting=csv.QUOTE_NONE, chunksize=2_000_000)

    for chunk in tqdm(cache_reader, desc="Analyzing"):
        chunk.columns = ['abbr', 'u_clean', 'u_pre', 'v_harm', 'oid', 'v_src']
        chunk['abbr'] = chunk['abbr'].str.strip().str.lower()
        chunk['u_pre'] = chunk['u_pre'].str.strip().str.upper().replace(["", "NAN", "NONE"], "NA")
        chunk['u_clean'] = chunk['u_clean'].str.strip().str.upper().replace(["", "NAN", "NONE"], "NA")
        
        v_h_num = pd.to_numeric(chunk['v_harm'], errors='coerce')
        v_s_num = pd.to_numeric(chunk['v_src'], errors='coerce')

        # 1. INJECTED
        mask_inj = (chunk['u_pre'] != chunk['u_clean'])
        if mask_inj.any():
            for key, group in chunk[mask_inj].groupby(['abbr', 'u_pre']):
                count_inj_row[key] = count_inj_row.get(key, 0) + len(group)
                num_vals = v_h_num.loc[group.index].dropna()
                count_inj_num[key] = count_inj_num.get(key, 0) + len(num_vals)
                if len(samples_inj_harm.get(key, [])) < MAX_SAMPLES:
                    samples_inj_harm.setdefault(key, []).extend(num_vals.head(MAX_SAMPLES).tolist())
                    samples_inj_src.setdefault(key, []).extend(v_s_num.loc[num_vals.head(MAX_SAMPLES).index].tolist())

        # 2. BASELINE (Fallback to source values if harmonized is empty)
        mask_base = (chunk['u_pre'] == chunk['u_clean']) & (chunk['oid'].isin(target_omops))
        if mask_base.any():
            for oid, group in chunk[mask_base].groupby('oid'):
                count_ref_row[oid] = count_ref_row.get(oid, 0) + len(group)
                # Try harmonized first, then source
                num_vals = v_h_num.loc[group.index].dropna()
                if len(num_vals) == 0:
                    num_vals = v_s_num.loc[group.index].dropna()
                
                count_ref_num[oid] = count_ref_num.get(oid, 0) + len(num_vals)
                if len(samples_ref_harm.get(oid, [])) < MAX_SAMPLES:
                    samples_ref_harm.setdefault(oid, []).extend(num_vals.head(MAX_SAMPLES).tolist())

    audit_results = []
    for _, row in df_sanity.iterrows():
        abbr, u_pre = row['TEST_NAME_ABBREVIATION'].lower(), row['source_unit_clean'].upper()
        if u_pre in ["", "NAN", "NONE", "NA"]: u_pre = "NA"
        key, oid = (abbr, u_pre), row['LINKED_OMOP_IDS'].split(',')[0]

        n_inj_row, n_inj_num = count_inj_row.get(key, 0), count_inj_num.get(key, 0)
        n_ref_row, n_ref_num = count_ref_row.get(oid, 0), count_ref_num.get(oid, 0)
        h_inj, h_ref = samples_inj_harm.get(key, []), samples_ref_harm.get(oid, [])

        status, ks_stat, ks_mlogp = "OK", "NA", "NA"
        if n_inj_row == 0: status = "NOT_IN_DATA"
        elif n_ref_row == 0: status = "NO_BASELINE"
        elif n_inj_num < 30 or n_ref_num < 30: status = "LOW_DATA"
        else:
            ks, p = ks_2samp(h_inj, h_ref)
            ks_stat, ks_mlogp = round(ks, 4), round(-np.log10(p + 1e-300), 4)
            status = "SUCCESS" if ks < 0.3 else "FAIL"

        audit_results.append({
            "TEST_NAME_ABBREVIATION": row['TEST_NAME_ABBREVIATION'], "source_unit_clean": row['source_unit_clean'],
            "KS_STAT": ks_stat, "KS_mlogp": ks_mlogp, "AUDIT_STATUS": status,
            "N_INJECTED_ROWS": n_inj_row, "N_INJ_NUM": n_inj_num, 
            "N_BASELINE_ROWS": n_ref_row, "N_HARM_REF_NUM": n_ref_num,
            "SOURCE_DECILES": get_deciles(samples_inj_src.get(key, [])), 
            "HARM_INJECTED_DECILES": get_deciles(h_inj), 
            "BASELINE_DECILES": get_deciles(h_ref)
        })

    final_df = pd.merge(df_sanity, pd.DataFrame(audit_results), on=['TEST_NAME_ABBREVIATION', 'source_unit_clean'], how='left')
    final_df['FINAL_STATUS'] = final_df.apply(lambda r: f"{r['AUDIT_STATUS']} ({r['MATCH_STATUS']})" if r['MATCH_STATUS'] != "APPROVED" else r['AUDIT_STATUS'], axis=1)
    
    # Final cleanup to ensure "NA" everywhere
    final_df = final_df.fillna("NA").replace({np.nan: "NA", "nan": "NA", "": "NA"})
    final_df.to_csv(args.output, sep='\t', index=False)
    logger.info(f"Audit complete. Results saved to {args.output}")

if __name__ == "__main__":
    main()
