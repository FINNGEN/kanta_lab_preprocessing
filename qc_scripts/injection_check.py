import pandas as pd
import numpy as np
import os
import argparse
import subprocess
import logging
from io import StringIO
from scipy.stats import ks_2samp

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# --- DEFAULT PATHS ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "..", "finngen_qc", "data")
DEFAULT_RULES = os.path.join(DATA_DIR, "fix_unit_based_in_abbreviation.tsv")
DEFAULT_USAGI = os.path.join(DATA_DIR, "LABfi_ALL.usagi.csv")

def run_ch_query(query):
    try:
        process = subprocess.run(['clickhouse', 'local', '--query', query, '--format', 'TSVWithNames'], 
                                 capture_output=True, text=True, check=True)
        return pd.read_csv(StringIO(process.stdout), sep='\t', keep_default_na=False)
    except subprocess.CalledProcessError as e:
        logger.warning(f"ClickHouse query failed: {e.stderr[:500]}")
        return pd.DataFrame()

def clean_str(s):
    s = str(s).lower().strip()
    if s in ['nan', 'none', 'null', '', 'na', r'\\n', r'\\v']:
        return 'na'
    return s

def get_deciles(arr):
    if arr is None or len(arr) == 0: return "NA"
    nums = pd.to_numeric(arr, errors='coerce')
    clean_nums = nums[~np.isnan(nums)]
    if len(clean_nums) == 0: return "NA"
    try:
        percentiles = np.percentile(clean_nums, np.arange(0, 110, 10))
        return "[" + ",".join([str(round(float(x), 4)) for x in percentiles]) + "]"
    except Exception:
        return "NA"

def main():
    parser = argparse.ArgumentParser(description="Audit harmonization with KS testing.")
    parser.add_argument("input", help="Path to the munged.parquet file")
    parser.add_argument("--rules", default=DEFAULT_RULES)
    parser.add_argument("--usagi", default=DEFAULT_USAGI)
    parser.add_argument("-o", "--output", default="audit.txt")
    parser.add_argument("--test", type=int, help="Sample N random rows")
    args = parser.parse_args()

    # 1. LOAD MAPPINGS
    df_rules = pd.read_csv(args.rules, sep='\t', keep_default_na=False)
    df_rules['source_unit_clean'] = df_rules['source_unit_clean'].replace(['', 'nan', 'NULL', 'null'], 'NA')
    df_rules['source_unit_clean_fix'] = df_rules['source_unit_clean_fix'].replace(['', 'nan', 'NULL', 'null'], 'NA')
    
    df_rules['abbr_key'] = df_rules['TEST_NAME_ABBREVIATION'].apply(clean_str)
    df_rules['trg_key'] = df_rules['source_unit_clean_fix'].apply(clean_str)

    df_usagi = pd.read_csv(args.usagi, keep_default_na=False)
    df_usagi_clean = pd.DataFrame({
        'abbr_key': df_usagi['ADD_INFO:testNameAbbreviation'].apply(clean_str),
        'trg_key': df_usagi['ADD_INFO:measurementUnit'].apply(clean_str),
        'LINKED_OMOP_IDS': df_usagi['conceptId'],
        'mappingStatus': df_usagi['mappingStatus'].str.upper().str.strip().str.replace(' ', '_')
    }).drop_duplicates(subset=['abbr_key', 'trg_key'])

    merged = pd.merge(df_rules, df_usagi_clean, on=['abbr_key', 'trg_key'], how='left')

    if args.test:
        merged = merged.sample(n=min(len(merged), args.test))
    
    # Bonferroni setup
    num_tests = len(merged)
    alpha_bonf = 0.05 / num_tests if num_tests > 0 else 0.05

    results = []
    iterable = merged.iterrows()
    pbar = tqdm(total=len(merged), desc="Auditing") if tqdm else None

    # 2. AUDIT LOOP
    for _, row in iterable:
        abbr, unit_pre, unit_post, omop_id = row['TEST_NAME_ABBREVIATION'], row['source_unit_clean'], row['source_unit_clean_fix'], str(row['LINKED_OMOP_IDS']).strip()
        sql_pre, sql_post = (unit_pre if unit_pre != 'NA' else 'NA'), (unit_post if unit_post != 'NA' else 'NA')

        # A. Injected Query
        inj_query = (
            f"SELECT MEASUREMENT_VALUE_SOURCE, MEASUREMENT_VALUE_HARMONIZED "
            f"FROM file('{args.input}', 'Parquet') "
            f"WHERE lower(trim(TEST_NAME)) = {repr(abbr.lower())} "
            f"AND ifNull(MEASUREMENT_UNIT_PRE_FIX, 'NA') = {repr(sql_pre)} "
            f"AND ifNull(MEASUREMENT_UNIT_CLEANED, 'NA') = {repr(sql_post)}"
        )
        df_inj = run_ch_query(inj_query)
        n_inj_rows = len(df_inj)
        
        # Clean numeric data for KS and Deciles
        inj_harm_all = pd.to_numeric(df_inj['MEASUREMENT_VALUE_HARMONIZED'], errors='coerce').dropna().values
        n_inj_num = len(inj_harm_all)
        inj_src_all = pd.to_numeric(df_inj['MEASUREMENT_VALUE_SOURCE'], errors='coerce').dropna().values

        # B. OMOP Reference Query
        val_ref = []
        n_ref = 0
        if omop_id not in ['nan', 'NA', '', '0', 'None']:
            ref_query = (
                f"SELECT MEASUREMENT_VALUE_HARMONIZED FROM file('{args.input}', 'Parquet') "
                f"WHERE ifNull(toString(OMOP_CONCEPT_ID), 'NA') = {repr(omop_id)} "
                f"AND MEASUREMENT_VALUE_HARMONIZED IS NOT NULL "
                f"AND NOT ("
                f"  lower(trim(TEST_NAME)) = {repr(abbr.lower())} "
                f"  AND ifNull(MEASUREMENT_UNIT_PRE_FIX, 'NA') = {repr(sql_pre)} "
                f"  AND ifNull(MEASUREMENT_UNIT_CLEANED, 'NA') = {repr(sql_post)}"
                f")"
            )
            df_ref = run_ch_query(ref_query)
            val_ref = pd.to_numeric(df_ref['MEASUREMENT_VALUE_HARMONIZED'], errors='coerce').dropna().values
            n_ref = len(val_ref)

        # C. KS Test Logic
        ks_stat, mlogp = "NA", "NA"
        ks_passed = True
        if n_inj_num > 5 and n_ref > 5:
            # Sample max 50k for speed/validity
            s1 = np.random.choice(inj_harm_all, min(n_inj_num, 50000), replace=False)
            s2 = np.random.choice(val_ref, min(n_ref, 50000), replace=False)
            res = ks_2samp(s1, s2)
            ks_stat = round(res.statistic, 4)
            mlogp = round(-np.log10(res.pvalue) if res.pvalue > 0 else 300, 4) # cap at 300
            if ks_stat >= 0.3 and res.pvalue < alpha_bonf:
                ks_passed = False

        # Determine Status
        if n_inj_rows == 0: status = "NO_DATA"
        elif n_inj_num == 0: status = "NO_HARMONIZED_DATA"
        elif n_ref == 0: status = "NO_OMOP_DATA"
        elif not ks_passed: status = "FAILED_KS"
        else: status = "SUCCESS"

        results.append({
            'TEST_NAME_ABBREVIATION': abbr,
            'source_unit_clean': unit_pre,
            'source_unit_clean_fix': unit_post,
            'AUDIT_STATUS': status,
            'KS': ks_stat,
            'MLOGP': mlogp,
            'LINKED_OMOP_IDS': omop_id,
            'MATCH_STATUS': row['mappingStatus'] if pd.notna(row['mappingStatus']) else 'MISSING',
            'N_INJ_ROWS': n_inj_rows,
            'N_INJ_NUM': n_inj_num,
            'N_OMOP_REF': n_ref,
            'SOURCE_DECILES': get_deciles(inj_src_all),
            'HARM_INJ_DECILES': get_deciles(inj_harm_all),
            'OMOP_REF_DECILES': get_deciles(val_ref)
        })
        if pbar: pbar.update(1)

    if pbar: pbar.close()
    pd.DataFrame(results).to_csv(args.output, sep='\t', index=False)
    logger.info(f"Audit complete. Results in {args.output}")

if __name__ == "__main__":
    main()
