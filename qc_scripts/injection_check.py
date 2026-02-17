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

# Column schemas — defined once so empty DataFrames always carry the right columns
OMOP_BLOCK_COLS = ['test_name', 'unit_pre', 'unit_post',
                   'MEASUREMENT_VALUE_SOURCE', 'MEASUREMENT_VALUE_HARMONIZED']
INJ_BLOCK_COLS  = ['MEASUREMENT_VALUE_SOURCE', 'MEASUREMENT_VALUE_HARMONIZED']


def run_ch_query(query):
    try:
        process = subprocess.run(
            ['clickhouse', 'local', '--query', query, '--format', 'TSVWithNames'],
            capture_output=True, text=True, check=True
        )
        return pd.read_csv(StringIO(process.stdout), sep='\t', keep_default_na=False)
    except subprocess.CalledProcessError as e:
        logger.warning(f"ClickHouse query failed: {e.stderr[:500]}")
        return pd.DataFrame()


def _ensure_cols(df, cols):
    """Add any missing columns as NaN and return df with exactly those columns."""
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan
    return df[cols]


def clean_str(s):
    s = str(s).lower().strip()
    if s in ['nan', 'none', 'null', '', 'na', r'\\n', r'\\v']:
        return 'na'
    return s


def get_deciles(arr):
    if arr is None or len(arr) == 0:
        return "NA"
    nums = pd.to_numeric(arr, errors='coerce')
    clean_nums = nums[~np.isnan(nums)]
    if len(clean_nums) == 0:
        return "NA"
    try:
        percentiles = np.percentile(clean_nums, np.arange(0, 110, 10))
        return "[" + ",".join([str(round(float(x), 4)) for x in percentiles]) + "]"
    except Exception:
        return "NA"


def fetch_omop_block(parquet_path, omop_id):
    """
    Fetch all rows for a given OMOP concept ID where MEASUREMENT_VALUE_HARMONIZED
    is not null. Always returns a DataFrame with OMOP_BLOCK_COLS columns.
    """
    query = (
        f"SELECT "
        f"  lower(trim(TEST_NAME)) AS test_name, "
        f"  ifNull(MEASUREMENT_UNIT_PRE_FIX, 'NA') AS unit_pre, "
        f"  ifNull(MEASUREMENT_UNIT_CLEANED, 'NA') AS unit_post, "
        f"  MEASUREMENT_VALUE_SOURCE, "
        f"  MEASUREMENT_VALUE_HARMONIZED "
        f"FROM file('{parquet_path}', 'Parquet') "
        f"WHERE ifNull(toString(OMOP_CONCEPT_ID), 'NA') = {repr(omop_id)} "
        f"AND MEASUREMENT_VALUE_HARMONIZED IS NOT NULL"
    )
    return _ensure_cols(run_ch_query(query), OMOP_BLOCK_COLS)


def fetch_injected_block(parquet_path, abbr, sql_pre, sql_post):
    """
    Fetch rows for a specific test/unit combo (including null harmonized values).
    Always returns a DataFrame with INJ_BLOCK_COLS columns.
    """
    query = (
        f"SELECT MEASUREMENT_VALUE_SOURCE, MEASUREMENT_VALUE_HARMONIZED "
        f"FROM file('{parquet_path}', 'Parquet') "
        f"WHERE lower(trim(TEST_NAME)) = {repr(abbr.lower())} "
        f"AND ifNull(MEASUREMENT_UNIT_PRE_FIX, 'NA') = {repr(sql_pre)} "
        f"AND ifNull(MEASUREMENT_UNIT_CLEANED, 'NA') = {repr(sql_post)}"
    )
    return _ensure_cols(run_ch_query(query), INJ_BLOCK_COLS)


def compute_ks(inj_harm, ref_harm, alpha_bonf):
    """Run KS test; return (ks_stat, mlogp, ks_passed)."""
    n_inj = len(inj_harm)
    n_ref = len(ref_harm)
    if n_inj <= 5 or n_ref <= 5:
        return "NA", "NA", True

    s1 = np.random.choice(inj_harm, min(n_inj, 50000), replace=False)
    s2 = np.random.choice(ref_harm, min(n_ref, 50000), replace=False)
    res = ks_2samp(s1, s2)
    ks_stat = round(res.statistic, 4)
    mlogp = round(-np.log10(res.pvalue) if res.pvalue > 0 else 300, 4)
    ks_passed = not (ks_stat >= 0.3 and res.pvalue < alpha_bonf)
    return ks_stat, mlogp, ks_passed


def main():
    parser = argparse.ArgumentParser(description="Audit harmonization with KS testing (OMOP-grouped).")
    parser.add_argument("input", help="Path to the munged.parquet file")
    parser.add_argument("--rules", default=DEFAULT_RULES)
    parser.add_argument("--usagi", default=DEFAULT_USAGI)
    parser.add_argument("-o", "--output", default="audit.txt")
    parser.add_argument("--test", type=int, help="Sample N random rows from the rules table")
    args = parser.parse_args()

    # ------------------------------------------------------------------ #
    # 1. LOAD MAPPINGS (identical to original)
    # ------------------------------------------------------------------ #
    df_rules = pd.read_csv(args.rules, sep='\t', keep_default_na=False)
    df_rules['source_unit_clean'] = df_rules['source_unit_clean'].replace(
        ['', 'nan', 'NULL', 'null'], 'NA'
    )
    df_rules['source_unit_clean_fix'] = df_rules['source_unit_clean_fix'].replace(
        ['', 'nan', 'NULL', 'null'], 'NA'
    )
    df_rules['abbr_key'] = df_rules['TEST_NAME_ABBREVIATION'].apply(clean_str)
    df_rules['trg_key']  = df_rules['source_unit_clean_fix'].apply(clean_str)

    df_usagi = pd.read_csv(args.usagi, keep_default_na=False)
    df_usagi_clean = pd.DataFrame({
        'abbr_key':        df_usagi['ADD_INFO:testNameAbbreviation'].apply(clean_str),
        'trg_key':         df_usagi['ADD_INFO:measurementUnit'].apply(clean_str),
        'LINKED_OMOP_IDS': df_usagi['conceptId'],
        'mappingStatus':   df_usagi['mappingStatus'].str.upper().str.strip().str.replace(' ', '_')
    }).drop_duplicates(subset=['abbr_key', 'trg_key'])

    merged = pd.merge(df_rules, df_usagi_clean, on=['abbr_key', 'trg_key'], how='left')

    if args.test:
        merged = merged.sample(n=min(len(merged), args.test))

    num_tests  = len(merged)
    alpha_bonf = 0.05 / num_tests if num_tests > 0 else 0.05

    # ------------------------------------------------------------------ #
    # 2. SPLIT: rows with a valid OMOP ID vs. rows without
    # ------------------------------------------------------------------ #
    def is_valid_omop(oid):
        return str(oid).strip() not in ['nan', 'NA', '', '0', 'None']

    merged['_omop_str'] = merged['LINKED_OMOP_IDS'].apply(lambda x: str(x).strip())
    merged['_has_omop'] = merged['_omop_str'].apply(is_valid_omop)

    omop_groups  = merged[merged['_has_omop']].groupby('_omop_str')
    no_omop_rows = merged[~merged['_has_omop']]

    logger.info(
        f"Rows with OMOP ID: {merged['_has_omop'].sum()} across "
        f"{len(omop_groups)} unique OMOP IDs. "
        f"Rows without OMOP ID: {len(no_omop_rows)}."
    )

    results = []
    pbar = tqdm(total=len(omop_groups) + len(no_omop_rows), desc="Auditing") if tqdm else None

    # ------------------------------------------------------------------ #
    # 3a. AUDIT LOOP — rows WITH a valid OMOP ID
    #     One SQL query per OMOP ID; pandas handles per-row exclusion.
    # ------------------------------------------------------------------ #
    for omop_id, group in omop_groups:
        df_block = fetch_omop_block(args.input, omop_id)  # single SQL query for this OMOP

        for _, row in group.iterrows():
            abbr      = row['TEST_NAME_ABBREVIATION']
            unit_pre  = row['source_unit_clean']
            unit_post = row['source_unit_clean_fix']
            sql_pre   = unit_pre  if unit_pre  != 'NA' else 'NA'
            sql_post  = unit_post if unit_post != 'NA' else 'NA'

            # Build injected subset and reference pool entirely in pandas
            if not df_block.empty:
                mask = (
                    (df_block['test_name'] == abbr.lower()) &
                    (df_block['unit_pre']   == sql_pre) &
                    (df_block['unit_post']  == sql_post)
                )
                df_inj_subset = df_block[mask]
                ref_harm = (
                    pd.to_numeric(df_block.loc[~mask, 'MEASUREMENT_VALUE_HARMONIZED'], errors='coerce')
                    .dropna().values
                )
            else:
                df_inj_subset = pd.DataFrame(columns=OMOP_BLOCK_COLS)
                ref_harm = np.array([])

            # Fallback SQL only when this combo had zero rows in the OMOP block
            # (also captures rows where harmonized IS NULL, for N_INJ_ROWS accuracy)
            if df_inj_subset.empty:
                df_inj_full = fetch_injected_block(args.input, abbr, sql_pre, sql_post)
            else:
                df_inj_full = df_inj_subset  # null-harmonized rows are a rare edge case

            n_inj_rows   = len(df_inj_full)
            inj_harm_all = (
                pd.to_numeric(df_inj_subset['MEASUREMENT_VALUE_HARMONIZED'], errors='coerce')
                .dropna().values
            )
            inj_src_all = (
                pd.to_numeric(df_inj_full['MEASUREMENT_VALUE_SOURCE'], errors='coerce')
                .dropna().values
            )
            n_inj_num = len(inj_harm_all)
            n_ref     = len(ref_harm)

            ks_stat, mlogp, ks_passed = compute_ks(inj_harm_all, ref_harm, alpha_bonf)

            if   n_inj_rows == 0: status = "NO_DATA"
            elif n_inj_num  == 0: status = "NO_HARMONIZED_DATA"
            elif n_ref      == 0: status = "NO_OMOP_DATA"
            elif not ks_passed:   status = "FAILED_KS"
            else:                 status = "SUCCESS"

            results.append({
                'TEST_NAME_ABBREVIATION': abbr,
                'source_unit_clean':      unit_pre,
                'source_unit_clean_fix':  unit_post,
                'AUDIT_STATUS':           status,
                'KS':                     ks_stat,
                'MLOGP':                  mlogp,
                'LINKED_OMOP_IDS':        omop_id,
                'MATCH_STATUS':           row['mappingStatus'] if pd.notna(row['mappingStatus']) else 'MISSING',
                'N_INJ_ROWS':             n_inj_rows,
                'N_INJ_NUM':              n_inj_num,
                'N_OMOP_REF':             n_ref,
                'SOURCE_DECILES':         get_deciles(inj_src_all),
                'HARM_INJ_DECILES':       get_deciles(inj_harm_all),
                'OMOP_REF_DECILES':       get_deciles(ref_harm),
            })

        if pbar:
            pbar.update(1)

    # ------------------------------------------------------------------ #
    # 3b. AUDIT LOOP — rows WITHOUT a valid OMOP ID
    #     No shared reference pool; one SQL query per row (same as original).
    # ------------------------------------------------------------------ #
    for _, row in no_omop_rows.iterrows():
        abbr      = row['TEST_NAME_ABBREVIATION']
        unit_pre  = row['source_unit_clean']
        unit_post = row['source_unit_clean_fix']
        sql_pre   = unit_pre  if unit_pre  != 'NA' else 'NA'
        sql_post  = unit_post if unit_post != 'NA' else 'NA'
        omop_id   = row['_omop_str']

        df_inj       = fetch_injected_block(args.input, abbr, sql_pre, sql_post)
        n_inj_rows   = len(df_inj)
        inj_harm_all = pd.to_numeric(df_inj['MEASUREMENT_VALUE_HARMONIZED'], errors='coerce').dropna().values
        inj_src_all  = pd.to_numeric(df_inj['MEASUREMENT_VALUE_SOURCE'],      errors='coerce').dropna().values
        n_inj_num    = len(inj_harm_all)

        if   n_inj_rows == 0: status = "NO_DATA"
        elif n_inj_num  == 0: status = "NO_HARMONIZED_DATA"
        else:                 status = "NO_OMOP_DATA"

        results.append({
            'TEST_NAME_ABBREVIATION': abbr,
            'source_unit_clean':      unit_pre,
            'source_unit_clean_fix':  unit_post,
            'AUDIT_STATUS':           status,
            'KS':                     "NA",
            'MLOGP':                  "NA",
            'LINKED_OMOP_IDS':        omop_id,
            'MATCH_STATUS':           row['mappingStatus'] if pd.notna(row['mappingStatus']) else 'MISSING',
            'N_INJ_ROWS':             n_inj_rows,
            'N_INJ_NUM':              n_inj_num,
            'N_OMOP_REF':             0,
            'SOURCE_DECILES':         get_deciles(inj_src_all),
            'HARM_INJ_DECILES':       get_deciles(inj_harm_all),
            'OMOP_REF_DECILES':       "NA",
        })

        if pbar:
            pbar.update(1)

    if pbar:
        pbar.close()

    pd.DataFrame(results).to_csv(args.output, sep='\t', index=False)
    logger.info(f"Audit complete. Results in {args.output}")


if __name__ == "__main__":
    main()
