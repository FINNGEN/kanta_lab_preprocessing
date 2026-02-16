import argparse
import subprocess
import os
import pandas as pd
import numpy as np
from scipy import stats
import io

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

def run_clickhouse(q, out):
    """Runs a query and writes direct to a file path."""
    try:
        with open(out, "w") as f:
            subprocess.run(["clickhouse", "local", "-q", q], stdout=f, check=True)
    except Exception as e:
        print(f"[!] ClickHouse execution error: {e}")

def run_clickhouse_to_df(q):
    """Runs a query and returns a pandas DataFrame."""
    try:
        res = subprocess.run(["clickhouse", "local", "-q", q], capture_output=True, text=True, check=True)
        return pd.read_csv(io.StringIO(res.stdout), sep='\t', keep_default_na=False, na_values=[''])
    except Exception as e:
        print(f"[!] ClickHouse to DF error: {e}")
        return pd.DataFrame()

def main():
    parser = argparse.ArgumentParser(description="Audit Script - Target Unit Logic with '-' Placeholder")
    parser.add_argument("input", help="Path to the input Parquet file")
    parser.add_argument("-u", "--unmapped-output", required=True, help="Output TSV for unmapped tests")
    parser.add_argument("-a", "--audit-output", required=True, help="Output TSV for the audited mismatches")
    parser.add_argument("--min_count", type=int, default=1, help="Min count to include")
    parser.add_argument("--ks-n", type=int, default=5000, help="N samples per group")
    args = parser.parse_args()

    # --- 1. PRE-INITIALIZATION (Ensure files exist no matter what) ---
    for out_path in [args.unmapped_output, args.audit_output]:
        os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    audit_headers = [
        'OMOP_ID', 'TEST_ABBR', 'ORIG_UNIT', 'TARGET_UNIT', 
        'N_SOURCE', 'N_REF_TOTAL', 'N_HARM_KS', 
        'KS_STAT', 'MLOGP', 'NOTES', 
        'SOURCE_DECILES', 'HARM_DECILES'
    ]
    unmapped_headers = ['TEST_NAME', 'MEASUREMENT_UNIT', 'COUNT', 'HAS_ANY_MAPPING']

    # Initialize files with headers
    pd.DataFrame(columns=audit_headers).to_csv(args.audit_output, sep='\t', index=False)
    pd.DataFrame(columns=unmapped_headers).to_csv(args.unmapped_output, sep='\t', index=False)

    # Column Constants
    COL_OMOP = "OMOP_CONCEPT_ID"
    COL_TEST = "TEST_NAME"
    COL_UNIT_CLEAN = "MEASUREMENT_UNIT_CLEANED"
    COL_VAL_CLEAN = "MEASUREMENT_VALUE_CLEANED"
    COL_HARM_VAL = "MEASUREMENT_VALUE_HARMONIZED"
    COL_HARM_UNIT = "MEASUREMENT_UNIT_HARMONIZED"

    # --- 2. UNMAPPED LOGIC (With min_count filter) ---
    print(f"[*] Step 0: Extracting unmapped tests (Min Count: {args.min_count})...")
    unmapped_query = f"""
    WITH 
        mapped_tests AS (
            SELECT DISTINCT {COL_TEST}
            FROM file('{args.input}', Parquet)
            WHERE {COL_OMOP} IS NOT NULL AND {COL_OMOP} != '' AND {COL_OMOP} != '-1'
        )
    SELECT 
        ifNull({COL_TEST}, 'NA') AS {COL_TEST},
        ifNull({COL_UNIT_CLEAN}, 'NA') AS MEASUREMENT_UNIT,
        count() AS COUNT,
        if({COL_TEST} IN mapped_tests, 'True', 'False') AS HAS_ANY_MAPPING
    FROM file('{args.input}', Parquet)
    WHERE {COL_OMOP} IS NULL OR {COL_OMOP} = '' OR {COL_OMOP} = '-1'
    GROUP BY {COL_TEST}, MEASUREMENT_UNIT
    HAVING COUNT >= {args.min_count}
    ORDER BY COUNT DESC
    FORMAT TabSeparated
    """
    df_unmapped_data = run_clickhouse_to_df(unmapped_query)
    if not df_unmapped_data.empty:
        df_unmapped_data.to_csv(args.unmapped_output, sep='\t', index=False, header=False, mode='a')
    print(f"[+] Unmapped file updated: {args.unmapped_output}")

    # --- 3. CANDIDATE IDENTIFICATION ---
    print(f"[*] Step 1 & 2: Querying ClickHouse for audit candidates...")
    
    global_ref_q = f"""
    SELECT {COL_OMOP}, 
           count({COL_HARM_VAL}) AS N_REF_TOTAL, 
           topK(1)({COL_HARM_UNIT})[1] AS REF_UNIT,
           quantiles(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)({COL_HARM_VAL}) AS HARM_DECILES
    FROM file('{args.input}', Parquet) 
    WHERE {COL_HARM_VAL} IS NOT NULL 
    GROUP BY {COL_OMOP}
    """

    audit_query = f"""
    WITH ref_stats AS ({global_ref_q})
    SELECT a.{COL_OMOP} AS OMOP_ID, 
           ifNull(nullIf(a.{COL_TEST}, ''), 'NA') AS TEST_ABBR, 
           ifNull(nullIf(a.{COL_UNIT_CLEAN}, ''), 'NA') AS ORIG_UNIT,
           ifNull(r.REF_UNIT, 'NA') AS UNIT_POOL,
           count() AS N_SOURCE, 
           ifNull(r.N_REF_TOTAL, 0) AS N_REF_TOTAL,
           quantiles(0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0)(a.{COL_VAL_CLEAN}) AS SOURCE_DECILES,
           ifNull(toString(r.HARM_DECILES), 'NA') AS HARM_DECILES
    FROM file('{args.input}', Parquet) AS a
    LEFT JOIN ref_stats AS r ON a.{COL_OMOP} = r.{COL_OMOP}
    WHERE (a.{COL_OMOP} IS NOT NULL AND a.{COL_OMOP} != '' AND a.{COL_OMOP} != '-1')
      AND a.{COL_VAL_CLEAN} IS NOT NULL AND a.{COL_HARM_VAL} IS NULL
    GROUP BY OMOP_ID, TEST_ABBR, ORIG_UNIT, UNIT_POOL, N_REF_TOTAL, HARM_DECILES
    HAVING N_SOURCE >= {args.min_count} 
    ORDER BY N_SOURCE DESC 
    FORMAT TabSeparatedWithNames
    """
    
    df_audit = run_clickhouse_to_df(audit_query)
    
    if df_audit.empty:
        print("[!] No audit candidates found. Audit file remains empty (headers only).")
        return
    
    print(f"[+] Step 2 Complete: {len(df_audit)} candidates for auditing.")

    # --- 4. TARGETED KS SAMPLING ---
    omops_to_test = df_audit[df_audit['N_REF_TOTAL'] > 0]['OMOP_ID'].unique().astype(str).tolist()
    
    if omops_to_test:
        print(f"[*] Step 3: Fetching raw samples (N={args.ks_n})...")
        omop_str = "'" + "','".join(omops_to_test) + "'"
        src_q = f"SELECT {COL_OMOP}, {COL_TEST}, {COL_VAL_CLEAN} FROM file('{args.input}', Parquet) WHERE {COL_OMOP} IN ({omop_str}) AND {COL_VAL_CLEAN} IS NOT NULL LIMIT {args.ks_n} BY {COL_OMOP}, {COL_TEST} FORMAT TabSeparatedWithNames"
        ref_q = f"SELECT {COL_OMOP}, {COL_HARM_VAL} FROM file('{args.input}', Parquet) WHERE {COL_OMOP} IN ({omop_str}) AND {COL_HARM_VAL} IS NOT NULL LIMIT {args.ks_n} BY {COL_OMOP} FORMAT TabSeparatedWithNames"
        
        src_samples = run_clickhouse_to_df(src_q)
        ref_samples = run_clickhouse_to_df(ref_q)
    else:
        src_samples, ref_samples = pd.DataFrame(), pd.DataFrame()

    # --- 5. STATISTICS LOOP ---
    print(f"[*] Step 4: Running KS calculations (D < 0.3 Success)...")
    df_audit['TARGET_UNIT'] = "-" 
    df_audit['KS_STAT'] = "NA"
    df_audit['MLOGP'] = "NA"
    df_audit['N_HARM_KS'] = 0
    df_audit['NOTES'] = "NO_REF_DATA"

    for idx, row in tqdm(df_audit.iterrows(), total=len(df_audit), desc="Auditing"):
        omop, test = str(row['OMOP_ID']), str(row['TEST_ABBR'])
        if row['N_REF_TOTAL'] == 0: continue

        s2_pool = ref_samples[ref_samples[COL_OMOP].astype(str) == omop][COL_HARM_VAL].values
        s1 = src_samples[(src_samples[COL_OMOP].astype(str) == omop) & (src_samples[COL_TEST].astype(str) == test)][COL_VAL_CLEAN].values

        if len(s1) > 20 and len(s2_pool) > 20:
            s1_clean = s1[np.isfinite(s1)]
            s2_clean = s2_pool[np.isfinite(s2_pool)]
            df_audit.at[idx, 'N_HARM_KS'] = len(s2_clean)

            d_stat, p_val = stats.ks_2samp(s1_clean, s2_clean)
            df_audit.at[idx, 'KS_STAT'] = round(d_stat, 4)
            df_audit.at[idx, 'MLOGP'] = round(-np.log10(max(p_val, 1e-300)), 2)
            
            if d_stat < 0.3:
                df_audit.at[idx, 'TARGET_UNIT'] = row['UNIT_POOL']
                df_audit.at[idx, 'NOTES'] = "SUCCESS"
            else:
                df_audit.at[idx, 'NOTES'] = "KS_FAIL"
        else:
            df_audit.at[idx, 'NOTES'] = "INSUFFICIENT_DATA"

    # Final Columns Setup & Write
    df_audit = df_audit[audit_headers]
    df_audit.to_csv(args.audit_output, sep='\t', index=False, na_rep='NA')
    print(f"\n[!] All Steps Finished.\nAudit: {args.audit_output}\nUnmapped: {args.unmapped_output}")

if __name__ == "__main__":
    main()
