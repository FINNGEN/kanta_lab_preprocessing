import pandas as pd
import json
import os
import argparse
import subprocess
from io import StringIO

def get_mode_stats(group, col):
    """Calculates distribution and prioritizes real units over NA."""
    counts = group.groupby(col, dropna=False)['n'].sum()
    if counts.empty:
        return "NA", {}
    
    total = counts.sum()
    processed_counts = {}
    
    # Normalize keys to 'NA' or the real string
    for k, v in counts.items():
        key = "NA" if str(k) in ['\\N', 'nan', 'None', '', 'NA'] else str(k)
        processed_counts[key] = processed_counts.get(key, 0) + int(v)

    prev = {k: round(v/total, 4) for k, v in processed_counts.items()}
    
    # Filter to find actual units
    real_units = {k: v for k, v in processed_counts.items() if k != "NA"}
    
    if real_units:
        # Pick the most frequent unit that is NOT NA
        final_mode = max(real_units, key=real_units.get)
    else:
        final_mode = "NA"
        
    return final_mode, prev


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_ref_path = os.path.abspath(os.path.join(script_dir, "..", "finngen_qc", "data", "harmonization_counts.tsv"))

    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Input Parquet file")
    parser.add_argument("--ref", default=default_ref_path)
    parser.add_argument("--out", default="harmonization_counts.tsv")
    parser.add_argument("--diff-out", default="harmonization_diffs.tsv")
    parser.add_argument("--min-count", type=int, default=1)
    args = parser.parse_args()

    # Legacy Headers
    id_ref = "harmonization_omop::OMOP_ID"
    qt_ref = "harmonization_omop::omopQuantity"
    un_ref = "harmonization_omop::MEASUREMENT_UNIT"

    # --- Step 1: Detect Casing ---
    describe_cmd = ['clickhouse', '--query', f"DESCRIBE file('{args.input}', 'Parquet')", '--format', 'TSVWithNames']
    schema_info = pd.read_csv(StringIO(subprocess.check_output(describe_cmd).decode()), sep='\t')
    actual_cols = schema_info['name'].tolist()

    def get_actual_name(target):
        if target in actual_cols: return target
        if target.lower() in actual_cols: return target.lower()
        return None

    id_phys = get_actual_name("OMOP_CONCEPT_ID")
    qt_phys = get_actual_name("OMOP_QUANTITY")
    un_injected_phys = get_actual_name("MEASUREMENT_UNIT_CLEANED")
    un_source_phys = get_actual_name("MEASUREMENT_UNIT_PRE_FIX")

    # --- Step 2: Load Reference ---
    reference_dict = {}
    reference_manual_dict = {}
    if os.path.exists(args.ref):
        ref_df = pd.read_csv(args.ref, sep='\t').fillna('NA')
        reference_dict = {
            (str(r[id_ref]), str(r[qt_ref])): str(r[un_ref]) 
            for _, r in ref_df.iterrows() if id_ref in ref_df.columns
        }
        # Load UNIT_SOURCE column to check for MANUAL mappings
        if "UNIT_SOURCE" in ref_df.columns:
            reference_manual_dict = {
                (str(r[id_ref]), str(r[qt_ref])): str(r["UNIT_SOURCE"]) 
                for _, r in ref_df.iterrows() if id_ref in ref_df.columns
            }

    # --- Step 3: Run Aggregation with NULL handling ---
    # We use coalesce in SQL to prevent \N from ever entering the omopQuantity field
    query = f"""
        SELECT 
            toString({id_phys}) AS omop_id, 
            coalesce(nullIf(toString({qt_phys}), ''), 'NA') AS qty, 
            {un_injected_phys} AS injected, 
            {un_source_phys} AS source, 
            count() as n
        FROM file('{args.input}', 'Parquet')
        WHERE {id_phys} NOT IN (0, -1, '0', '-1')
        GROUP BY 1, 2, 3, 4
        HAVING n >= {args.min_count}
    """

    process = subprocess.run(['clickhouse', '--query', query, '--format', 'TSVWithNames'], 
                             capture_output=True, text=True, check=True)
    full_counts = pd.read_csv(StringIO(process.stdout), sep='\t').fillna('NA')

    # --- Step 4: Analysis ---
    final_rows, diff_rows = [], []
    def normalize_na(val):
        v = str(val).lower()
        return "na" if v in ["nan", "none", "n/a", "na", "", "null", "\\n"] else v

    for (omop_id, qty), group in full_counts.groupby(['omop_id', 'qty'], dropna=False):
        group_total_n = group['n'].sum()
        source_mode, source_prev = get_mode_stats(group, 'source')
        injected_mode, injected_prev = get_mode_stats(group, 'injected')

        note_msg = "Injected matches Source"
        impact_flag = False
        
        # Determine the source of the target unit
        if normalize_na(injected_mode) != normalize_na(source_mode):
            target_source = "INJECTED"
            note_msg = f"INJECTION CHANGE: Source mode was '{source_mode}'"
            impact_flag = True
        else:
            target_source = "SOURCE"

        ref_key = (str(omop_id), str(qty))
        ref_unit_val = reference_dict.get(ref_key, "N/A")
        ref_is_manual = reference_manual_dict.get(ref_key, "").upper() == "MANUAL"
        
        norm_injected = normalize_na(injected_mode)
        norm_ref = normalize_na(ref_unit_val)

        # If reference is marked as MANUAL, do not override it
        if ref_is_manual:
            final_unit = ref_unit_val
            note_msg = f"MANUAL MAPPING (locked) - Current data suggests: {injected_mode}"
            target_source = "MANUAL"
            impact_flag = False  # Don't flag manual mappings as diffs
        else:
            final_unit = injected_mode
            
            if ref_unit_val != "N/A":
                if norm_injected != norm_ref:
                    note_msg += f" | REF DIFF: ref was {ref_unit_val}"
                    impact_flag = True
            elif norm_injected != "na":
                note_msg += " | NEW MAPPING"
                impact_flag = True

        row_data = {
            id_ref: omop_id,
            qt_ref: qty,
            un_ref: final_unit,
            "UNIT_SOURCE": target_source,
            "NOTES": note_msg,
            "PREV_SOURCE": json.dumps(source_prev).replace('"', "'"),
            "PREV_INJECTED": json.dumps(injected_prev).replace('"', "'"),
            "_total_count": int(group_total_n)
        }
        final_rows.append(row_data)
        if impact_flag:
            diff_rows.append(row_data)

    if final_rows:
        res = pd.DataFrame(final_rows).sort_values("_total_count", ascending=False)
        output_cols = [id_ref, qt_ref, un_ref, "UNIT_SOURCE", "NOTES", "PREV_SOURCE", "PREV_INJECTED"]
        
        # Final cleanup for the dataframe to ensure no \N or nan escapes
        res = res.replace(['\\N', 'nan', None], 'NA')
        
        res[output_cols].to_csv(args.out, sep='\t', index=False)
        if diff_rows:
            diff_res = pd.DataFrame(diff_rows).sort_values("_total_count", ascending=False).replace(['\\N', 'nan', None], 'NA')
            diff_res[output_cols].to_csv(args.diff_out, sep='\t', index=False)
            
    print(f"Done. Output: {args.out}")

if __name__ == "__main__":
    main()
