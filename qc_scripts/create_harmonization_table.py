import pandas as pd
import argparse
import sys
import os
import json
import csv

def main():
    # Determine the script directory to set up relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Path to reference: go up one level from 'qc_scripts' then into 'finngen_qc/data'
    default_ref_path = os.path.abspath(os.path.join(
        script_dir, "..", "finngen_qc", "data", "harmonization_counts.tsv"
    ))

    parser = argparse.ArgumentParser(description="Find common units and output differences to a file.")
    parser.add_argument("input", nargs='?', 
                        default=os.path.expanduser("~/fg-3/kanta_v3/munged/kanta_v3_harmonized_2026_02_02.txt.gz"), 
                        help="Path to the input .gz or .txt file")
    
    parser.add_argument("--ref", 
                        default=default_ref_path,
                        help="Path to the reference file for comparison")
    
    parser.add_argument("--test", nargs='?', type=int, const=2000000, help="Process limited lines")
    parser.add_argument("--out", default="harmonization_counts.tsv", help="Main output filename")
    parser.add_argument("--diff-out", default="harmonization_diffs.tsv", help="Filename for differences only")
    args = parser.parse_args()

    # Column Mapping
    id_in = "harmonization_omop::OMOP_ID"
    qt_in = "harmonization_omop::omopQuantity"
    un_primary = "cleaned::MEASUREMENT_UNIT"
    un_prefix = "cleaned-pre-fix::MEASUREMENT_UNIT"
    un_out = "harmonization_omop::MEASUREMENT_UNIT"

    if not os.path.exists(args.input):
        print(f"Error: Input file not found at {args.input}")
        sys.exit(1)

    # Load Reference File
    reference_dict = {}
    if os.path.exists(args.ref):
        print(f"--- Loading Reference: {args.ref} ---")
        try:
            ref_df = pd.read_csv(args.ref, sep='\t', usecols=[id_in, qt_in, un_out])
            for _, row in ref_df.iterrows():
                key = (str(row[id_in]), str(row[qt_in]))
                reference_dict[key] = str(row[un_out])
        except Exception as e:
            print(f"Warning: Could not parse reference: {e}")
    else:
        print(f"Warning: Reference file not found at {args.ref}. Comparison file will only contain new mappings.")

    print(f"--- Starting Process ---")
    
    try:
        reader = pd.read_csv(
            args.input, 
            sep='\t', 
            usecols=[id_in, qt_in, un_primary, un_prefix],
            nrows=args.test,
            chunksize=1_000_000, 
            engine='c',
            low_memory=False
        )

        full_counts = pd.DataFrame()

        for i, chunk in enumerate(reader):
            # Progress in Millions of lines
            print(f" > Processing: {i+1}M lines...", end='\r', flush=True)
            
            # Filter out OMOP_ID 0 and -1
            chunk = chunk[~chunk[id_in].isin([0, -1, "0", "-1"])]
            if chunk.empty: continue

            chunk_counts = chunk.groupby([id_in, qt_in, un_primary, un_prefix], dropna=False).size().reset_index(name='n')
            full_counts = pd.concat([full_counts, chunk_counts])
            full_counts = full_counts.groupby([id_in, qt_in, un_primary, un_prefix], dropna=False, as_index=False).sum()

        print("\n--- Aggregating Results & Generating Diffs ---")
        
        full_counts['effective_unit'] = full_counts[un_primary].fillna(full_counts[un_prefix])
        final_rows = []
        diff_rows = []

        for (omop_id, qty), group in full_counts.groupby([id_in, qt_in], dropna=False):
            group_total_n = group['n'].sum()
            unit_stats = group.groupby('effective_unit', dropna=False)['n'].sum().sort_values(ascending=False)
            
            top_3 = unit_stats.head(3)
            prevalence = {
                (str(u) if pd.notna(u) and str(u).lower() != 'nan' else "NA"): round(count / group_total_n, 4) 
                for u, count in top_3.items()
            }
            
            valid_units = unit_stats[unit_stats.index.notna() & (unit_stats.index.astype(str).str.lower() != 'nan')]
            
            if not valid_units.empty:
                mode_unit = valid_units.index[0]
                mode_display = mode_unit if str(mode_unit).lower() != 'nan' else "NA"
                mode_subset = group[group['effective_unit'] == mode_unit]
                is_fallback = mode_subset[un_primary].isna().all()
                note_msg = "Used fallback (pre-fix)" if is_fallback else "Used primary"
            else:
                mode_display = "NA"
                note_msg = "No valid units found (All NA)"

            # Comparison Logic
            ref_key = (str(omop_id), str(qty))
            ref_unit_val = "N/A"
            is_diff = False
            
            if ref_key in reference_dict:
                ref_unit_val = reference_dict[ref_key]
                if str(mode_display) != ref_unit_val:
                    note_msg += f" | DIFF: ref was {ref_unit_val}"
                    is_diff = True
            elif mode_display != "NA":
                note_msg += " | NEW MAPPING"
                is_diff = True

            row_data = {
                id_in: omop_id,
                qt_in: qty,
                un_out: mode_display,
                "REF_UNIT": ref_unit_val,
                "NOTES": note_msg,
                "PREVALENCE": json.dumps(prevalence),
                "_total_count": group_total_n
            }
            
            final_rows.append(row_data)
            if is_diff:
                diff_rows.append(row_data)

        # Main Output
        result_df = pd.DataFrame(final_rows).sort_values("_total_count", ascending=False)
        main_out_df = result_df.drop(columns=["_total_count", "REF_UNIT"])
        main_out_df.to_csv(args.out, sep='\t', index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
        
        # Diffs Output
        if diff_rows:
            diff_df = pd.DataFrame(diff_rows).sort_values("_total_count", ascending=False).drop(columns=["_total_count"])
            diff_df.to_csv(args.diff_out, sep='\t', index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
            print(f"Comparison complete: {len(diff_rows)} differences/new mappings saved to {args.diff_out}")
        else:
            print("Comparison complete: No differences found.")

        print(f"Success! Full results saved to: {args.out}")

    except Exception as e:
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
