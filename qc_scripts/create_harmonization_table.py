import pandas as pd
import argparse
import sys
import os
import json
import csv

def get_mode_stats(group, col_name, total_n):
    """Calculates mode (ignoring NA) and top 3 prevalence (including NA)."""
    stats = group.groupby(col_name, dropna=False)['n'].sum().sort_values(ascending=False)
    
    valid = stats[stats.index.notna() & (stats.index.astype(str).str.lower() != 'nan')]
    mode = valid.index[0] if not valid.empty else "NA"
    
    top_3 = stats.head(3)
    prevalence = {
        (str(u) if pd.notna(u) and str(u).lower() != 'nan' else "NA"): round(count / total_n, 4) 
        for u, count in top_3.items()
    }
    return mode, prevalence

def main():
    # --- Setup ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_ref_path = os.path.abspath(os.path.join(script_dir, "..", "finngen_qc", "data", "harmonization_counts.tsv"))

    parser = argparse.ArgumentParser()
    parser.add_argument("input", nargs='?', default=os.path.expanduser("~/fg-3/kanta_v3/munged/kanta_v3_harmonized_2026_02_02.txt.gz"))
    parser.add_argument("--ref", default=default_ref_path)
    parser.add_argument("--test", nargs='?', type=int, const=2000000)
    parser.add_argument("--out", default="harmonization_counts.tsv")
    parser.add_argument("--diff-out", default="harmonization_diffs.tsv")
    args = parser.parse_args()

    id_in = "harmonization_omop::OMOP_ID"
    qt_in = "harmonization_omop::omopQuantity"
    un_injected = "cleaned::MEASUREMENT_UNIT"
    un_source = "cleaned-pre-fix::MEASUREMENT_UNIT"
    un_out = "harmonization_omop::MEASUREMENT_UNIT"

    # --- Load Reference ---
    reference_dict = {}
    if os.path.exists(args.ref):
        ref_df = pd.read_csv(args.ref, sep='\t', usecols=[id_in, qt_in, un_out])
        reference_dict = {(str(r[id_in]), str(r[qt_in])): str(r[un_out]) for _, r in ref_df.iterrows()}

    # --- Vectorized Chunk Reading ---
    # We use a list to store summarized dataframes.
    # Appending to a list is O(1), whereas pd.concat in a loop is O(N^2) because it copies data every time.
    summaries = []

    reader = pd.read_csv(
        args.input, 
        sep='\t', 
        usecols=[id_in, qt_in, un_injected, un_source],
        nrows=args.test,
        chunksize=1_000_000, 
        engine='c',
        low_memory=False
    )

    print(f"--- Reading and Summarizing Chunks ---")
    for i, chunk in enumerate(reader):
        print(f" > Reading: {i+1}M lines...", end='\r', flush=True)
        
        # Vectorized filtering of invalid IDs
        chunk = chunk[~chunk[id_in].isin([0, -1, "0", "-1"])]
        if chunk.empty: continue

        # Local aggregation (reduces million rows to a few thousand unique combos)
        chunk_summary = chunk.groupby([id_in, qt_in, un_injected, un_source], dropna=False).size().reset_index(name='n')
        summaries.append(chunk_summary)

    # One single vectorized collapse of all chunk summaries
    print("\n--- Final Vectorized Collapse ---")
    full_counts = pd.concat(summaries, ignore_index=True)
    full_counts = full_counts.groupby([id_in, qt_in, un_injected, un_source], dropna=False, as_index=False).sum()

    # --- Analysis & Output ---
    final_rows = []
    diff_rows = []

    print("--- Calculating Modes and Comparisons ---")
    for (omop_id, qty), group in full_counts.groupby([id_in, qt_in], dropna=False):
        group_total_n = group['n'].sum()
        
        source_mode, source_prev = get_mode_stats(group, un_source, group_total_n)
        injected_mode, injected_prev = get_mode_stats(group, un_injected, group_total_n)

        note_msg = "Injected matches Source"
        impact_flag = False

        if injected_mode != source_mode:
            note_msg = f"INJECTION CHANGE: Source mode was '{source_mode}'"
            impact_flag = True

        ref_key = (str(omop_id), str(qty))
        ref_unit_val = reference_dict.get(ref_key, "N/A")
        
        if ref_unit_val != "N/A":
            if str(injected_mode) != ref_unit_val:
                note_msg += f" | REF DIFF: ref was {ref_unit_val}"
                impact_flag = True
        elif injected_mode != "NA":
            note_msg += " | NEW MAPPING"
            impact_flag = True

        row_data = {
            id_in: omop_id,
            qt_in: qty,
            un_out: injected_mode,
            "cleaned-source": source_mode,
            "REF_UNIT": ref_unit_val,
            "NOTES": note_msg,
            "PREV_SOURCE": json.dumps(source_prev),
            "PREV_INJECTED": json.dumps(injected_prev),
            "_total_count": group_total_n
        }
        
        final_rows.append(row_data)
        if impact_flag:
            diff_rows.append(row_data)

    # Final Output
    result_df = pd.DataFrame(final_rows).sort_values("_total_count", ascending=False)
    result_df[[id_in, qt_in, un_out, "NOTES", "PREV_SOURCE", "PREV_INJECTED"]].to_csv(
        args.out, sep='\t', index=False, quoting=csv.QUOTE_NONE, escapechar='\\'
    )
    
    if diff_rows:
        diff_df = pd.DataFrame(diff_rows).sort_values("_total_count", ascending=False).drop(columns=["_total_count"])
        diff_df.rename(columns={un_out: "cleaned-injected"}).to_csv(
            args.diff_out, sep='\t', index=False, quoting=csv.QUOTE_NONE, escapechar='\\'
        )
    
    print(f"Done. Main results: {args.out}")

if __name__ == "__main__":
    main()
