#!/usr/bin/env python3
"""Polars/parquet port of extract_pos_counts.py, producing identical output files.

Reads the engine's main output parquet (not _RELEASE, which has no MEASUREMENT_FREE_TEXT) and
writes, in the current directory:
  pos_neg_summary.tsv / pos_neg_summary_pasteable.tsv     free texts containing pos/neg
  plusplus_summary.tsv / plusplus_summary_pasteable.tsv   free texts containing "+", per OMOP_ID
each compared against the existing mapping table (ratio_COUNT/ratio_Npeople/NOTES, new entries
flagged), so the results can be curated and dropped back in as negpos_mapping.tsv /
kanta_plusplus_abnormality.tsv.

Scanning, filtering and counting run in polars; the (small) aggregated tables then go through
the same pandas post-processing as extract_pos_counts.py so row order, number formatting and
notes match exactly.

--map/--pn_orig/--plus_orig default to the engine's own tables in src/kanta/engine/data/,
located relative to this script, so they only need passing to compare against other versions.

Usage
-----
  python3 scripts/qc_scripts/extract_pos_counts_parquet.py <engine_output>.parquet
"""
import argparse
import os
import sys
from pathlib import Path

import pandas as pd
import polars as pl

# Constants
DEFAULT_TEST_LINES = 1_000_000

# Engine reference tables, relative to this script (scripts/qc_scripts/ -> repo root).
_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "src/kanta/engine/data"
DEFAULT_MAP = _DATA_DIR / "LABfi.tsv"
DEFAULT_PN_ORIG = _DATA_DIR / "negpos_mapping.tsv"
DEFAULT_PLUS_ORIG = _DATA_DIR / "kanta_plusplus_abnormality.tsv"

# Tokens pandas.read_csv turns into NaN by default. extract_pos_counts.py read TSV with pandas
# and then filled NaN back with "NA", so the same tokens are normalized to "NA" here.
PANDAS_NA_TOKENS = [
    "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan", "1.#IND", "1.#QNAN",
    "<NA>", "N/A", "NA", "NULL", "NaN", "None", "n/a", "nan", "null",
]

INPUT_COLUMNS = {
    "id": "FINNGENID",
    "text": "MEASUREMENT_FREE_TEXT",
    "pos": "EXTRACTED::IS_POS",
    "omop": "HARMONIZATION_OMOP::OMOP_ID",
}


def escape_sheets(val):
    s = str(val)
    if s.startswith(('+', '-', '=')):
        return f"'{s}"
    return s


def apply_reconciliation(new_df, ref_path, join_cols):
    """
    Standardizes join logic and calculates numerical ratios (New/Old).
    """
    # Initialize default columns
    new_df['ratio_COUNT'] = "NA"
    new_df['ratio_Npeople'] = "NA"
    new_df['NOTES'] = "NA"

    if not ref_path or not os.path.exists(ref_path):
        return new_df

    try:
        # Load ref - force string for keys
        ref_df = pd.read_csv(ref_path, sep='\t', dtype=str).fillna("NA")

        # Identify relevant columns in reference
        notes_col = next((c for c in ref_df.columns if c.upper() == "NOTES"), None)
        count_col = next((c for c in ref_df.columns if c.upper() == "COUNT"), None)
        nppl_col = next((c for c in ref_df.columns if c.upper() == "NPEOPLE"), None)

        # Create shadow keys for matching
        def make_shadow_key(df, cols):
            return df[cols].astype(str).apply(lambda x: "_".join(x.str.strip()), axis=1)

        new_df['_match_key'] = make_shadow_key(new_df, join_cols)
        ref_df['_match_key'] = make_shadow_key(ref_df, join_cols)

        # Drop duplicates in ref to prevent row explosion
        ref_df = ref_df.drop_duplicates('_match_key').set_index('_match_key')

        def get_ratio(new_val, old_series, key):
            try:
                if key not in old_series.index:
                    return "NA"
                old_val = float(old_series.get(key, 0))
                if old_val == 0:
                    return "NA"
                # Return as a float rounded to 3 decimal places for clean sorting
                return round(float(new_val) / old_val, 3)
            except:
                return "NA"

        # 1. Calculate Ratios
        if count_col:
            ref_counts = pd.to_numeric(ref_df[count_col], errors='coerce').fillna(0)
            new_df['ratio_COUNT'] = new_df.apply(
                lambda row: get_ratio(row['COUNT'], ref_counts, row['_match_key']), axis=1
            )

        if nppl_col:
            ref_nppl = pd.to_numeric(ref_df[nppl_col], errors='coerce').fillna(0)
            new_df['ratio_Npeople'] = new_df.apply(
                lambda row: get_ratio(row['Npeople'], ref_nppl, row['_match_key']), axis=1
            )

        # 2. Map Notes
        ref_keys = set(ref_df.index)
        def finalize_note(key):
            if key not in ref_keys:
                return "!! WARNING: NEW ENTRY !!"
            if notes_col:
                val = str(ref_df.loc[key, notes_col]).strip()
                return val if val not in ["NA", "nan", "None", ""] else "NA"
            return "NA"

        new_df['NOTES'] = new_df['_match_key'].apply(finalize_note)
        new_df.drop(columns=['_match_key'], inplace=True)

    except Exception as e:
        print(f"  Warning: Reconciliation failed: {e}")

    return new_df


def load_omop_names(map_file):
    """OMOP_ID -> concept name, from the Usagi mapping table (LABfi.tsv)."""
    omop_map = {}
    if map_file and os.path.exists(map_file):
        try:
            m_df = pd.read_csv(
                map_file,
                sep='\t',
                usecols=['harmonization_omop::OMOP_ID', 'harmonization_omop::OMOP_NAME'],
                dtype=str,
            )
            omop_map = dict(zip(m_df['harmonization_omop::OMOP_ID'], m_df['harmonization_omop::OMOP_NAME']))
        except Exception as e:
            print(f"Warning: Could not read map file: {e}")
    return omop_map


def scan_input(input_file, test_lines=None):
    """Lazily select the 4 needed columns (matched case-insensitively), as strings with
    pandas' NA tokens normalized to "NA"."""
    actual_cols = pl.scan_parquet(input_file).collect_schema().names()
    col_map = {}
    for key, wanted in INPUT_COLUMNS.items():
        match = next((c for c in actual_cols if c.upper() == wanted), None)
        if match is None:
            sys.exit(f"Error: column {wanted} not found in {input_file}")
        col_map[key] = match

    lf = pl.scan_parquet(input_file)
    if test_lines:
        lf = lf.head(test_lines)
    return lf.select(
        pl.when(pl.col(col).cast(pl.String).is_null() | pl.col(col).cast(pl.String).is_in(PANDAS_NA_TOKENS))
        .then(pl.lit("NA"))
        .otherwise(pl.col(col).cast(pl.String))
        .alias(key)
        for key, col in col_map.items()
    )


def aggregate(lf, row_filter, group_cols):
    """COUNT (rows) and Npeople (distinct FINNGENID) per group, as a pandas frame sorted by
    the group keys -- the same shape pandas' groupby().agg().reset_index() produces."""
    df = (
        lf.filter(row_filter)
        .group_by(group_cols)
        .agg(COUNT=pl.len(), Npeople=pl.col("id").n_unique())
        .sort(group_cols)
        .collect(engine="streaming")
        .to_pandas()
    )
    return df.astype({"COUNT": "int64", "Npeople": "int64"})


def process_data(input_file, map_file, pn_orig=None, plus_orig=None, test_lines=None):
    omop_map = load_omop_names(map_file)

    print(f"Reading {input_file}...")
    lf = scan_input(input_file, test_lines)

    print("Aggregating...")

    # Column layout
    pn_cols = ['MEASUREMENT_FREE_TEXT', 'extracted::IS_POS', 'COUNT', 'ratio_COUNT', 'Npeople', 'ratio_Npeople', 'NOTES']
    pl_cols = ['harmonization_omop::OMOP_ID', 'MEASUREMENT_FREE_TEXT', 'extracted::IS_POS', 'DESC', 'COUNT', 'ratio_COUNT', 'Npeople', 'ratio_Npeople', 'NOTES']

    pn_res = aggregate(lf, pl.col("text").str.contains("(?i)pos|neg"), ["text", "pos"])
    pn_res = pn_res[pn_res['Npeople'] >= 5].sort_values('COUNT', ascending=False)
    pn_res = pn_res.rename(columns={'text': 'MEASUREMENT_FREE_TEXT', 'pos': 'extracted::IS_POS'})
    pn_res = apply_reconciliation(pn_res, pn_orig, ['MEASUREMENT_FREE_TEXT', 'extracted::IS_POS'])

    pn_res[pn_cols].to_csv("pos_neg_summary.tsv", sep='\t', index=False)
    pn_res[pn_cols].map(escape_sheets).to_csv("pos_neg_summary_pasteable.tsv", sep='\t', index=False)

    plus_filter = pl.col("text").str.contains("+", literal=True) & (pl.col("omop") != "-1")
    pl_res = aggregate(lf, plus_filter, ["omop", "text", "pos"])
    pl_res = pl_res[pl_res['Npeople'] >= 5].sort_values('COUNT', ascending=False)
    pl_res = pl_res.rename(columns={
        'omop': 'harmonization_omop::OMOP_ID',
        'text': 'MEASUREMENT_FREE_TEXT',
        'pos': 'extracted::IS_POS'
    })
    pl_res['DESC'] = pl_res['harmonization_omop::OMOP_ID'].map(lambda x: omop_map.get(x, "NOT_IN_MAP"))
    pl_res = apply_reconciliation(pl_res, plus_orig,
                                  ['harmonization_omop::OMOP_ID', 'MEASUREMENT_FREE_TEXT', 'extracted::IS_POS'])

    pl_res[pl_cols].to_csv("plusplus_summary.tsv", sep='\t', index=False)
    pl_res[pl_cols].map(escape_sheets).to_csv("plusplus_summary_pasteable.tsv", sep='\t', index=False)

    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("input", help="Engine main output parquet (needs MEASUREMENT_FREE_TEXT)")
    parser.add_argument("--map", default=DEFAULT_MAP,
                        help="Usagi mapping table for OMOP concept names (default: %(default)s)")
    parser.add_argument("--pn_orig", default=DEFAULT_PN_ORIG,
                        help="Existing pos/neg table to compare against (default: %(default)s)")
    parser.add_argument("--plus_orig", default=DEFAULT_PLUS_ORIG,
                        help="Existing plus table to compare against (default: %(default)s)")
    parser.add_argument("--test", nargs='?', const=DEFAULT_TEST_LINES, type=int)
    args = parser.parse_args()
    process_data(args.input, args.map, args.pn_orig, args.plus_orig, args.test)
