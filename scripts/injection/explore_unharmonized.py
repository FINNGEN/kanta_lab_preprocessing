#!/usr/bin/env python3
"""
explore_unharmonized.py

Identifies lab measurements that hold a real source value but failed unit
harmonization (harmonization_omop::MEASUREMENT_VALUE == "NA"), and resolves an
OMOP_ID for each one of two ways:

  MAPPED          harmonization_omop::OMOP_ID is already a real concept (mapping
                   succeeded on the row's own (TEST_NAME_ABBREVIATION,
                   MEASUREMENT_UNIT) pair) — conversion itself is what failed
                   (no factor defined for this unit, including MEASUREMENT_UNIT
                   == "NA" i.e. the unit is missing entirely).
  ABBREV_FALLBACK  harmonization_omop::OMOP_ID is "0"/"NA" (the row's own unit
                   isn't a recognised (abbreviation, unit) pair in the Usagi
                   mapping table at all — e.g. "osuus" for b-hkr, which is only
                   mapped via "ratio"/"%"/blank) but TEST_NAME_ABBREVIATION
                   resolves to exactly one OMOP_ID via its OTHER, Usagi-mapped
                   units. That OMOP_ID is used as the candidate.

Abbreviations that resolve to 2+ distinct OMOP_IDs across their mapped units
can't be fallback-resolved automatically — rows for those get OMOP_ID="AMBIGUOUS".
Abbreviations absent from the Usagi mapping table entirely get OMOP_ID="UNMAPPED".

Usage
-----
  python3 explore_unharmonized.py PARQUET [--min-count 50] [--out unharmonized_counts.tsv]
"""

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_USAGI_FILE = _REPO_ROOT / "src/kanta/finngen_qc/data/LABfi_ALL.usagi.csv"


def clickhouse(query):
    proc = subprocess.run(["clickhouse", "-q", query], capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise RuntimeError("ClickHouse query failed")
    return proc.stdout


def query_raw_counts(parquet, min_count, cache="unharmonized_raw_counts.tsv"):
    """(TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID_RAW, COUNT) for every row with a real
    source value but a failed harmonized value. OMOP_ID_RAW is whatever omop_mapping already
    assigned the row (a real concept, or "0"/"NA" if that (abbreviation, unit) pair has no Usagi
    match at all) — resolve_omop_ids() turns this into a usable OMOP_ID."""
    if Path(cache).exists():
        print(f"{cache} already exists, skipping query.")
        return pd.read_csv(cache, sep="\t", keep_default_na=False, na_values=[""])

    result = clickhouse(f"""
        SELECT
            TEST_NAME_ABBREVIATION,
            MEASUREMENT_UNIT,
            `harmonization_omop::OMOP_ID` AS OMOP_ID_RAW,
            count() AS COUNT
        FROM file('{parquet}')
        WHERE MEASUREMENT_VALUE != 'NA'
          AND `harmonization_omop::MEASUREMENT_VALUE` = 'NA'
        GROUP BY TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID_RAW
        HAVING COUNT >= {min_count}
        ORDER BY COUNT DESC
        FORMAT TSVWithNames
    """)
    Path(cache).write_text(result)
    df = pd.read_csv(cache, sep="\t", keep_default_na=False, na_values=[""])
    print(f"Wrote {cache}  ({len(df)} rows)")
    return df


def load_abbrev_omop_map(usagi_path):
    """TEST_NAME_ABBREVIATION -> sorted list of distinct OMOP_IDs it maps to via ANY of its
    APPROVED, Measurement-domain units in the Usagi table (unit-agnostic, unlike omop_mapping's
    own per-row (abbreviation, unit) join)."""
    df = pd.read_csv(usagi_path, dtype=str)
    df = df[(df["domainId"] == "Measurement") & (df["mappingStatus"] == "APPROVED")]
    grouped = df.groupby("ADD_INFO:testNameAbbreviation")["conceptId"].agg(lambda s: sorted(set(s)))
    return grouped.to_dict()


def resolve_omop_ids(df, abbrev_map):
    """Add OMOP_ID/SOURCE columns, then re-aggregate COUNT since rows that only differed by a
    now-collapsed OMOP_ID_RAW (e.g. several "0"/"NA" variants) can merge into the same output row."""

    def resolve(row):
        raw = row["OMOP_ID_RAW"]
        if raw not in ("0", "NA", ""):
            return raw, "MAPPED"
        ids = abbrev_map.get(row["TEST_NAME_ABBREVIATION"])
        if not ids:
            return "UNMAPPED", "UNMAPPED"
        if len(ids) == 1:
            return ids[0], "ABBREV_FALLBACK"
        return "AMBIGUOUS", "AMBIGUOUS"

    resolved = df.apply(resolve, axis=1, result_type="expand")
    df = df.copy()
    df["OMOP_ID"] = resolved[0]
    df["SOURCE"] = resolved[1]

    out = (
        df.groupby(["TEST_NAME_ABBREVIATION", "MEASUREMENT_UNIT", "OMOP_ID", "SOURCE"])["COUNT"]
        .sum()
        .reset_index()
        .sort_values("COUNT", ascending=False)
        .reset_index(drop=True)
    )
    return out


def print_summary(df):
    total_rows = int(df["COUNT"].sum())
    print(f"\n{'=' * 60}")
    print("UNHARMONIZED SUMMARY")
    print(f"{'=' * 60}")
    print(f"  (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID) rows: {len(df):,}")
    print(f"  Distinct TEST_NAME_ABBREVIATION: {df['TEST_NAME_ABBREVIATION'].nunique():,}")
    print(f"  Distinct OMOP_ID:                {df.loc[~df['OMOP_ID'].isin(['AMBIGUOUS', 'UNMAPPED']), 'OMOP_ID'].nunique():,}")
    print(f"  Total measurements affected:     {total_rows:,}")
    print()
    print(f"  {'SOURCE':<16}  {'ROWS':>8}  {'MEASUREMENTS':>14}")
    for source, sub in df.groupby("SOURCE"):
        n = int(sub["COUNT"].sum())
        print(f"  {source:<16}  {len(sub):>8,}  {n:>14,}  ({100 * n / total_rows:.1f}%)")
    print()
    no_unit = df[df["MEASUREMENT_UNIT"] == "NA"]["COUNT"].sum()
    print(f"  Of which MEASUREMENT_UNIT == NA: {int(no_unit):,} "
          f"({100 * no_unit / total_rows:.1f}%)")
    print()


def build_parser():
    p = argparse.ArgumentParser(
        description="Extract unit-harmonization-failed lab measurements, resolving an OMOP_ID "
                    "either from the row's own mapping or (as a fallback) its abbreviation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("parquet", help="Input parquet file (engine output, e.g. kanta.parquet)")
    p.add_argument("--min-count", type=int, default=50, metavar="INT",
                   help="Minimum row count per (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID_RAW) to include")
    p.add_argument("--usagi", default=str(_DEFAULT_USAGI_FILE), metavar="PATH",
                   help="Usagi lab mapping CSV, for the abbreviation-level OMOP_ID fallback")
    p.add_argument("--out", default="unharmonized_counts.tsv", metavar="PATH",
                   help="Output TSV path (final, resolved table)")
    p.add_argument("--raw-cache", default="unharmonized_raw_counts.tsv", metavar="PATH",
                   help="Cache path for the (expensive) raw ClickHouse query; skips re-querying if present")
    return p


def main():
    args = build_parser().parse_args()
    raw = query_raw_counts(args.parquet, args.min_count, cache=args.raw_cache)
    abbrev_map = load_abbrev_omop_map(args.usagi)
    df = resolve_omop_ids(raw, abbrev_map)
    df.to_csv(args.out, sep="\t", index=False)
    print(f"Wrote {args.out}  ({len(df)} rows)")
    print_summary(df)


if __name__ == "__main__":
    main()
