#!/usr/bin/env python3
"""
omop_unit_table.py

Build a per-OMOP-concept unit conversion table.

Logic
-----
1. LABfi_ALL.usagi.csv  → OMOP_CONCEPT_ID → omopQuantity  (only these two fields matter)
2. quantity_source_unit_conversion.tsv → for each omopQuantity, what units exist
   and what are their pairwise conversions?

CATEGORY
--------
  SINGLE      — only one unit defined for this quantity
  EQUIVALENT  — multiple units, all pairwise conversions = 1
  AMBIGUOUS   — multiple units, at least one conversion ≠ 1
  NO_CONV     — quantity known but not in conversion table
  NO_QUANTITY — concept has no omopQuantity in LABfi

Usage
-----
  python3 omop_unit_table.py [--approved-only] [--out omop_unit_table.tsv]
"""

import argparse
from pathlib import Path

import pandas as pd

DATA_DIR = Path("~/Dropbox/Projects/kanta_lab_preprocessing/src/kanta/finngen_qc/data").expanduser()


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def load_lab(path, approved_only=False):
    """OMOP_CONCEPT_ID → (CONCEPT_NAME, omopQuantity). Units ignored."""
    df = pd.read_csv(path)
    df = df[df["domainId"] == "Measurement"]
    if approved_only:
        df = df[df["mappingStatus"] == "APPROVED"]
    df = df.rename(columns={
        "conceptId":             "OMOP_CONCEPT_ID",
        "conceptName":           "CONCEPT_NAME",
        "ADD_INFO:omopQuantity": "omop_quantity",
    })
    return df[["OMOP_CONCEPT_ID", "CONCEPT_NAME", "omop_quantity"]].drop_duplicates()


def load_conversions(path):
    df = pd.read_csv(path, sep="\t")
    df = df.rename(columns={
        "omop_quantity":        "omop_quantity",
        "source_unit_valid":    "unit_from",
        "to_source_unit_valid": "unit_to",
        "conversion":           "factor",
    })
    df = df[df["unit_from"].notna() & df["unit_to"].notna()]
    df["factor"] = pd.to_numeric(df["factor"], errors="coerce")
    return df[["omop_quantity", "unit_from", "unit_to", "factor"]]


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

def build_table(lab_df, conv_df):
    # Build per-quantity: valid units and conversion lookup
    qty_units  = {}   # omop_quantity -> sorted list of units
    conv_lookup = {}  # (omop_quantity, unit_from, unit_to) -> factor

    for _, row in conv_df.iterrows():
        q = row["omop_quantity"]
        qty_units.setdefault(q, set())
        if pd.notna(row["unit_from"]):
            qty_units[q].add(row["unit_from"])
        if pd.notna(row["unit_to"]):
            qty_units[q].add(row["unit_to"])
        if pd.notna(row["factor"]):
            conv_lookup[(q, row["unit_from"], row["unit_to"])] = float(row["factor"])

    qty_units = {q: sorted(us) for q, us in qty_units.items()}

    rows = []

    for omop_id, grp in lab_df.groupby("OMOP_CONCEPT_ID"):
        concept_name  = grp["CONCEPT_NAME"].iloc[0]
        omop_quantity = grp["omop_quantity"].iloc[0]

        if pd.isna(omop_quantity) or str(omop_quantity).strip() == "":
            rows.append(dict(
                OMOP_CONCEPT_ID=omop_id, CONCEPT_NAME=concept_name,
                OMOP_QUANTITY=omop_quantity, N_UNITS=0,
                UNITS="", CONVERSIONS="", CATEGORY="NO_QUANTITY",
                CANONICAL_UNIT=pd.NA,
            ))
            continue

        units   = qty_units.get(str(omop_quantity).strip(), [])
        n_units = len(units)

        if n_units == 0:
            rows.append(dict(
                OMOP_CONCEPT_ID=omop_id, CONCEPT_NAME=concept_name,
                OMOP_QUANTITY=omop_quantity, N_UNITS=0,
                UNITS="", CONVERSIONS="", CATEGORY="NO_CONV",
                CANONICAL_UNIT=pd.NA,
            ))
            continue

        if n_units == 1:
            rows.append(dict(
                OMOP_CONCEPT_ID=omop_id, CONCEPT_NAME=concept_name,
                OMOP_QUANTITY=omop_quantity, N_UNITS=1,
                UNITS=units[0], CONVERSIONS="1", CATEGORY="SINGLE",
                CANONICAL_UNIT=units[0],
            ))
            continue

        # Multiple units — check pairwise conversions
        factors = []
        missing = []
        for i, u1 in enumerate(units):
            for u2 in units[i + 1:]:
                fwd = conv_lookup.get((omop_quantity, u1, u2))
                rev = conv_lookup.get((omop_quantity, u2, u1))
                if fwd is not None:
                    factors.append(fwd)
                elif rev is not None:
                    factors.append(rev)
                else:
                    missing.append(f"{u1}↔{u2}")

        if missing:
            category  = "NO_CONV"
            canonical = pd.NA
        elif all(abs(f - 1.0) < 1e-9 for f in factors):
            category  = "EQUIVALENT"
            canonical = units[0]   # alphabetically first — all are equivalent
        else:
            category  = "AMBIGUOUS"
            canonical = pd.NA

        unique_factors = sorted(set(round(f, 8) for f in factors))
        rows.append(dict(
            OMOP_CONCEPT_ID=omop_id, CONCEPT_NAME=concept_name,
            OMOP_QUANTITY=omop_quantity, N_UNITS=n_units,
            UNITS="|".join(units),
            CONVERSIONS="|".join(str(f) for f in unique_factors),
            CATEGORY=category,
            CANONICAL_UNIT=canonical,
        ))

    df = pd.DataFrame(rows).sort_values(["CATEGORY", "OMOP_CONCEPT_ID"]).reset_index(drop=True)
    df["N_UNITS"] = df["N_UNITS"].astype("Int64")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Build per-OMOP-concept unit conversion table.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--lab-file",      default=str(DATA_DIR / "LABfi_ALL.usagi.csv"))
    p.add_argument("--conv-file",     default=str(DATA_DIR / "quantity_source_unit_conversion.tsv"))
    p.add_argument("--out",           default="omop_unit_table.tsv")
    p.add_argument("--approved-only", action="store_true",
                   help="Only include APPROVED mappings")
    args = p.parse_args()

    print(f"Loading lab mapping  ({'APPROVED only' if args.approved_only else 'all statuses'})...")
    lab_df = load_lab(args.lab_file, approved_only=args.approved_only)
    print(f"  {lab_df['OMOP_CONCEPT_ID'].nunique():>6} unique OMOP concepts")

    print("Loading conversion table...")
    conv_df = load_conversions(args.conv_file)
    print(f"  {len(conv_df):>6} conversion entries")

    print("Building table...")
    result = build_table(lab_df, conv_df)

    print(f"\nCategory breakdown ({len(result)} concepts total):")
    for cat, n in result["CATEGORY"].value_counts().items():
        print(f"  {cat:<15} {n:>5}  ({100*n/len(result):.1f}%)")

    result.to_csv(args.out, sep="\t", index=False, na_rep="NA")
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
