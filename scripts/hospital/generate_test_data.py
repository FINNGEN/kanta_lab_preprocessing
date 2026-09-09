#!/usr/bin/env python3
"""
generate_test_data.py

Generate a small, fully synthetic stand-in for the FinnGen detailed
longitudinal register file, for use with build_intervals.py --test.

All FINNGENIDs, dates, ages, and codes below are made up (hand-picked) --
nothing here is derived from or resembles real data. The file mirrors the
real schema (same 11 columns, same SOURCE/CODE/INDEX conventions) so it
exercises build_intervals.py's parsing and grouping logic faithfully. Rows
are written sorted by (FINNGENID, EVENT_AGE), same as a --debug/--test trace
displays them.

Covers, by synthetic patient:
  TEST0001 -- two well-separated, non-overlapping INPAT visits
  TEST0002 -- two INPAT visits that overlap (same-day transfer, distinct
              INDEX values) and must merge into one episode
  TEST0003 -- one INPAT visit split across several rows (multiple diagnosis
              codes sharing one INDEX -- main dx, side dx, external cause)
  TEST0004 -- one zero-duration INPAT visit (CODE4 = 0), plus a later,
              disjoint visit with a missing CODE4 (NA duration)
  TEST0005 -- non-hospital rows only (PURCH, OUTPAT), to confirm the
              INPAT-only filter drops everything for this patient

Usage:
  python3 generate_test_data.py
"""

import gzip
from pathlib import Path

OUT_PATH = Path(__file__).parent / "test_data" / "detailed_longitudinal_test.txt.gz"

COLUMNS = [
    "FINNGENID", "SOURCE", "EVENT_AGE", "APPROX_EVENT_DAY",
    "CODE1", "CODE2", "CODE3", "CODE4", "ICDVER", "CATEGORY", "INDEX",
]

ROWS = [
    # TEST0001: two separate, non-overlapping INPAT visits
    ("TEST0001", "INPAT", "40.100", "2010-02-01", "A001", "NA", "NA", "3", "10", "0", "1"),
    ("TEST0001", "INPAT", "45.500", "2015-07-10", "A002", "NA", "NA", "5", "10", "0", "2"),

    # TEST0002: two INPAT visits on consecutive days (different INDEX) whose
    # intervals overlap once duration is applied -- must merge into one episode
    ("TEST0002", "INPAT", "30.000", "2005-01-01", "B001", "NA", "NA", "2", "10", "0", "10"),
    ("TEST0002", "INPAT", "30.005", "2005-01-03", "B002", "NA", "NA", "4", "10", "0", "11"),
    # a later, clearly disjoint visit
    ("TEST0002", "INPAT", "50.000", "2025-01-01", "B003", "NA", "NA", "1", "10", "0", "12"),

    # TEST0003: one visit, several rows sharing INDEX (main dx, side dx, external cause)
    ("TEST0003", "INPAT", "60.200", "2020-05-05", "C001", "NA", "NA", "6", "10", "0", "20"),
    ("TEST0003", "INPAT", "60.200", "2020-05-05", "C002", "NA", "NA", "6", "10", "1", "20"),
    ("TEST0003", "INPAT", "60.200", "2020-05-05", "X01", "NA", "NA", "6", "10", "EX1", "20"),

    # TEST0004: zero-duration visit, plus a separate visit with missing duration
    ("TEST0004", "INPAT", "25.000", "2000-03-01", "D001", "NA", "NA", "0", "10", "0", "30"),
    ("TEST0004", "INPAT", "70.000", "2045-03-01", "D002", "NA", "NA", "NA", "10", "0", "31"),

    # TEST0005: no INPAT rows at all -- should not appear in the output
    ("TEST0005", "PURCH", "20.000", "1995-06-01", "N02BE01", "NA", "123456", "1", "NA", "NA", "40"),
    ("TEST0005", "OUTPAT", "21.000", "1996-06-01", "E001", "NA", "NA", "NA", "10", "0", "41"),
]


def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fid_idx = COLUMNS.index("FINNGENID")
    age_idx = COLUMNS.index("EVENT_AGE")
    rows = sorted(ROWS, key=lambda row: (row[fid_idx], float(row[age_idx])))

    with gzip.open(OUT_PATH, "wt") as f:
        f.write("\t".join(COLUMNS) + "\n")
        for row in rows:
            f.write("\t".join(row) + "\n")

    print(f"wrote {len(rows)} rows -> {OUT_PATH}")


if __name__ == "__main__":
    main()
