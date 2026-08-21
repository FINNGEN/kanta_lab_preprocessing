"""Re-sort src/kanta/engine/data/omop_qc.tsv: rows with a real THRESHOLD first (ordered by
OMOP_ID, then THRESHOLD, both numeric), then every placeholder row (THRESHOLD="NA") as one
block at the bottom (ordered by OMOP_ID). Run this after hand-editing the file, before
committing -- tests/test_omop_qc_sorted.py enforces the same ordering in CI, so an out-of-order
commit fails review rather than just looking untidy.

Sorting is purely for human legibility: reference_data.get_compiled_omop_qc() re-derives its own
ordering at load time and doesn't depend on the file's row order for correctness.

Usage
-----
  python3 scripts/qc_scripts/sort_omop_qc.py
"""

from pathlib import Path

import pandas as pd

_QC_FILE = Path(__file__).resolve().parent.parent.parent / "src/kanta/engine/data/omop_qc.tsv"


def main():
    # keep_default_na=False so the file's own literal "NA" text round-trips unchanged --
    # otherwise pandas reads "NA" as a real NaN and writes it back out as an empty string.
    df = pd.read_csv(_QC_FILE, sep="\t", dtype=str, keep_default_na=False)

    threshold = pd.to_numeric(df["THRESHOLD"], errors="coerce")
    key = pd.DataFrame({
        "is_placeholder": threshold.isna(),
        "omop_id": pd.to_numeric(df["harmonization_omop::OMOP_ID"], errors="coerce"),
        "threshold": threshold,
    })
    order = key.sort_values(["is_placeholder", "omop_id", "threshold"], kind="stable").index

    if (order == df.index).all():
        print(f"{_QC_FILE} is already sorted -- nothing to do.")
        return

    df.loc[order].to_csv(_QC_FILE, sep="\t", index=False)
    print(f"Re-sorted {_QC_FILE} ({len(df)} rows).")


if __name__ == "__main__":
    main()
