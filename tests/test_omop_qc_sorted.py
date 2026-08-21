import pandas as pd

from kanta import config


def test_omop_qc_sorted_by_omop_id_then_threshold():
    """omop_qc.tsv must stay sorted: rows with a real THRESHOLD first (by OMOP_ID, then
    THRESHOLD, both numeric), then every placeholder row (THRESHOLD="NA") as one block at the
    bottom (by OMOP_ID) -- so the active rules are easy to scan as their own block, and a
    multi-rule OMOP_ID reads as one obvious group within it.

    Execution itself doesn't depend on this order -- reference_data.get_compiled_omop_qc()
    re-derives its own ordering at load time -- this is purely enforcing legibility on a
    hand-edited, ever-growing rules file. Re-sort with scripts/qc_scripts/sort_omop_qc.py.
    """
    df = pd.read_csv(config.OMOP_QC_FILE, sep="\t", dtype=str)

    threshold = pd.to_numeric(df["THRESHOLD"], errors="coerce")
    key = pd.DataFrame({
        "is_placeholder": threshold.isna(),
        "omop_id": pd.to_numeric(df["harmonization_omop::OMOP_ID"], errors="coerce"),
        "threshold": threshold,
    })
    expected_order = key.sort_values(["is_placeholder", "omop_id", "threshold"], kind="stable").index

    out_of_order = key.index[key.index != expected_order]
    assert len(out_of_order) == 0, (
        f"omop_qc.tsv is not sorted (active rules first by OMOP_ID/THRESHOLD, then all "
        f"placeholder rows by OMOP_ID) -- {len(out_of_order)} row(s) out of place, first "
        f"offender at row {out_of_order[0]} (0-indexed, TSV line {out_of_order[0] + 2}): "
        f"OMOP_ID={df.loc[out_of_order[0], 'harmonization_omop::OMOP_ID']}. "
        f"Run scripts/qc_scripts/sort_omop_qc.py and commit the result."
    )
