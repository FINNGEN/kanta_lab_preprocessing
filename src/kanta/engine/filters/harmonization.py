import numpy as np
import pandas as pd

from kanta import config
from kanta.engine import reference_data


def approve_status(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Zero out OMOP_ID for non-APPROVED rows in the (cached) Usagi mapping table.

    Doesn't modify df: mirrors the old finngen_qc step, which only corrected the
    reference table itself, not the data being processed. Since get_usagi_mapping()
    is cached, this mutates the same table object on every call, so it's idempotent.
    """
    usagi_mapping = reference_data.get_usagi_mapping()
    not_approved = usagi_mapping["harmonization_omop::mappingStatus"] != "APPROVED"
    usagi_mapping.loc[not_approved, "harmonization_omop::OMOP_ID"] = "0"

    if verbose:
        print(
            f"[harmonization] usagi_mapping: {not_approved.sum()}/{len(usagi_mapping)} rows "
            "not APPROVED (OMOP_ID zeroed)"
        )
    return df


def check_usagi_unit(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Populate harmonization_omop::IS_UNIT_VALID: whether MEASUREMENT_UNIT is Usagi-approved."""
    is_valid = df["MEASUREMENT_UNIT"].isin(reference_data.get_usagi_units())
    df["harmonization_omop::IS_UNIT_VALID"] = np.where(is_valid, "1", "0")
    if verbose:
        counts = df["harmonization_omop::IS_UNIT_VALID"].value_counts().to_dict()
        print(f"[harmonization] IS_UNIT_VALID counts: {counts}")

    return df


def extract_measurement(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Populate MEASUREMENT_VALUE from MEASUREMENT_FREE_TEXT where it's currently missing.

    Cleans the free text (lowercase, strip whitespace), strips the row's own
    MEASUREMENT_UNIT from it when that unit is Usagi-approved (reference_data.get_usagi_units())
    so numbers glued directly to their unit (e.g. "5.2mmol/l") can still be parsed, then
    applies the free-text cleanup rules and parses what's left as a number.
    """
    ft_col = "MEASUREMENT_FREE_TEXT"
    value_col = "MEASUREMENT_VALUE"

    cleaned = df[ft_col].astype(str).str.lower().str.strip().str.replace(r"\s", "", regex=True)

    is_valid_unit = df["MEASUREMENT_UNIT"].isin(reference_data.get_usagi_units())
    cleaned.loc[is_valid_unit] = [
        text.replace(unit, "")
        for text, unit in zip(cleaned.loc[is_valid_unit], df.loc[is_valid_unit, "MEASUREMENT_UNIT"])
    ]

    for pattern in config.FREE_TEXT_RESULT_STRINGS:
        cleaned = cleaned.str.replace(rf"^\s*{pattern}\s*", "", regex=True)
    for pattern, replacement in config.FREE_TEXT_MEASUREMENT_REPLACEMENTS:
        cleaned = cleaned.str.replace(pattern, replacement, regex=True)

    extracted = pd.to_numeric(cleaned, errors="coerce")

    is_missing = df[value_col] == "NA"
    is_extracted = is_missing & extracted.notna()

    df["IS_VALUE_EXTRACTED"] = np.where(is_extracted, "1", "0")
    df.loc[is_extracted, value_col] = extracted.loc[is_extracted].astype(str)

    if verbose:
        print(f"[harmonization] {is_extracted.sum()}/{len(df)} MEASUREMENT_VALUE extracted from free text")

    return df


def add_qc_note(df: pd.DataFrame, idx: pd.Index, notes: pd.Series) -> None:
    """Append notes to QC_NOTES at idx, creating the column (as "NA") if it doesn't exist yet.

    Concatenates with ";" when a row already has a note there, so filters running at different
    stages can each layer their own note onto the same row without clobbering earlier ones.
    """
    if "QC_NOTES" not in df.columns:
        df["QC_NOTES"] = "NA"

    existing = df.loc[idx, "QC_NOTES"]
    is_first_note = existing == "NA"
    df.loc[idx, "QC_NOTES"] = np.where(is_first_note, notes, existing + ";" + notes)


def inject_missing_unit(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Assign MEASUREMENT_UNIT to rows with a value but no unit, from scripts/injection's results.

    Snapshots the pre-injection MEASUREMENT_UNIT into cleaned-pre-fix::MEASUREMENT_UNIT first
    (same bookkeeping convention as the old finngen_qc pipeline's dump_unit_before_fix), so an
    injected row can be spotted afterward by comparing the two columns. One lookup
    (reference_data.get_injection_table()) handles both simple (single-unit) and bimodal-split
    tests: simple tests have CUTOFF=-inf, so comparing MEASUREMENT_VALUE against CUTOFF always
    resolves to HIGH_UNIT for them. Whenever a split has any mode overlap at all, its BC/overlap
    values are copied into QC_NOTES for transparency.
    """
    df["cleaned-pre-fix::MEASUREMENT_UNIT"] = df["MEASUREMENT_UNIT"]

    is_eligible = (df["MEASUREMENT_VALUE"] != "NA") & (df["MEASUREMENT_UNIT"] == "NA")
    table = reference_data.get_injection_table()

    is_candidate = is_eligible & df["TEST_NAME_ABBREVIATION"].isin(table.index)
    test_name = df.loc[is_candidate, "TEST_NAME_ABBREVIATION"]
    value = pd.to_numeric(df.loc[is_candidate, "MEASUREMENT_VALUE"], errors="coerce")

    cutoff = test_name.map(table["CUTOFF"])
    is_low_side = value < cutoff
    low_unit = test_name.map(table["LOW_UNIT"])
    high_unit = test_name.map(table["HIGH_UNIT"])
    assigned_unit = low_unit.where(is_low_side, high_unit)

    has_unit = assigned_unit.notna()
    inject_idx = has_unit[has_unit].index
    df.loc[inject_idx, "MEASUREMENT_UNIT"] = assigned_unit.loc[inject_idx]

    bc = test_name.map(table["BIMODAL_BC"])
    overlap = test_name.map(table["BIMODAL_OVERLAP"])
    needs_note = has_unit & (overlap > 0)
    note_idx = needs_note[needs_note].index
    notes = pd.Series(
        [
            f"bimodal_split (BC={b:.3g}, overlap={o:.3g}%)"
            for b, o in zip(bc.loc[note_idx], overlap.loc[note_idx])
        ],
        index=note_idx,
    )
    add_qc_note(df, note_idx, notes)

    if verbose:
        print(f"[harmonization] unit injected: {int(has_unit.sum())} ({int(needs_note.sum())} with overlap noted)")
    return df


def run(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    df = (
        df.pipe(approve_status, verbose)
        .pipe(extract_measurement, verbose)
        .pipe(inject_missing_unit, verbose)
        .pipe(check_usagi_unit, verbose)
    )
    return df
