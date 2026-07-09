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


def inject_missing_unit(
    df: pd.DataFrame,
    verbose: bool = False,
    bc_threshold: float = config.BIMODAL_BC_THRESHOLD_DEFAULT,
    overlap_threshold: float = config.BIMODAL_OVERLAP_THRESHOLD_DEFAULT,
) -> pd.DataFrame:
    """Assign MEASUREMENT_UNIT to rows with a value but no unit, from scripts/injection's results.

    Snapshots the pre-injection MEASUREMENT_UNIT into cleaned-pre-fix::MEASUREMENT_UNIT first
    (same bookkeeping convention as the old finngen_qc pipeline's dump_unit_before_fix), so an
    injected row can be spotted afterward by comparing the two columns. Simple (single-unit per
    test) assignments are applied first, then bimodal-split tests (comparing MEASUREMENT_VALUE
    against the split's cutoff) — each side gated independently by whether it passed statistical
    validation. Splits with a weak bimodality coefficient or high mode overlap are flagged in
    QC_NOTES, since values near the cutoff can't be confidently assigned in those cases.
    """
    df["cleaned-pre-fix::MEASUREMENT_UNIT"] = df["MEASUREMENT_UNIT"]
    df["QC_NOTES"] = "NA"

    is_eligible = (df["MEASUREMENT_VALUE"] != "NA") & (df["MEASUREMENT_UNIT"] == "NA")
    injection_results = reference_data.get_injection_results()

    # Simple tests: one validated unit per TEST_NAME, no value-based splitting.
    simple = injection_results[
        (injection_results["SUB_DIST"] == "all") & (injection_results["OUTCOME"] == "PASS")
    ]
    simple_units = dict(zip(simple["TEST_NAME"], simple["UNIT"]))
    is_simple = is_eligible & df["TEST_NAME_ABBREVIATION"].isin(simple_units)
    df.loc[is_simple, "MEASUREMENT_UNIT"] = df.loc[is_simple, "TEST_NAME_ABBREVIATION"].map(simple_units)

    # Bimodal-split tests: low/high candidate unit depends on which side of CUTOFF the value
    # falls on. low_all keeps FAIL rows too, since the CUTOFF is only ever recorded on the low
    # row and is needed to place a value even when that side's own unit assignment is invalid.
    split = injection_results[injection_results["SUB_DIST"] != "all"]
    low_all = split[split["SUB_DIST"] == "low"].set_index("TEST_NAME")
    low_pass = low_all[low_all["OUTCOME"] == "PASS"]
    high_pass = split[(split["SUB_DIST"] == "high") & (split["OUTCOME"] == "PASS")].set_index("TEST_NAME")

    is_split = is_eligible & ~is_simple & df["TEST_NAME_ABBREVIATION"].isin(low_all.index)
    test_name = df.loc[is_split, "TEST_NAME_ABBREVIATION"]
    value = pd.to_numeric(df.loc[is_split, "MEASUREMENT_VALUE"], errors="coerce")

    cutoff = test_name.map(low_all["CUTOFF"])
    is_low_side = value < cutoff
    low_unit = test_name.map(low_pass["UNIT"])
    high_unit = test_name.map(high_pass["UNIT"])
    assigned_unit = low_unit.where(is_low_side, high_unit)

    has_unit = assigned_unit.notna()
    inject_idx = has_unit[has_unit].index
    df.loc[inject_idx, "MEASUREMENT_UNIT"] = assigned_unit.loc[inject_idx]

    bc = test_name.map(low_all["BIMODAL_BC"])
    overlap = test_name.map(low_all["BIMODAL_OVERLAP"])
    needs_flag = has_unit & ((bc < bc_threshold) | (overlap > overlap_threshold))
    flag_idx = needs_flag[needs_flag].index
    df.loc[flag_idx, "QC_NOTES"] = [
        f"uncertain_bimodal_split (BC={b:.2f}, overlap={o:.1f}%)"
        for b, o in zip(bc.loc[flag_idx], overlap.loc[flag_idx])
    ]

    if verbose:
        print(
            f"[harmonization] unit injected: {is_simple.sum()} simple, "
            f"{int(has_unit.sum())} bimodal-split ({int(needs_flag.sum())} flagged uncertain)"
        )
    return df


def run(
    df: pd.DataFrame,
    verbose: bool = False,
    bc_threshold: float = config.BIMODAL_BC_THRESHOLD_DEFAULT,
    overlap_threshold: float = config.BIMODAL_OVERLAP_THRESHOLD_DEFAULT,
) -> pd.DataFrame:
    df = (
        df.pipe(approve_status, verbose)
        .pipe(extract_measurement, verbose)
        .pipe(inject_missing_unit, verbose, bc_threshold, overlap_threshold)
        .pipe(check_usagi_unit, verbose)
    )
    return df
