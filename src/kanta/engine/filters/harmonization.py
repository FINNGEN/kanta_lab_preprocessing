import re

import numpy as np
import pandas as pd

from kanta import config
from kanta.engine import reference_data
from kanta.engine.errors import UnitSink
from kanta.engine.filters.fix_unit import normalize_unit_candidate


def approve_status(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Zero out OMOP_ID for non-APPROVED rows in the cached Usagi mapping table."""
    usagi_mapping = reference_data.get_usagi_mapping()
    not_approved = usagi_mapping["harmonization_omop::MAPPING_STATUS"] != "APPROVED"
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


_VALUE_TRAILING_RE = re.compile(r"^(-?\d+(?:\.\d+)?)\s*(.*)$")


def extract_measurement(df: pd.DataFrame, unit_changes: UnitSink, verbose: bool = False) -> pd.DataFrame:
    """Populate MEASUREMENT_VALUE from MEASUREMENT_FREE_TEXT where it's currently missing.

    MEASUREMENT_FREE_TEXT   -> MEASUREMENT_VALUE  MEASUREMENT_UNIT  IS_VALUE_EXTRACTED  IS_UNIT_EXTRACTED
    "4.9"                   -> 4.9                NA                1                   0
    "tulos: 4.82 e12/l"     -> 4.82               e12/l             1                   1
    "4.1 hyytynyt"          -> unchanged          NA                0                   0

    The trailing text is cleaned via normalize_unit_candidate() and only kept as a unit if
    it's Usagi-recognized (get_usagi_units()).
    """
    ft_col = "MEASUREMENT_FREE_TEXT"
    value_col = "MEASUREMENT_VALUE"
    unit_col = "MEASUREMENT_UNIT"

    text = df[ft_col].astype(str).str.lower().str.strip()
    for pattern in config.FREE_TEXT_RESULT_STRINGS:
        text = text.str.replace(rf"^\s*{pattern}\s*", "", regex=True)
    for pattern, replacement in config.FREE_TEXT_MEASUREMENT_REPLACEMENTS:
        text = text.str.replace(pattern, replacement, regex=True)

    parts = text.str.extract(_VALUE_TRAILING_RE)
    parts.columns = ["num", "trailing"]
    numeric = pd.to_numeric(parts["num"], errors="coerce")
    trailing_candidate = normalize_unit_candidate(parts["trailing"].fillna("NA"))

    usagi_units = reference_data.get_usagi_units()
    is_bare_number = numeric.notna() & (trailing_candidate == "NA")
    is_number_plus_unit = numeric.notna() & trailing_candidate.isin(usagi_units)
    can_extract = is_bare_number | is_number_plus_unit

    is_missing = df[value_col] == "NA"
    is_extracted = is_missing & can_extract

    df["IS_VALUE_EXTRACTED"] = np.where(is_extracted, "1", "0")
    df.loc[is_extracted, value_col] = numeric.loc[is_extracted].astype(str)

    inject_idx = is_extracted & is_number_plus_unit & (df[unit_col] == "NA")
    injected_rows = df.loc[inject_idx]
    old_unit_with_source_text = "NA/" + df.loc[inject_idx, ft_col].astype(str)
    unit_changes.add(
        injected_rows,
        err_name="EXTRACTION",
        old_unit=old_unit_with_source_text,
        new_unit=trailing_candidate.loc[inject_idx],
    )
    df["IS_UNIT_EXTRACTED"] = np.where(inject_idx, "1", "0")
    df.loc[inject_idx, unit_col] = trailing_candidate.loc[inject_idx]

    if verbose:
        print(f"[harmonization] {is_extracted.sum()}/{len(df)} MEASUREMENT_VALUE extracted from free text")
        print(f"[harmonization] {inject_idx.sum()} with MEASUREMENT_UNIT recovered from free text")

    return df


def add_qc_note(df: pd.DataFrame, idx: pd.Index, notes: pd.Series) -> None:
    """Append notes to QC_NOTES at idx, creating the column (as "NA") if it doesn't exist yet.

    Concatenates with ";" when a row already has a note there, so filters running at different
    stages can each layer their own note onto the same row without clobbering earlier ones.
    """
    if "QC_NOTES" not in df.columns:
        df["QC_NOTES"] = "NA"

    if idx.empty:
        return

    existing = df.loc[idx, "QC_NOTES"]
    is_first_note = existing == "NA"
    df.loc[idx, "QC_NOTES"] = np.where(is_first_note, notes, existing + ";" + notes)


def inject_missing_unit(df: pd.DataFrame, unit_changes: UnitSink, verbose: bool = False) -> pd.DataFrame:
    """Assign MEASUREMENT_UNIT to rows with a value but no unit.

    The unit is guessed by comparing a test's no-unit values against the value distributions
    of other units seen for that same test name. See scripts/injection/README.md.
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

    unit_changes.add(
        df.loc[inject_idx],
        err_name="INJECTION",
        old_unit=df.loc[inject_idx, "MEASUREMENT_UNIT"],
        new_unit=assigned_unit.loc[inject_idx],
    )
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


def fix_unit_based_on_abbreviation(df: pd.DataFrame, unit_changes: UnitSink, verbose: bool = False) -> pd.DataFrame:
    """Secondary injection: harmonize missing/incorrect/incomplete units still present in the data.

    Looked up from reference_data.get_omop_injection_table() by (TEST_NAME_ABBREVIATION,
    MEASUREMENT_UNIT). Examples:
    - b-hkr "osuus" -> "ratio" (not a formal unit)
    - du-prot "g" -> "g/24h" (incomplete)
    - p-krea "mmol/l" -> "umol/l" (incorrect)
    - -l-ind "NA" -> "index" (missing)
    """
    table = reference_data.get_omop_injection_table()
    keys = pd.MultiIndex.from_arrays([df["TEST_NAME_ABBREVIATION"], df["MEASUREMENT_UNIT"]])
    matched = table.reindex(keys).set_axis(df.index)

    fix_idx = matched.index[matched["source_unit_clean_fix"].notna()]

    unit_changes.add(
        df.loc[fix_idx],
        err_name="ABBREVIATION_FIX",
        old_unit=df.loc[fix_idx, "MEASUREMENT_UNIT"],
        new_unit=matched.loc[fix_idx, "source_unit_clean_fix"],
    )
    df.loc[fix_idx, "MEASUREMENT_UNIT"] = matched.loc[fix_idx, "source_unit_clean_fix"]

    if verbose:
        print(f"[harmonization] fix_unit_based_on_abbreviation: {len(fix_idx)}/{len(df)} units corrected")

    return df


def omop_mapping(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Assign harmonization_omop::OMOP_ID / OMOP_QUANTITY by looking up each row's
    (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT) in the Usagi mapping table.

    No match -> "NA". Matched but not APPROVED -> "0" (via approve_status()).
    """
    join_cols = ["TEST_NAME_ABBREVIATION", "MEASUREMENT_UNIT"]
    out_cols = ["harmonization_omop::OMOP_ID", "harmonization_omop::OMOP_QUANTITY"]

    mapping = reference_data.get_usagi_mapping().drop_duplicates(subset=join_cols, keep="first")
    lookup = mapping.set_index(join_cols)[out_cols]

    keys = pd.MultiIndex.from_arrays([df["TEST_NAME_ABBREVIATION"], df["MEASUREMENT_UNIT"]])
    matched = lookup.reindex(keys).set_axis(df.index).fillna("NA")
    df[out_cols] = matched

    if verbose:
        n_matched = (matched["harmonization_omop::OMOP_ID"] != "NA").sum()
        print(f"[harmonization] omop_mapping: {n_matched}/{len(df)} rows matched to an OMOP_ID")

    return df


def unit_harmonization(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Convert MEASUREMENT_VALUE into harmonization_omop::MEASUREMENT_VALUE via
    reference_data.get_conversion_table(). No match or non-numeric value -> "NA".

    OMOP_ID   QUANTITY           UNIT    VALUE  ->  MEASUREMENT_VALUE
    3020564   Substance Conc.    mmol/l  5      ->  5000      (factor 1000)
    3004410   Mass fraction      %       10     ->  86.3      ("10.93*X-23.50")
    9999999   (no match)         g/l     5      ->  NA
    """
    join_cols = ["harmonization_omop::OMOP_ID", "harmonization_omop::OMOP_QUANTITY", "MEASUREMENT_UNIT"]
    table = reference_data.get_conversion_table()

    keys = pd.MultiIndex.from_arrays([df[col] for col in join_cols])
    matched = table.reindex(keys).set_axis(df.index)

    df["harmonization_omop::MEASUREMENT_UNIT"] = matched["harmonization_omop::MEASUREMENT_UNIT"].fillna("NA")
    factor = matched["harmonization_omop::CONVERSION_FACTOR"]
    df["harmonization_omop::CONVERSION_FACTOR"] = factor.fillna("NA")

    # MEASUREMENT_VALUE is Arrow-backed (string[pyarrow]) from the parquet read, so a
    # non-numeric string like "NA" coerces to a NaN *value* that pyarrow's own validity
    # bitmap still marks as "not null" -- .notna() on the raw pd.to_numeric() result would
    # then wrongly report True for every unit-less row. Casting to plain float64 first
    # switches to standard IEEE NaN semantics, where .notna() correctly excludes it.
    value = pd.to_numeric(df["MEASUREMENT_VALUE"], errors="coerce").astype("float64")
    has_conversion = factor.notna() & value.notna()
    is_formula = factor.astype(str).str.contains("X", na=False)

    converted = pd.Series(np.nan, index=df.index, dtype=float)

    formula_idx = df.index[has_conversion & is_formula]
    converted.loc[formula_idx] = [
        round(eval(f.replace(",", ".").replace("X", str(v)), {"__builtins__": {}}), 2)
        for f, v in zip(factor.loc[formula_idx], value.loc[formula_idx])
    ]

    numeric_idx = df.index[has_conversion & ~is_formula]
    converted.loc[numeric_idx] = (
        pd.to_numeric(factor.loc[numeric_idx], errors="coerce") * value.loc[numeric_idx]
    )

    df["harmonization_omop::MEASUREMENT_VALUE"] = np.where(converted.notna(), converted.astype(str), "NA")

    if verbose:
        print(
            f"[harmonization] unit_harmonization: {int(converted.notna().sum())}/{len(df)} rows converted "
            f"({len(formula_idx)} formula, {len(numeric_idx)} numeric)"
        )
    return df


def run(df: pd.DataFrame, unit_changes: UnitSink, verbose: bool = False) -> pd.DataFrame:
    df = (
        df.pipe(approve_status, verbose)
        .pipe(extract_measurement, unit_changes, verbose)
        .pipe(inject_missing_unit, unit_changes, verbose)
        .pipe(fix_unit_based_on_abbreviation, unit_changes, verbose)
        .pipe(omop_mapping, verbose)
        .pipe(unit_harmonization, verbose)
        .pipe(check_usagi_unit, verbose)
    )
    return df
