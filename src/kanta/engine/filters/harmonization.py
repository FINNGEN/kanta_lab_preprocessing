import re

import numpy as np
import pandas as pd

from kanta import config
from kanta.engine import reference_data
from kanta.engine.errors import UnitSink
from kanta.engine.filters.fix_unit import normalize_unit_candidate


def approve_status(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Zero out OMOP_ID for non-APPROVED rows in the (cached) Usagi mapping table.

    Doesn't modify df: mirrors the old finngen_qc step, which only corrected the
    reference table itself, not the data being processed. Since get_usagi_mapping()
    is cached, this mutates the same table object on every call, so it's idempotent.
    """
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

    Cleans the free text (lowercase, strip whitespace, strip known result-prefixes, comma ->
    period) then splits it into a leading number and whatever trails it (_VALUE_TRAILING_RE).
    An empty trailing part means a bare number (e.g. "4.9") — extract directly. A non-empty
    trailing part is a unit *candidate*: it's run through normalize_unit_candidate() (the exact
    same cleaning fix_unit.run() applies to the structured MEASUREMENT_UNIT column, so a unit
    embedded in free text is held to the identical standard as one coming from the source
    field) and checked against reference_data.get_usagi_units(). Only if that candidate turns
    out to be a real, recognized unit does extraction succeed at all (e.g. "4.82 e12/l" ->
    trailing "e12/l" is valid -> extract 4.82; "peruttu" doesn't even reach this far, since it
    doesn't start with a number; "4.1 hyytynyt" starts with a number but "hyytynyt" isn't a
    real unit, so nothing is extracted) — a number attached to text we can't identify as a
    unit isn't reliably a measurement, so it's safer to skip it than to guess. Whenever the
    trailing part does validate, it's also written into MEASUREMENT_UNIT wherever that's
    currently missing ("NA") — never overwriting a unit the row already has. Every such
    injection is logged to unit_changes (err_name="EXTRACTION"), same <prefix>_unit.parquet sink
    fix_unit.py's regex-driven changes go to, so it's traceable alongside them. OLD_UNIT is
    recorded as "NA/<original free text>" (e.g. "NA/4.82 E12/L") rather than a bare "NA", so the
    free-text source of the extraction is visible directly in the sink without needing to
    cross-reference MEASUREMENT_FREE_TEXT separately. IS_UNIT_EXTRACTED ("1"/"0") flags exactly
    these rows on df itself too — in principle this is reconstructable later from source::
    MEASUREMENT_UNIT vs cleaned-pre-fix::MEASUREMENT_UNIT (fix_unit.py never turns a genuinely
    blank unit into a real one, so a diff there would mean it came from here), but that inference
    would silently rely on an undocumented invariant of fix_unit.py's behavior — recording it
    explicitly, where the answer is known for certain, is more robust.
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


def omop_mapping(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Assign harmonization_omop::OMOP_ID / harmonization_omop::OMOP_QUANTITY by looking up each
    row's (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT) pair in the Usagi mapping table.

    approve_status() already zeroed OMOP_ID to "0" for non-APPROVED rows within the (cached)
    mapping table, so that's reflected here for free, with no separate APPROVED filter needed.
    Rows whose (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT) pair has no match at all in the mapping
    table get "NA" (distinct from the "0" approve_status assigns to a matched-but-not-approved
    pair). The mapping table is deduplicated on the join keys (keep first) so a row in df can
    never fan out into more than one output row.
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
    """Convert MEASUREMENT_VALUE into harmonization_omop::MEASUREMENT_VALUE using the per-
    (OMOP_ID, OMOP_QUANTITY, MEASUREMENT_UNIT) factor from reference_data.get_conversion_table().

    Writes the converted value into a *new* column instead of overwriting MEASUREMENT_VALUE, so
    the original value stays available to compare conversion success/failure against. The target
    unit (harmonization_omop::MEASUREMENT_UNIT) and the factor used
    (harmonization_omop::CONVERSION_FACTOR) are copied onto df alongside it for traceability.
    Rows with no OMOP_ID match, no conversion entry, or a non-numeric MEASUREMENT_VALUE get "NA"
    for all three.

    CONVERSION_FACTOR is either a plain numeric multiplier or a formula string containing "X"
    (e.g. "10.93*X-23.50", X = the row's MEASUREMENT_VALUE); eval() is run with builtins disabled
    since the formula only ever comes from our own static reference table, not user input.
    """
    join_cols = ["harmonization_omop::OMOP_ID", "harmonization_omop::OMOP_QUANTITY", "MEASUREMENT_UNIT"]
    table = reference_data.get_conversion_table()

    keys = pd.MultiIndex.from_arrays([df[col] for col in join_cols])
    matched = table.reindex(keys).set_axis(df.index)

    df["harmonization_omop::MEASUREMENT_UNIT"] = matched["harmonization_omop::MEASUREMENT_UNIT"].fillna("NA")
    factor = matched["harmonization_omop::CONVERSION_FACTOR"]
    df["harmonization_omop::CONVERSION_FACTOR"] = factor.fillna("NA")

    value = pd.to_numeric(df["MEASUREMENT_VALUE"], errors="coerce")
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
        .pipe(inject_missing_unit, verbose)
        .pipe(omop_mapping, verbose)
        .pipe(unit_harmonization, verbose)
        .pipe(check_usagi_unit, verbose)
    )
    return df
