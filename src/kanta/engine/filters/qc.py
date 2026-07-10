import operator

import pandas as pd

from kanta import config
from kanta.engine import reference_data
from kanta.engine.errors import ErrorSink
from kanta.engine.filters.harmonization import add_qc_note

_OPS = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "!=": operator.ne,
}


def _ensure_qc_pass(df: pd.DataFrame) -> None:
    """Create QC_PASS as "2" (unchecked) if it doesn't exist yet, mirroring add_qc_note's
    lazy-create-as-"NA" pattern for QC_NOTES.

    The old pipeline had a dedicated initialize_qc_columns step ahead of every QC filter;
    since that wasn't ported, each QC filter here is responsible for creating it itself.
    """
    if "QC_PASS" not in df.columns:
        df["QC_PASS"] = "2"


def check_dates_in_measurement(df: pd.DataFrame, errors: ErrorSink, verbose: bool = False) -> pd.DataFrame:
    """Drop rows whose MEASUREMENT_VALUE looks like a DDMMYY date typed into a value field
    (exactly 6 digits, day/month/year each in a plausible range).

    Checks MEASUREMENT_VALUE (the raw/free-text-extracted value), not the harmonized one: a
    date-shaped data-entry error is a raw-value problem, and unit-conversion arithmetic would
    destroy the 6-digit pattern anyway.
    """
    col = "MEASUREMENT_VALUE"

    cleaned = df[col].str.replace(r"\.0$", "", regex=True).str.strip()
    parts = cleaned.str.extract(r"^(?P<day>\d{2})(?P<month>\d{2})(?P<year>\d{2})$")
    day = pd.to_numeric(parts["day"], errors="coerce")
    month = pd.to_numeric(parts["month"], errors="coerce")
    year = pd.to_numeric(parts["year"], errors="coerce")

    is_date = (
        (day >= 1) & (day <= 31) & (month >= 1) & (month <= 12) & (year >= 0) & (year <= 99)
    ).fillna(False)

    bad_rows = df.loc[is_date]
    errors.add(
        bad_rows,
        err_name="DATE_IN_MEASUREMENT",
        err_value=(
            bad_rows["TEST_NAME_ABBREVIATION"]
            + "::"
            + bad_rows["harmonization_omop::MEASUREMENT_VALUE"]
            + "::"
            + bad_rows["MEASUREMENT_FREE_TEXT"]
            + "::"
            + bad_rows[col]
        ),
    )

    if verbose:
        print(f"[qc] check_dates_in_measurement: {int(is_date.sum())}/{len(df)} rows dropped")
    return df.loc[~is_date]


def flag_omop_qc(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Flag QC_PASS="0" for rows whose harmonization_omop::MEASUREMENT_VALUE fails a per-OMOP_ID
    threshold rule (reference_data.get_omop_qc()); any OMOP_ID appearing in that table at all is
    first marked QC_PASS="1" (checked), then downgraded to "0" per failing rule. A single OMOP_ID
    can carry several rules (e.g. both a too-high and a too-low check); some rows are
    placeholder "register this OMOP_ID as checked" entries with no SIDE/THRESHOLD, skipped here.

    Uses the harmonized value only (same reasoning as harmonization.impute_outcome): THRESHOLD is
    keyed only by OMOP_ID, implicitly assuming the harmonized/target unit, so comparing an
    unconverted value against it would be unit-inconsistent.
    """
    _ensure_qc_pass(df)

    rules = reference_data.get_omop_qc(verbose=verbose)
    is_registered = df["harmonization_omop::OMOP_ID"].isin(rules["harmonization_omop::OMOP_ID"])
    df.loc[is_registered, "QC_PASS"] = "1"

    value = pd.to_numeric(df["harmonization_omop::MEASUREMENT_VALUE"], errors="coerce")
    n_failed = 0
    for _, rule in rules.iterrows():
        side = rule["SIDE"]
        if pd.isna(side) or side not in _OPS or pd.isna(rule["THRESHOLD"]):
            continue

        omop_mask = df["harmonization_omop::OMOP_ID"] == rule["harmonization_omop::OMOP_ID"]
        fail_mask = omop_mask & _OPS[side](value, float(rule["THRESHOLD"]))
        if not fail_mask.any():
            continue

        fail_idx = df.index[fail_mask]
        df.loc[fail_idx, "QC_PASS"] = "0"
        add_qc_note(df, fail_idx, pd.Series(rule["QC_NOTES"], index=fail_idx))
        n_failed += len(fail_idx)

    if verbose:
        print(
            f"[qc] flag_omop_qc: {int(is_registered.sum())}/{len(df)} rows registered, "
            f"{n_failed} rule failures"
        )
    return df


def flag_outcome_mismatch(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Flag QC_PASS="0" + QC_NOTES += "OUTCOME_EXTRACT_CONFLICT" where (TEST_OUTCOME,
    extracted::IS_POS) lands on a config.OUTCOME_MISMATCH pair (e.g. a categorical "Normal"
    outcome alongside a text-extracted positive result).
    """
    _ensure_qc_pass(df)

    keys = pd.MultiIndex.from_frame(df[["TEST_OUTCOME", "extracted::IS_POS"]])
    fail_mask = keys.isin(config.OUTCOME_MISMATCH)

    if fail_mask.any():
        fail_idx = df.index[fail_mask]
        df.loc[fail_idx, "QC_PASS"] = "0"
        add_qc_note(df, fail_idx, pd.Series("OUTCOME_EXTRACT_CONFLICT", index=fail_idx))

    if verbose:
        print(f"[qc] flag_outcome_mismatch: {int(fail_mask.sum())}/{len(df)} rows flagged")
    return df


def run(df: pd.DataFrame, errors: ErrorSink, verbose: bool = False) -> pd.DataFrame:
    df = (
        df.pipe(check_dates_in_measurement, errors, verbose)
        .pipe(flag_omop_qc, verbose)
        .pipe(flag_outcome_mismatch, verbose)
    )
    return df
