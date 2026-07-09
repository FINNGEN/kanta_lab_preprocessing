import numpy as np
import pandas as pd

from kanta import config
from kanta.engine import reference_data
from kanta.engine.errors import AbbrSink, ErrorSink


def fix_date(df: pd.DataFrame, errors: ErrorSink) -> pd.DataFrame:
    """Combine APPROX_EVENT_DAY and TIME into APPROX_EVENT_DATETIME, dropping unparseable rows."""
    datetime_str = df["APPROX_EVENT_DAY"] + "T" + df["TIME"]
    is_bad_date = pd.to_datetime(
        datetime_str, format=config.DATE_TIME_FORMAT, errors="coerce"
    ).isna()

    bad_rows = df.loc[is_bad_date]
    errors.add(
        bad_rows,
        err_name="DATE",
        err_value=bad_rows["APPROX_EVENT_DAY"] + " " + bad_rows["TIME"],
    )

    df = df.loc[~is_bad_date].copy()
    df["APPROX_EVENT_DATETIME"] = datetime_str.loc[~is_bad_date]
    return df.drop(columns=["APPROX_EVENT_DAY", "TIME"])


def remove_spaces(df: pd.DataFrame) -> pd.DataFrame:
    """Strip and collapse whitespace in every string column except free-text ones.

    Missing values are filled with the literal "NA" string; specific NA-like keywords
    (e.g. "Puuttuu") are normalized separately in fix_na.
    """
    for col in df.columns:
        if col in config.COLUMNS_WITH_SPACES:
            continue
        if not pd.api.types.is_string_dtype(df[col]):
            continue
        df[col] = df[col].str.strip().str.replace(r"\s", "", regex=True).fillna("NA")
    return df


def fix_na(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize NA-like keyword tokens (e.g. "Puuttuu") to the literal "NA" string.

    Some columns use their own keyword list via NA_KEYWORDS_OVERRIDES, since a token like
    "-1" can be a legitimate value there instead of signaling missingness.
    """
    override_cols = set(config.NA_KEYWORDS_OVERRIDES)
    for col in override_cols:
        df[col] = df[col].replace(config.NA_KEYWORDS_OVERRIDES[col], "NA")

    other_cols = df.columns.difference(override_cols)
    df[other_cols] = df[other_cols].replace(config.NA_KEYWORDS, "NA")
    return df


def filter_measurement_status(df: pd.DataFrame, errors: ErrorSink) -> pd.DataFrame:
    """Drop rows whose MEASUREMENT_STATUS is one of the problematic codes."""
    is_problematic = df["MEASUREMENT_STATUS"].isin(config.PROBLEMATIC_MEASUREMENT_STATUS)

    bad_rows = df.loc[is_problematic]
    errors.add(
        bad_rows, err_name="measurement_status", err_value=bad_rows["MEASUREMENT_STATUS"]
    )

    return df.loc[~is_problematic]


def lab_id_source(df: pd.DataFrame) -> pd.DataFrame:
    """Derive TEST_ID and TEST_ID_IS_NATIONAL from the local/national lab id columns.

    Uses the national (THL) lab id (laboratoriotutkimusnimike) when present, falling back
    to the local lab code (paikallinentutkimusnimike_koodi) otherwise.
    """
    is_local = df["laboratoriotutkimusnimike"] == "NA"
    df["TEST_ID_IS_NATIONAL"] = np.where(is_local, "0", "1")
    df["TEST_ID"] = np.where(
        is_local, df["paikallinentutkimusnimike_koodi"], df["laboratoriotutkimusnimike"]
    )
    return df


def get_lab_abbrv(df: pd.DataFrame, errors: ErrorSink) -> pd.DataFrame:
    """Assign TEST_NAME_ABBREVIATION: keep the local name if TEST_ID is local, map via THL otherwise.

    National ids missing from the THL map are logged but kept (not dropped) — the mapping
    lookup itself falls back to the raw TEST_ID when unmapped.
    """
    col = "TEST_NAME_ABBREVIATION"
    df[col] = df[col].str.lower()

    thl_lab_map = reference_data.get_thl_lab_map()
    is_national = df["TEST_ID_IS_NATIONAL"] == "1"
    is_unmapped = ~df["TEST_ID"].isin(thl_lab_map)

    bad_rows = df.loc[is_national & is_unmapped]
    errors.add(bad_rows, err_name="lab_mapping", err_value=bad_rows["TEST_ID"])

    df.loc[is_national, col] = df.loc[is_national, "TEST_ID"].map(thl_lab_map)
    df[col] = df[col].str.replace('"', "")
    return df


def get_coding_map(df: pd.DataFrame) -> pd.DataFrame:
    """Map CODING_SYSTEM via the THL organization map, then derive CODING_SYSTEM_MAP.

    Faithfully replicates finngen_qc's ordering: CODING_SYSTEM_MAP's prefix-stripping step
    reads CODING_SYSTEM *after* it has already been overwritten by the first mapping round,
    so it only resolves to a non-"NA" value for rows the first round left unmapped.
    """
    col = "CODING_SYSTEM"
    df[col] = df[col].map(reference_data.get_thl_sote_map())

    tmp_system = (
        df[col]
        .str.replace("1.2.246.10.", "", regex=False)
        .str.replace("1.2.246.537.10.", "", regex=False)
        .str.split(".", n=1, expand=False)
        .str[0]
    )
    df["CODING_SYSTEM_MAP"] = tmp_system.map(reference_data.get_thl_manual_map()).fillna("NA")
    return df


def fix_abbreviation(df: pd.DataFrame, abbr_changes: AbbrSink) -> pd.DataFrame:
    """Strip stray characters/patterns from TEST_NAME_ABBREVIATION and normalize dashes.

    Logs rows whose abbreviation changed, combining deletions and replacements into a
    single before/after diff, to its own abbr_changes sink (a separate output file) with
    explicit OLD_ABBR/NEW_ABBR columns.
    """
    col = "TEST_NAME_ABBREVIATION"
    old = df[col].copy()

    pattern = "|".join(config.ABBREVIATION_DELETION_PATTERNS)
    df[col] = df[col].replace(pattern, "", regex=True)
    for old_char, new_char in config.ABBREVIATION_REPLACEMENTS:
        df[col] = df[col].replace(old_char, new_char, regex=True)

    is_changed = old != df[col]
    changed_rows = df.loc[is_changed]
    abbr_changes.add(
        changed_rows,
        err_name="abbreviation_change",
        old_abbr=old.loc[is_changed],
        new_abbr=df.loc[is_changed, col],
    )
    return df


def map_measurement_method(df: pd.DataFrame) -> pd.DataFrame:
    """Map MEASUREMENT_METHOD codes to a short English label."""
    df["MEASUREMENT_METHOD"] = (
        df["MEASUREMENT_METHOD"].map(config.MEASUREMENT_METHOD_MAP).fillna("NA")
    )
    return df


def run(
    df: pd.DataFrame, errors: ErrorSink, abbr_changes: AbbrSink, verbose: bool = False
) -> pd.DataFrame:
    df = (
        df.pipe(fix_date, errors)
        .pipe(remove_spaces)
        .pipe(fix_na)
        .pipe(filter_measurement_status, errors)
        .pipe(lab_id_source)
        .pipe(get_lab_abbrv, errors)
        .pipe(get_coding_map)
        .pipe(fix_abbreviation, abbr_changes)
        .pipe(map_measurement_method)
    )
    if verbose:
        n_errors = sum(len(frame) for frame in errors.frames)
        n_abbr_changes = sum(len(frame) for frame in abbr_changes.frames)
        print(f"[filter_minimal] {n_errors} rows flagged/dropped, {n_abbr_changes} abbreviations changed")
    return df
