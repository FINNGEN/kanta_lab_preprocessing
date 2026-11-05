import re

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


def get_lab_abbrv(df: pd.DataFrame, abbr_changes: AbbrSink) -> pd.DataFrame:
    """Assign TEST_NAME_ABBREVIATION: keep the local name if TEST_ID is local, map via THL otherwise.

    National ids missing from the THL map fall back to the raw TEST_ID (get_thl_lab_map() is a
    FallbackToKeyDict) rather than being dropped, so this is logged as an abbreviation change
    to abbr_changes, not an error — the row isn't lost, just resolved differently.
    """
    col = "TEST_NAME_ABBREVIATION"
    is_not_na = df[col] != "NA"
    df.loc[is_not_na, col] = df.loc[is_not_na, col].str.lower()

    thl_lab_map = reference_data.get_thl_lab_map()
    is_national = df["TEST_ID_IS_NATIONAL"] == "1"
    is_unmapped = ~df["TEST_ID"].isin(thl_lab_map)

    old_abbr = df[col].copy()
    df.loc[is_national, col] = df.loc[is_national, "TEST_ID"].map(thl_lab_map)
    df[col] = df[col].str.replace('"', "")

    fallback_mask = is_national & is_unmapped
    fallback_rows = df.loc[fallback_mask]
    abbr_changes.add(
        fallback_rows,
        err_name="lab_mapping",
        old_abbr=old_abbr.loc[fallback_mask],
        new_abbr=fallback_rows[col],
    )
    return df


# Coding systems hosted by CGI (org code 3575029) embed the customer organization as "11" + its
# Y-tunnus digits, usually zero-padded to 8, e.g. "...3575029.12.2.10.1102012566.6.17" -> "2012566"
# (Helsinki). Customer numbers of another form can't be decoded and stay unmapped.
CGI_CODING_SYSTEM = re.compile(r"^1\.2\.246\.537\.10\.3575029\.12\.2\.10\.(\d+)\.6\.17$")
CGI_CUSTOMER = re.compile(r"^110?(\d{7,8})$")
# Other organization OIDs: the code is the arc after the root
OID_ORG_CODE = re.compile(r"^1\.2\.246\.(?:537\.)?10\.(\d+)")


def ytunnus_check_digit(body: str) -> int | None:
    """Check digit of a 7-digit Y-tunnus body (weights 7,9,10,5,8,4,2, mod 11); None if no valid one exists."""
    remainder = sum(int(d) * w for d, w in zip(body, (7, 9, 10, 5, 8, 4, 2))) % 11
    if remainder == 1:
        return None
    return 0 if remainder == 0 else 11 - remainder


def complete_old_form_code(code: str) -> str:
    """Old-form OIDs embed the 7-digit Y-tunnus body without its check digit, keeping the leading zero
    (e.g. "1.2.246.10.0132239.10.0": 0132239 -> Y-tunnus 0132239-4). OID arcs normally have no leading
    zeros, so a 7-digit code starting with 0 is completed to the current form used by the mapping table:
    body + check digit, leading zeros stripped ("0132239" -> "1322394"). Other codes are returned as is."""
    if len(code) == 7 and code.startswith("0") and code.isdigit():
        check = ytunnus_check_digit(code)
        if check is not None:
            return (code + str(check)).lstrip("0")
    return code


def coding_system_org_code(coding_system: str) -> str | None:
    """Organization code (Y-tunnus digits, no leading zeros) embedded in a CODING_SYSTEM OID: the CGI
    customer for CGI-hosted systems, else the arc after 1.2.246.10. / 1.2.246.537.10. (old-form codes
    completed); None if there is none (not an organization OID, or an undecodable CGI customer)."""
    m = CGI_CODING_SYSTEM.match(coding_system)
    if m:
        customer = CGI_CUSTOMER.match(m.group(1))
        return customer.group(1).lstrip("0") if customer else None
    m = OID_ORG_CODE.match(coding_system)
    return complete_old_form_code(m.group(1)).lstrip("0") if m else None


def get_coding_map(df: pd.DataFrame) -> pd.DataFrame:
    """Derive CODING_SYSTEM_MAP (organization name) and CODING_SYSTEM_MAAKUNTA (the organization's
    maakunta) from the CODING_SYSTEM OID, which is kept as is.

    Looked up in the coding system table (reference_data.get_coding_system_map, built from THL's sote
    register): by the full OID, else by the organization code embedded in it, see
    coding_system_org_code(). The maakunta is "NA" for organizations without one region (private,
    labs serving several regions); both columns are "NA" when nothing matches.
    """
    col = "CODING_SYSTEM"
    by_oid, by_code = reference_data.get_coding_system_map()
    # few distinct values per chunk: resolve each once, then map
    resolved = {}
    for coding_system in df[col].dropna().unique():
        hit = by_oid.get(coding_system)
        if hit is None:
            hit = by_code.get(coding_system_org_code(coding_system))
        if hit is not None:
            resolved[coding_system] = hit
    df["CODING_SYSTEM_MAP"] = df[col].map({k: v[0] for k, v in resolved.items()}).fillna("NA")
    df["CODING_SYSTEM_MAAKUNTA"] = df[col].map({k: v[1] for k, v in resolved.items()}).fillna("NA")
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


def run(df: pd.DataFrame, errors: ErrorSink, abbr_changes: AbbrSink, verbose: bool = False) -> pd.DataFrame:
    df = (
        df.pipe(fix_date, errors)
        .pipe(remove_spaces)
        .pipe(fix_na)
        .pipe(filter_measurement_status, errors)
        .pipe(lab_id_source)
        .pipe(get_lab_abbrv, abbr_changes)
        .pipe(get_coding_map)
        .pipe(fix_abbreviation, abbr_changes)
        .pipe(map_measurement_method)
    )
    if verbose:
        n_errors = sum(len(frame) for frame in errors.frames)
        n_abbr_changes = sum(len(frame) for frame in abbr_changes.frames)
        print(f"[filter_minimal] {n_errors} rows flagged/dropped, {n_abbr_changes} abbreviations changed")
    return df
