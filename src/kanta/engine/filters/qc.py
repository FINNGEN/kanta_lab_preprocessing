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
    """Create QC_PASS as "2" (unchecked) if it doesn't exist yet."""
    if "QC_PASS" not in df.columns:
        df["QC_PASS"] = "2"


def check_dates_in_measurement(df: pd.DataFrame, errors: ErrorSink, verbose: bool = False) -> pd.DataFrame:
    """Drop rows whose MEASUREMENT_VALUE looks like a DDMMYY date typed into a value field
    (exactly 6 digits, day/month/year each in a plausible range).
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
            bad_rows["TEST_NAME_ABBREVIATION"].str.cat(bad_rows["harmonization_omop::MEASUREMENT_VALUE"], sep="::")
            .str.cat(bad_rows["MEASUREMENT_FREE_TEXT"], sep="::")
            .str.cat(bad_rows[col], sep="::")
        ),
    )

    if verbose:
        print(f"[qc] check_dates_in_measurement: {int(is_date.sum())}/{len(df)} rows dropped")
    return df.loc[~is_date]


def flag_omop_qc(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Flag QC_PASS="0" for rows failing a per-OMOP_ID threshold rule (get_omop_qc()).

    OMOP_ID  THRESHOLD  SIDE  ->  QC_PASS
    3000963  500        >     ->  "0" if value > 500
    3026361  20         >     ->  "0" if value > 20
    3026361  0.5        <     ->  "0" if value < 0.5

    Interval (</<=/>/>=) rules are applied via a single grouped merge_asof against a compiled
    per-OMOP_ID step function (see reference_data.get_compiled_omop_qc); ==/!= point rules are
    applied separately and OR'd on top, since they aren't representable as an interval.
    """
    _ensure_qc_pass(df)

    breakpoints, point_rules, registered_ids = reference_data.get_compiled_omop_qc()
    is_registered = df["harmonization_omop::OMOP_ID"].isin(registered_ids)
    df.loc[is_registered, "QC_PASS"] = "1"

    value = pd.to_numeric(df["harmonization_omop::MEASUREMENT_VALUE"], errors="coerce")
    n_flagged = 0

    has_interval = (
        df["harmonization_omop::OMOP_ID"].isin(breakpoints["harmonization_omop::OMOP_ID"])
        & value.notna()
    )
    if has_interval.any():
        probe = pd.DataFrame({
            "_orig_idx": df.index[has_interval],
            "harmonization_omop::OMOP_ID": df.loc[has_interval, "harmonization_omop::OMOP_ID"].to_numpy(),
            "value": value.loc[has_interval].to_numpy(),
        }).sort_values("value")

        merged = pd.merge_asof(
            probe, breakpoints.sort_values("threshold"),
            left_on="value", right_on="threshold",
            by="harmonization_omop::OMOP_ID",
            direction="backward",
        )
        fail = merged.loc[merged["note"].notna()]
        if len(fail):
            fail_idx = pd.Index(fail["_orig_idx"])
            df.loc[fail_idx, "QC_PASS"] = "0"
            add_qc_note(df, fail_idx, pd.Series(fail["note"].to_numpy(), index=fail_idx))
            n_flagged += len(fail_idx)

    for _, rule in point_rules.iterrows():
        omop_mask = df["harmonization_omop::OMOP_ID"] == rule["harmonization_omop::OMOP_ID"]
        fail_mask = omop_mask & _OPS[rule["SIDE"]](value, rule["THRESHOLD"])
        if not fail_mask.any():
            continue
        fail_idx = df.index[fail_mask]
        df.loc[fail_idx, "QC_PASS"] = "0"
        add_qc_note(df, fail_idx, pd.Series(rule["QC_NOTES"], index=fail_idx))
        n_flagged += len(fail_idx)

    if verbose:
        print(
            f"[qc] flag_omop_qc: {int(is_registered.sum())}/{len(df)} rows registered, "
            f"{n_flagged} entries flagged"
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
