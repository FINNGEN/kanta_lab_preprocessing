import re

import pandas as pd

from kanta import config
from kanta.engine import reference_data


def extract_positive(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Assign extracted::IS_POS ("0"/"1") from a free-text positive/negative lookup table."""
    posneg_table = reference_data.get_posneg_table()
    df["extracted::IS_POS"] = df["MEASUREMENT_FREE_TEXT"].map(posneg_table).fillna("NA")

    if verbose:
        counts = df["extracted::IS_POS"].value_counts().to_dict()
        print(f"[outcome] extract_positive: {counts}")
    return df


def extract_outcome(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Extract a "<comparator><value><unit>" string (e.g. "<5.2mmol/l") from free-text
    out-of-range results into extracted::TEST_OUTCOME_TEXT ("NA" if nothing is extracted).

    Only text containing config.STATUS_INDICATORS ("<", ">", "yli"/"alle") is considered; the
    unit is cleaned the same way MEASUREMENT_UNIT is and kept only if Usagi-recognized.
    """
    ft_col = "MEASUREMENT_FREE_TEXT"
    col = "extracted::TEST_OUTCOME_TEXT"

    df[col] = "NA"

    cleaned = df[ft_col].str.lower()
    for pattern in config.FREE_TEXT_RESULT_STRINGS:
        cleaned = cleaned.str.replace(rf"^\s*{pattern}\s*", "", regex=True)
    for pattern, replacement in config.FREE_TEXT_MEASUREMENT_REPLACEMENTS:
        cleaned = cleaned.str.replace(pattern, replacement, regex=True)

    status_mask = cleaned.str.contains("|".join(config.STATUS_INDICATORS), na=False)
    if not status_mask.any():
        if verbose:
            print(f"[outcome] extract_outcome: 0/{len(df)} rows extracted")
        return df

    status_text = cleaned.loc[status_mask]
    for indicator in config.STATUS_INDICATORS:
        status_text = status_text.str.replace(indicator, indicator + " ", regex=True)
    status_text = status_text.str.replace(r"\s+", " ", regex=True)

    parts = status_text.str.split(" ", expand=True, n=4).reindex(columns=[0, 1, 2, 3])
    parts.columns = ["comp", "value", "unit", "extra"]
    if parts.empty:
        if verbose:
            print(f"[outcome] extract_outcome: 0/{len(df)} rows extracted")
        return df

    parts["comp"] = parts["comp"].replace("alle", "<", regex=True).replace("yli", ">", regex=True)

    has_dot = parts["value"].str.contains(".", regex=False, na=False)
    trimmed_value = parts["value"].str.replace(r"0+$", "", regex=True).str.replace(r"\.$", "", regex=True)
    parts["value"] = parts["value"].where(~has_dot, trimmed_value)

    strip_pattern = "(" + "|".join(re.escape(char) for char in config.UNIT_STRIP_CHARS) + ")"
    parts["unit"] = parts["unit"].replace(strip_pattern, "", regex=True)
    unit_map = reference_data.get_unit_map()
    is_mapped = parts["unit"].isin(unit_map)
    parts.loc[is_mapped, "unit"] = parts.loc[is_mapped, "unit"].map(unit_map)

    usagi_units = reference_data.get_usagi_units()
    # parts["value"] stays Arrow-backed (string[pyarrow]) through str.split(expand=True), so
    # .notna() on the raw pd.to_numeric() result would wrongly admit a non-numeric token that
    # happens to coerce to a NaN value (pyarrow's validity bitmap still marks it "not null") --
    # see the same fix/reasoning in harmonization.py's unit_harmonization.
    is_numeric_value = pd.to_numeric(parts["value"], errors="coerce").astype("float64").notna()
    is_valid = (
        parts["comp"].isin(["<", ">"])
        & is_numeric_value
        & (parts["unit"].isin(usagi_units) | parts["unit"].isna())
    )

    extracted_idx = parts.index[is_valid]
    df.loc[extracted_idx, col] = (
        parts.loc[extracted_idx, "comp"]
        + parts.loc[extracted_idx, "value"]
        + parts.loc[extracted_idx, "unit"].fillna("")
    )

    if verbose:
        print(f"[outcome] extract_outcome: {len(extracted_idx)}/{len(df)} rows extracted")
    return df


def impute_outcome(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Assign imputed::TEST_OUTCOME (L/L*/H/H*/N) by comparing harmonization_omop::
    MEASUREMENT_VALUE against the (OMOP_ID-indexed) LOW_LIMIT/HIGH_LIMIT reference range.

    See scripts/qc_scripts/abnormality.py for how that table is built.
    """
    limits = reference_data.get_ab_limits()
    matched = limits.reindex(df["harmonization_omop::OMOP_ID"]).set_axis(df.index)

    value = pd.to_numeric(df["harmonization_omop::MEASUREMENT_VALUE"], errors="coerce")
    low_limit = pd.to_numeric(matched["LOW_LIMIT"], errors="coerce")
    high_limit = pd.to_numeric(matched["HIGH_LIMIT"], errors="coerce")

    outcome = pd.Series("NA", index=df.index)

    is_low = value < low_limit
    outcome.loc[is_low] = "L"
    outcome.loc[is_low & (matched["LOW_PROBLEM"] == "1")] = "L*"

    is_high = value > high_limit
    outcome.loc[is_high] = "H"
    outcome.loc[is_high & (matched["HIGH_PROBLEM"] == "1")] = "H*"

    is_normal = (value >= low_limit) & (value <= high_limit)
    outcome.loc[is_normal] = "N"

    df["imputed::TEST_OUTCOME"] = outcome

    if verbose:
        print(f"[outcome] impute_outcome: {outcome.value_counts().to_dict()}")
    return df


def run(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    df = (
        df.pipe(extract_positive, verbose)
        .pipe(extract_outcome, verbose)
        .pipe(impute_outcome, verbose)
    )
    return df
