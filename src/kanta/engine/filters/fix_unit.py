import re

import pandas as pd

from kanta import config
from kanta.engine import reference_data
from kanta.engine.errors import UnitSink


def _strip_unit_chars(series: pd.Series) -> pd.Series:
    """Remove stray characters from a MEASUREMENT_UNIT-like string series, then lowercase
    non-NA values. Pulled out of strip_unit_characters() so the exact same cleaning can be
    applied to a free-standing candidate (e.g. a unit token pulled from free text), not just
    the structured MEASUREMENT_UNIT column."""
    pattern = "(" + "|".join(re.escape(char) for char in config.UNIT_STRIP_CHARS) + ")"
    series = series.replace(pattern, "", regex=True).replace(r"^\s*$", "NA", regex=True)

    is_not_na = series != "NA"
    series.loc[is_not_na] = series.loc[is_not_na].str.lower()
    return series


def _apply_unit_fixes(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Fix a MEASUREMENT_UNIT-like string series in two steps: exact-match dictionary lookup,
    then regex clean-up. Returns (fixed_series, is_mapped) — is_mapped identifies rows the
    dictionary lookup already resolved (excluded from the regex step), needed by the caller to
    know which changes were dictionary- vs regex-driven. Pulled out of fix_measurement_unit()
    for the same sharing reason as _strip_unit_chars()."""
    unit_map = reference_data.get_unit_map()
    is_mapped = series.isin(unit_map)
    series = series.copy()
    series.loc[is_mapped] = series.loc[is_mapped].map(unit_map)

    for pattern, replacement in config.UNIT_REPLACEMENTS:
        series.loc[~is_mapped] = series.loc[~is_mapped].replace(pattern, replacement, regex=True)

    return series, is_mapped


def normalize_unit_candidate(series: pd.Series) -> pd.Series:
    """Apply the exact same cleaning fix_unit.run() applies to the structured MEASUREMENT_UNIT
    column to an arbitrary candidate unit-like string series (e.g. a token pulled from free
    text in harmonization.extract_measurement) — no UnitSink/logging, since a candidate isn't
    a row's real unit yet, just a string being evaluated for whether it could be one.
    """
    return _apply_unit_fixes(_strip_unit_chars(series))[0]


def strip_unit_characters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove stray characters from MEASUREMENT_UNIT, then lowercase non-NA values."""
    df["MEASUREMENT_UNIT"] = _strip_unit_chars(df["MEASUREMENT_UNIT"])
    return df


def fix_measurement_unit(df: pd.DataFrame, unit_changes: UnitSink) -> pd.DataFrame:
    """Fix MEASUREMENT_UNIT in two steps: exact-match dictionary lookup, then regex clean-up.

    Rows resolved by the dictionary lookup (config.UNIT_MAP_FILE) are excluded from the
    regex step. Only regex-driven changes are logged to unit_changes, matching prior
    behavior where dictionary-driven changes were never logged.
    """
    col = "MEASUREMENT_UNIT"
    old = df[col].copy()
    df[col], is_mapped = _apply_unit_fixes(df[col])

    is_changed = ~is_mapped & (old != df[col])
    changed_rows = df.loc[is_changed]
    unit_changes.add(
        changed_rows,
        err_name="unit_regex",
        old_unit=old.loc[is_changed],
        new_unit=df.loc[is_changed, col],
    )
    return df


def fix_test_outcome(df: pd.DataFrame) -> pd.DataFrame:
    """Rewrite TEST_OUTCOME codes to their standard AR/LABRA equivalent (< -> L, > -> H).

    Every other raw value passes through unchanged.
    """
    col = "TEST_OUTCOME"
    is_mapped = df[col].isin(config.TEST_OUTCOME_MAP)
    df.loc[is_mapped, col] = df.loc[is_mapped, col].map(config.TEST_OUTCOME_MAP)
    return df


def run(df: pd.DataFrame, unit_changes: UnitSink, verbose: bool = False) -> pd.DataFrame:
    df = (
        df.pipe(strip_unit_characters)
        .pipe(fix_measurement_unit, unit_changes)
        .pipe(fix_test_outcome)
    )
    if verbose:
        n_unit_changes = sum(len(frame) for frame in unit_changes.frames)
        print(f"[fix_unit] {n_unit_changes} units regex-fixed")
    return df
