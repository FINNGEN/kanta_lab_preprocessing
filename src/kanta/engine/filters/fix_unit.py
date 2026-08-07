import re

import pandas as pd

from kanta import config
from kanta.engine import reference_data
from kanta.engine.errors import UnitSink


def _strip_unit_chars(series: pd.Series) -> pd.Series:
    """Remove stray characters from a MEASUREMENT_UNIT-like string series, then lowercase
    non-NA values."""
    pattern = "(" + "|".join(re.escape(char) for char in config.UNIT_STRIP_CHARS) + ")"
    series = series.replace(pattern, "", regex=True).replace(r"^\s*$", "NA", regex=True)

    is_not_na = series != "NA"
    series.loc[is_not_na] = series.loc[is_not_na].str.lower()
    return series


def _apply_unit_fixes(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Fix a MEASUREMENT_UNIT-like string series: exact-match dictionary lookup, then regex
    clean-up. Returns (fixed_series, is_mapped)."""
    unit_map = reference_data.get_unit_map()
    is_mapped = series.isin(unit_map)
    series = series.copy()
    series.loc[is_mapped] = series.loc[is_mapped].map(unit_map)

    for pattern, replacement in config.UNIT_REPLACEMENTS:
        series.loc[~is_mapped] = series.loc[~is_mapped].replace(pattern, replacement, regex=True)

    return series, is_mapped


def normalize_unit_candidate(series: pd.Series) -> pd.Series:
    """Apply the same cleaning fix_unit.run() applies to MEASUREMENT_UNIT to an arbitrary
    candidate string (e.g. a unit token pulled from free text). No logging — a candidate
    isn't a row's real unit yet."""
    return _apply_unit_fixes(_strip_unit_chars(series))[0]


def strip_unit_characters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove stray characters from MEASUREMENT_UNIT, then lowercase non-NA values."""
    df["MEASUREMENT_UNIT"] = _strip_unit_chars(df["MEASUREMENT_UNIT"])
    return df


def fix_measurement_unit(df: pd.DataFrame, unit_changes: UnitSink) -> pd.DataFrame:
    """Fix MEASUREMENT_UNIT: exact-match dictionary lookup, then regex clean-up on the rest.

    Both steps log to unit_changes under separate err_names ("unit_map" vs "unit_regex"),
    since dictionary hits alone can be millions of rows (e.g. "10e9/l" -> "e9/l").
    """
    col = "MEASUREMENT_UNIT"
    old = df[col].copy()
    df[col], is_mapped = _apply_unit_fixes(df[col])

    is_map_changed = is_mapped & (old != df[col])
    unit_changes.add(
        df.loc[is_map_changed],
        err_name="unit_map",
        old_unit=old.loc[is_map_changed],
        new_unit=df.loc[is_map_changed, col],
    )

    is_regex_changed = ~is_mapped & (old != df[col])
    unit_changes.add(
        df.loc[is_regex_changed],
        err_name="unit_regex",
        old_unit=old.loc[is_regex_changed],
        new_unit=df.loc[is_regex_changed, col],
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
        print(f"[fix_unit] {n_unit_changes} units fixed (dictionary + regex)")
    return df
