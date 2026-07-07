import re

import pandas as pd

from kanta import config
from kanta.engine import reference_data
from kanta.engine.errors import UnitSink


def strip_unit_characters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove stray characters from MEASUREMENT_UNIT, then lowercase non-NA values."""
    col = "MEASUREMENT_UNIT"
    pattern = "(" + "|".join(re.escape(char) for char in config.UNIT_STRIP_CHARS) + ")"
    df[col] = df[col].replace(pattern, "", regex=True).replace(r"^\s*$", "NA", regex=True)

    is_not_na = df[col] != "NA"
    df.loc[is_not_na, col] = df.loc[is_not_na, col].str.lower()
    return df


def fix_measurement_unit(df: pd.DataFrame, unit_changes: UnitSink) -> pd.DataFrame:
    """Fix MEASUREMENT_UNIT in two steps: exact-match dictionary lookup, then regex clean-up.

    Rows resolved by the dictionary lookup (config.UNIT_MAP_FILE) are excluded from the
    regex step. Only regex-driven changes are logged to unit_changes, matching prior
    behavior where dictionary-driven changes were never logged.
    """
    col = "MEASUREMENT_UNIT"
    unit_map = reference_data.get_unit_map()
    is_mapped = df[col].isin(unit_map)
    df.loc[is_mapped, col] = df.loc[is_mapped, col].map(unit_map)

    old = df[col].copy()
    for pattern, replacement in config.UNIT_REPLACEMENTS:
        df.loc[~is_mapped, col] = df.loc[~is_mapped, col].replace(pattern, replacement, regex=True)

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


def run(df: pd.DataFrame, unit_changes: UnitSink) -> pd.DataFrame:
    return (
        df.pipe(strip_unit_characters)
        .pipe(fix_measurement_unit, unit_changes)
        .pipe(fix_test_outcome)
    )
