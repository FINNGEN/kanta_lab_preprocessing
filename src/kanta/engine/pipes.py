import pandas as pd

from kanta import config
from kanta.engine.errors import AbbrSink, ErrorSink, UnitSink
from kanta.engine.filters import filter_minimal, fix_unit, harmonization


def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=config.COLUMN_ALIASES)


def snapshot_source_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Preserve the raw pre-cleaning value of config.SOURCE_COLUMNS as source::<col>."""
    for col in config.SOURCE_COLUMNS:
        df[f"source::{col}"] = df[col]
    return df


def select_out_cols(df: pd.DataFrame) -> pd.DataFrame:
    if not config.OUT_COLUMNS:
        return df
    return df[config.OUT_COLUMNS]


def run_all(
    df: pd.DataFrame,
    errors: ErrorSink,
    abbr_changes: AbbrSink,
    unit_changes: UnitSink,
    verbose: bool = False,
    bc_threshold: float = config.BIMODAL_BC_THRESHOLD_DEFAULT,
    overlap_threshold: float = config.BIMODAL_OVERLAP_THRESHOLD_DEFAULT,
) -> pd.DataFrame:
    return (
        df.pipe(rename_cols)
        .pipe(snapshot_source_cols)
        .pipe(filter_minimal.run, errors, abbr_changes, verbose)
        .pipe(fix_unit.run, unit_changes, verbose)
        .pipe(harmonization.run, verbose, bc_threshold, overlap_threshold)
        .pipe(select_out_cols)
    )
