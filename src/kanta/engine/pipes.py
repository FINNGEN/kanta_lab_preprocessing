import pandas as pd

from kanta import config
from kanta.engine.errors import AbbrSink, ErrorSink
from kanta.engine.filters import filter_minimal


def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=config.COLUMN_ALIASES)


def select_out_cols(df: pd.DataFrame) -> pd.DataFrame:
    if not config.OUT_COLUMNS:
        return df
    return df[config.OUT_COLUMNS]


def run_all(df: pd.DataFrame, errors: ErrorSink, abbr_changes: AbbrSink) -> pd.DataFrame:
    return (
        df.pipe(rename_cols)
        .pipe(filter_minimal.run, errors, abbr_changes)
        .pipe(select_out_cols)
    )
