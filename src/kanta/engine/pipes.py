import pandas as pd

from kanta import config


def noop_filter(df):
    return df


def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=config.COLUMN_ALIASES)


def run_all(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.pipe(rename_cols)
        .pipe(noop_filter)
        .pipe(noop_filter)
        .pipe(noop_filter)
        .pipe(noop_filter)
        .pipe(noop_filter)
        .pipe(noop_filter)
    )
