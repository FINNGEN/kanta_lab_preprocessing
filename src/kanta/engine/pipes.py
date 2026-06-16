import pandas as pd

from kanta import config


def noop_filter(df):
    return df


def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=config.RENAME_COLUMNS)
