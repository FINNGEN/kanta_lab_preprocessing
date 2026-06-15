import pandas as pd


def noop_filter(df):
    return df


def rename_cols(df : pd.DataFrame, *, col_mapping: dict) -> pd.DataFrame:
    return df.rename(columns=col_mapping)
