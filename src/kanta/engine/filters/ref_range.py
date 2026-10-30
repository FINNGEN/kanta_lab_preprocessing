import numpy as np
import pandas as pd

from kanta.engine import reference_data
from kanta.engine.reference_data import ref_key

LOWER, UPPER = "REFERENCE_RANGE_LOWER_VALUE", "REFERENCE_RANGE_UPPER_VALUE"


def _number(s: pd.Series) -> pd.Series:
    """The column as floats, NaN where it is not a number; parses only the values that aren't missing ("NA")."""
    out = pd.Series(np.nan, index=s.index)
    given = (s.notna() & (s != "NA")).to_numpy(dtype=bool)
    # .astype("float64") before .notna(): an Arrow-backed result of pd.to_numeric() can mark a non-numeric token "not
    # null" (see harmonization.py's unit_harmonization)
    out[given] = pd.to_numeric(s[given], errors="coerce").astype("float64").to_numpy()
    return out


def _key(s: pd.Series) -> pd.Series:
    """ref_key() per row, computed once per distinct value."""
    s = s.astype(object)
    keys = {v: ref_key(v) for v in pd.unique(s)}
    return s.map(keys)


def inject_text_limits(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    """Fill REFERENCE_RANGE_LOWER/UPPER_VALUE from the curated reference-limits table on rows without numeric limits,
    and record where each row's limits come from in REFERENCE_RANGE_SOURCE: "num" (sent by the lab), "text" (filled
    here), "NA" (none).

    The key is ref_key(REFERENCE_RANGE_TEXT), else ref_key(REFERENCE_RANGE_GROUP), joined with TEST_NAME_ABBREVIATION
    on the table's approved pairs (reference_data.get_text_limits()). A row with any numeric limit from the lab is never
    touched; a one-sided text fills one column. The unit columns are left as they are: a text's limits are in the
    row's own unit.
    """
    has_num = _number(df[LOWER]).notna() | _number(df[UPPER]).notna()
    source = pd.Series(np.where(has_num, "num", "NA"), index=df.index)

    limits = reference_data.get_text_limits()
    key = _key(df["REFERENCE_RANGE_TEXT"]).fillna(_key(df["REFERENCE_RANGE_GROUP"]))
    rows = pd.MultiIndex.from_arrays([df["TEST_NAME_ABBREVIATION"].astype(object), key])
    matched = limits.reindex(rows).set_axis(df.index)
    fill = ~has_num & (matched["final_lower"].notna() | matched["final_upper"].notna())
    for col, limit in ((LOWER, matched["final_lower"]), (UPPER, matched["final_upper"])):
        m = fill & limit.notna()
        text = {x: np.format_float_positional(x, trim="-") for x in pd.unique(limit[m])}  # '4.0' -> '4', once each
        df.loc[m, col] = limit[m].map(text)
    source[fill] = "text"

    df["REFERENCE_RANGE_SOURCE"] = source
    if verbose:
        print(f"[ref_range] inject_text_limits: {source.value_counts().to_dict()}")
    return df


def run(df: pd.DataFrame, verbose: bool = False) -> pd.DataFrame:
    return df.pipe(inject_text_limits, verbose)
