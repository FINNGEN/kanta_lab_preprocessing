import pandas as pd
import pyarrow as pa

from kanta import config

# pandas dtype string (as used in config.OUTPUT_COLUMNS) -> pyarrow type, for
# RELEASE_EMPTY_SCHEMA below.
_DTYPE_TO_ARROW = {
    "Int64": pa.int64(),
    "Int8": pa.int8(),
    "Float64": pa.float64(),
    "boolean": pa.bool_(),
    "string": pa.string(),
    "datetime64[ns, UTC]": pa.timestamp("ns", tz="UTC"),
}


def _release_spec(column_spec: tuple) -> tuple[bool, str, str, bool]:
    """Normalize a config.OUTPUT_COLUMNS value to (keep, name, dtype, nullify)."""
    keep, name, dtype, *rest = column_spec
    nullify = rest[0] if rest else True
    return keep, name, dtype, nullify


def build_release(df: pd.DataFrame) -> pd.DataFrame:
    """Select/rename/type the engine-output columns kept for the release file.

    Reads config.OUTPUT_COLUMNS: keep=False columns are dropped. Everything else is
    renamed and cast to its release dtype, replacing the literal "NA" sentinel with a
    real null first -- unless nullify=False for that column, where an unmapped/unknown
    value is itself informative (e.g. TEST_NAME) rather than missing data.
    """
    out = {}
    for src_col, spec in config.OUTPUT_COLUMNS.items():
        keep, name, dtype, nullify = _release_spec(spec)
        if not keep:
            continue

        series = df[src_col]

        if dtype == "datetime64[ns, UTC]":
            out[name] = pd.to_datetime(series, format=config.DATE_TIME_FORMAT, utc=True)
            continue

        if nullify:
            series = series.replace("NA", pd.NA)
        if dtype == "boolean":
            series = series.map({"1": True, "0": False})
        out[name] = series.astype(dtype)

    return pd.DataFrame(out, index=df.index)


# Schema used to write an empty release file when a chunk has zero rows, so the release
# output file can always be relied on to exist.
RELEASE_EMPTY_SCHEMA = pa.schema(
    [
        (name, _DTYPE_TO_ARROW[dtype])
        for keep, name, dtype, _ in (_release_spec(spec) for spec in config.OUTPUT_COLUMNS.values())
        if keep
    ]
)
