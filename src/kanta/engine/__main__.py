import warnings

import pandas as pd

from kanta import config
from kanta.engine import injection


def main():
    injection.fake_run(config.EXAMPLE_VAR)

    # Efficient read
    pd.read_parquet(
        "path-to-file.parquet",
        engine="pyarrow",
        # Select only the given columns, this speeds up the read quite a lot for Parquet files.
        # The resulting DataFrame contains only the selected columns.
        columns=["COL_A", "COL_C"],
        # Filtering of rows.
        # Only basic filters are available (==, =, >, >=, <, <=, !=, in, not in). This speeds up
        # the read as well.
        # !! WARNING !!
        # The behavior of the filtering changes based on the engine! Only when engine is "pyarrow"
        # will the resulting rows be only the one matching the filter. Otherwise this will result
        # in a superset of requested rows.
        filters=[
            ('COL_A', '=', 'something')
        ],
        # Use nullable data-types backed by pyarrow.
        # - Nullable: missing values stay as NA rather than forcing integer columns up to float or
        #   string columns into the object dtype.
        # - pyarrow: better fit for Parquet files, and future-proofing since Pandas is moving into
        #   that direction.
        dtype_backend="pyarrow",
    )


def configure_pandas():
    # Treat NaN (a real float value) and NA (a missing value) as distinct.
    pd.options.future.distinguish_nan_and_na = True

    # Default to the pyarrow engine for all Parquet reads/writes, so we don't have to pass
    # engine="pyarrow" on every call.
    pd.options.io.parquet.engine = "pyarrow"

    # Turn chained assignment into a hard error.
    # Chained assignment have weird behavior in that they would turn operations into no-op.
    # For example this chained assignment *does not change any value* of df:
    #   `df[df["A"] > 0]["B"] = 1`
    # Instead, use `.loc` or `.iloc` to get correct behavior:
    #   `df.loc[df["A"] > 0, "B"] = 1`
    # We change Pandas default from jsut throw a warning (ChainedAssignmentError), to actually
    # raising an error.
    # Note that unfortunately we can't just set the option
    # `mode.chained_assignment` to `"raise"` because it has no effect under
    # Copy-on-Write, so we have to resort to promoting the warning to an error.
    warnings.filterwarnings("error", category=pd.errors.ChainedAssignmentError)


if __name__ == "__main__":
    configure_pandas()
    main()
