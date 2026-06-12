import warnings
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd

from kanta.engine import output, reader


def main():
    args = cli_init()

    for df_chunk in reader.chunk_iterator(args.input_file):

        def noop_filter(df):
            return df

        df_chunk = (
            df_chunk.pipe(noop_filter)
            .pipe(noop_filter)
            .pipe(noop_filter)
            .pipe(noop_filter)
            .pipe(noop_filter)
            .pipe(noop_filter)
        )

    # TODO concat chunks for outputting


def cli_init():
    parser = ArgumentParser(
        description="Kanta Lab preprocessing pipeline: raw data ⇒ clean data."
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        help="Path to the Kanta Lab data file coming from the intake stage (Parquet)",
        required=True,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Directory to store the output data files.",
        required=True,
    )

    return parser.parse_args()


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
