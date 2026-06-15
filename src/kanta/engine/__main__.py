import warnings
from argparse import ArgumentParser
from pathlib import Path

import pandas as pd

from kanta import output
from kanta.engine import reader, writer


def main(
    input_file: Path,
    output_file: Path,
    tmp_dir: Path,
    *,
    is_test_run=False,
):
    # Setup
    configure_pandas()

    chunks_dir = tmp_dir / "chunks"
    chunks_dir.mkdir()

    # Iterate over each chunk
    for chunk_index, df_chunk in enumerate(
        reader.chunk_iterator(input_file, is_test_run=is_test_run)
    ):

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

        writer.write_chunk(df_chunk, chunks_dir, chunk_index)

    writer.concatenate_chunks(chunks_dir, output_file)


def init_cli():
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
        "--test",
        action="store_true",
        help="Process only the first chunk. Use for development.",
        required=False,
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        help="Output file path (Parquet)",
        required=True,
    )
    parser.add_argument(
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
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
    args = init_cli()

    output.check_safe_write(args.output_file)
    tmp_dir = output.create_tmp_dir()

    main(
        args.input_file,
        args.output_file,
        tmp_dir,
        is_test_run=args.test,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
