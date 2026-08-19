import os
from argparse import ArgumentParser, BooleanOptionalAction
from datetime import date
from pathlib import Path

from kanta import log_utils, output


def init_cli():
    parser = ArgumentParser(
        description="Kanta Lab preprocessing pipeline: post-intake data ⇒ clean data."
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
        help=(
            "Process only the first chunk (useful for debugging). "
            "This overwrites --n-workers to 1."
        ),
        required=False,
    )
    parser.add_argument(
        "--output-prefix",
        type=Path,
        default=f"kanta_dev_{date.today():%Y_%m_%d}",
        help=(
            "Prefix for output file paths (Parquet). Produces <prefix>.parquet for the "
            "cleaned data, <prefix>_errors.parquet for rows dropped/flagged by filters, "
            "<prefix>_abbr.parquet for TEST_NAME_ABBREVIATION changes, and "
            "<prefix>_unit.parquet for MEASUREMENT_UNIT changes. Future output files "
            "follow the same <prefix>_<name>.parquet convention. Defaults to "
            "kanta_dev_<YYYY_MM_DD> (today's date) in the current directory."
        ),
        required=False,
    )
    parser.add_argument(
        "--release",
        action=BooleanOptionalAction,
        default=True,
        help=(
            "Also build the curated release file (<prefix>_RELEASE.parquet): a subset of "
            "the output columns, renamed/typed for external consumption. Use --no-release "
            "to skip it. Defaults to on."
        ),
    )
    parser.add_argument(
        "--n-workers",
        type=int,
        default=os.process_cpu_count() or 1,
        help=(
            "Number of worker processes used to process chunks in parallel. "
            "Defaults to the number of available CPUs. Use 1 to run serially "
            "(useful for debugging)."
        ),
        required=False,
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=chunking.N_LINES_PER_CHUNK,
        help=(
            f"Number of rows per chunk when streaming the input Parquet file. "
            f"Defaults to {chunking.N_LINES_PER_CHUNK}. Independent of --n-workers "
            "(see chunking.py for why scaling it by worker count is a bad idea)."
        ),
        required=False,
    )
    parser.add_argument(
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
    )
    parser.add_argument(
        "--verbose",
        help=(
            "Print per-filter debugging output (e.g. mapping counts). Only takes effect "
            "together with --test."
        ),
        action="store_true",
    )

    args = parser.parse_args()

    if args.n_workers < 1:
        raise ValueError("--n-workers must be 1 or more")

    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be 1 or more")

    if args.test:
        args.n_workers = 1

    return args


if __name__ == "__main__":
    args = init_cli()

    output_file = output.check_safe_write(output.derive_output_path(args.output_prefix))
    errors_file = output.check_safe_write(
        output.derive_output_path(args.output_prefix, "_errors")
    )
    abbr_file = output.check_safe_write(
        output.derive_output_path(args.output_prefix, "_abbr")
    )
    unit_file = output.check_safe_write(
        output.derive_output_path(args.output_prefix, "_unit")
    )
    log_file = output.check_safe_write(
        args.output_prefix.parent / f"{args.output_prefix.name}.log"
    )
    release_file = output.check_safe_write(
        output.derive_output_path(args.output_prefix, "_RELEASE")
    ) if args.release else None

    log_utils.configure_logging(log_file)

    tmp_dir = output.create_tmp_dir()

    main(
        args.input_file,
        output_file,
        errors_file,
        abbr_file,
        unit_file,
        tmp_dir,
        release_file=release_file,
        is_test_run=args.test,
        n_workers=args.n_workers,
        chunk_size=args.chunk_size,
        verbose=args.verbose,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
