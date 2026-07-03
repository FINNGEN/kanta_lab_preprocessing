import os
from argparse import ArgumentParser
from pathlib import Path

from kanta import output
from kanta.engine import main


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
        help=(
            "Process only the first chunk (useful for debugging). "
            "This overwrites --n-workers to 1."
        ),
        required=False,
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        help="Output file path (Parquet)",
        required=True,
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
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
    )

    args = parser.parse_args()

    if args.n_workers < 1:
        raise ValueError("--n-workers must be 1 or more")

    if args.test:
        args.n_workers = 1

    return args


if __name__ == "__main__":
    args = init_cli()

    output.check_safe_write(args.output_file)
    tmp_dir = output.create_tmp_dir()

    main(
        args.input_file,
        args.output_file,
        tmp_dir,
        is_test_run=args.test,
        n_workers=args.n_workers,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
