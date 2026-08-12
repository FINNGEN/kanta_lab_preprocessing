if __name__ == "__main__":
    import datetime
    import os
    from argparse import ArgumentParser
    from pathlib import Path

    from kanta import engine, output
    from kanta.engine import chunking
    from kanta.intake import assemble, tidyup


    today = datetime.datetime.now(tz=datetime.UTC).date()

    parser = ArgumentParser(
        description="Kanta Lab preprocessing pipeline: raw data ⇒ clean data"
    )

    parser.add_argument(
        "--source-list-file",
        required=True,
        type=Path,
        help="File containing pair of paths to main & freetext data, one pair per line (TSV without header).",
    )
    parser.add_argument(
        "--phenotype-file",
        help="Path to phenotype file with FINNGENID and SEX columns (.txt.gz)",
        required=True,
        type=Path,
    )
    parser.add_argument(
        "--output-dir",
        help="Path to write the output files",
        required=True,
        type=Path,
    )
    parser.add_argument(
        "--partition-n-buckets",
        help="How many buckets to partition the data into to spread the sort+dedup computations.",
        required=False,
        type=int,
        default=24,
    )
    parser.add_argument(
        "--engine-n-workers",
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
        "--engine-chunk-size",
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
        "--engine-test-run",
        action="store_true",
        help=(
            "Process only the first chunk (useful for debugging). "
            "This overwrites --engine-n-workers to 1."
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
            "together with --engine-test."
        ),
        action="store_true",
    )

    args = parser.parse_args()

    if args.engine_n_workers < 1:
        raise ValueError("--engine-n-workers must be 1 or more")

    if args.engine_test_run:
        args.engine_n_workers = 1

    if args.engine_chunk_size < 1:
        raise ValueError("--engine-chunk-size must be 1 or more")

    # Setup
    output_file_assemble_stage = output.check_safe_write(
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses.assemble-stage.{today}.parquet"
    )
    output_file_tidyup_stage = output.check_safe_write(
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_internal_1.0_{today}.parquet"
    )
    output_file_engine = output.check_safe_write(
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_1.0_{today}.parquet"
    )
    output_file_engine_errors = output.check_safe_write(
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_1.0_{today}_errors.parquet"
    )
    output_file_engine_abbr = output.check_safe_write(
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_1.0_{today}_abbr.parquet"
    )
    output_file_engine_unit = output.check_safe_write(
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_1.0_{today}_unit.parquet"
    )

    tmp_dir = output.create_tmp_dir()

    # STAGE: Intake - Assemble
    assemble.main(
        source_list_file=args.source_list_file, output_file=output_file_assemble_stage
    )

    # STAGE: Intake - Tidyup
    tidyup.main(
        output_file_assemble_stage,
        args.phenotype_file,
        output_file_tidyup_stage,
        tmp_dir=tmp_dir,
        partition_n_buckets=args.partition_n_buckets,
    )

    # STAGE: Engine
    engine.main(
        input_file=output_file_tidyup_stage,
        output_file=output_file_engine,
        errors_file=output_file_engine_errors,
        abbr_file=output_file_engine_abbr,
        unit_file=output_file_engine_unit,
        tmp_dir=tmp_dir,
        is_test_run=args.engine_test_run,
        n_workers=args.engine_n_workers,
        chunk_size=args.engine_chunk_size,
        verbose=args.verbose,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
