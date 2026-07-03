if __name__ == "__main__":
    import os
    from argparse import ArgumentParser
    from datetime import date
    from pathlib import Path

    from kanta import engine
    from kanta import output
    from kanta.intake import assemble
    from kanta.intake import tidyup

    parser = ArgumentParser()

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

    args = parser.parse_args()

    if args.engine_n_workers < 1:
        raise ValueError("--engine-n-workers must be 1 or more")

    if args.engine_test_run:
        args.engine_n_workers = 1


    # Setup
    output_file_assemble_stage = (
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses.assemble-stage.{date.today()}.parquet"
    )
    output.check_safe_write(output_file_assemble_stage)

    output_file_tidyup_stage = (
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_internal_1.0_{date.today()}.parquet"
    )
    output.check_safe_write(output_file_tidyup_stage)

    output_file_tidyup_stage_duplicates = output_file_tidyup_stage.with_name(
        f"{output_file_tidyup_stage.stem}_duplicates.parquet"
    )
    output.check_safe_write(output_file_tidyup_stage_duplicates)

    output_file_engine = (
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_1.0_{date.today()}.parquet"
    )
    output.check_safe_write(output_file_engine)

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
        tmp_dir=tmp_dir,
        is_test_run=args.engine_test_run,
        n_workers=args.engine_n_workers,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
