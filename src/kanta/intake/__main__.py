if __name__ == "__main__":
    from argparse import ArgumentParser
    from datetime import date
    from pathlib import Path

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
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
    )

    args = parser.parse_args()

    # Setup
    output_file_assemble_stage = (
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses.assemble-stage.{date.today()}.parquet"
    )
    output_file_tidyup_stage = (
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_internal_1.0_{date.today()}.parquet"
    )
    output.check_safe_write(output_file_assemble_stage)
    output.check_safe_write(output_file_tidyup_stage)

    tmp_dir = output.create_tmp_dir()

    # Assemble stage
    assemble.main(
        args.source_list_file, output_file_assemble_stage
    )

    # Tidy-up stage
    tidyup.main(
        output_file_assemble_stage,
        args.phenotype_file,
        output_file_tidyup_stage,
        tmp_dir=tmp_dir,
        partition_n_buckets=args.partition_n_buckets,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
