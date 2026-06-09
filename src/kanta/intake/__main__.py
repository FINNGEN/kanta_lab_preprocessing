if __name__ == "__main__":
    from argparse import ArgumentParser
    from datetime import date
    from pathlib import Path

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
        "--debug",
        help="Increase verbosity and keep intermediate files",
        required=False,
        action="store_true",
    )

    args = parser.parse_args()

    # Assemble stage
    output_file_assemble_stage = (
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses.assemble-stage.{date.today()}.parquet"
    )
    post_assemble_file = assemble.main(
        args.source_list_file, output_file_assemble_stage
    )

    # Tidy-up stage
    output_file_tidyup_stage = (
        args.output_dir
        / f"finngen_R14_kanta_laboratory_responses_internal_1.0_{date.today()}.parquet"
    )
    tidyup.main(
        output_file_assemble_stage,
        args.phenotype_file,
        output_file_tidyup_stage,
        partition_n_buckets=args.partition_n_buckets,
        keep_intermediate_files=args.debug,
    )
