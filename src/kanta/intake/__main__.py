if __name__ == "__main__":
    from argparse import ArgumentParser
    from pathlib import Path

    from kanta import output
    from kanta.intake import assemble, tidyup

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
        "--prefix",
        default=None,
        help="Output names: <prefix>_assembled.parquet and <prefix>.parquet (as the WDL names them; default: the "
        "source files' shared name start, e.g. finngen_R15_kanta_laboratory_responses)",
    )
    parser.add_argument(
        "--partition-n-buckets",
        help="How many buckets to partition the data into to spread the sort+dedup computations.",
        required=False,
        type=int,
        default=24,
    )
    parser.add_argument(
        "--include-ids-file",
        help="Only keep rows whose FINNGENID is in this file (one ID per line, optional FINNGENID header).",
        required=False,
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
    )

    args = parser.parse_args()
    args.source_list_file = assemble.readable_source_list(args.source_list_file)  # a pipe can be read only once
    prefix = args.prefix or assemble.shared_root(args.source_list_file)

    # Setup: the same names as the separate steps (and the WDL) give them
    output_file_assemble_stage = output.check_safe_write(args.output_dir / f"{prefix}_assembled.parquet")
    output_file_tidyup_stage = output.check_safe_write(args.output_dir / f"{prefix}.parquet")
    output.check_safe_write(
        output_file_tidyup_stage.with_name(f"{output_file_tidyup_stage.stem}_duplicates.parquet")
    )

    tmp_dir = output.create_tmp_dir()

    # Assemble stage
    assemble.main(args.source_list_file, output_file_assemble_stage)

    # Tidy-up stage
    tidyup.main(
        output_file_assemble_stage,
        args.phenotype_file,
        output_file_tidyup_stage,
        tmp_dir=tmp_dir,
        partition_n_buckets=args.partition_n_buckets,
        include_ids_file=args.include_ids_file,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
