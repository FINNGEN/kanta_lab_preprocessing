if __name__ == "__main__":
    import tempfile
    import os
    from argparse import ArgumentParser
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
    _fd, absolute_pathname = tempfile.mkstemp()
    tmp_file_assemble = Path(absolute_pathname)
    post_assemble_file = assemble.main(args.source_list_file, tmp_file_assemble)

    # Tidy-up stage
    tidyup.main(
        tmp_file_assemble,
        args.phenotype_file,
        args.output_dir,
        partition_n_buckets=args.partition_n_buckets,
        keep_intermediate_files=args.debug,
    )

    # Cleaning up
    if not args.debug:
        os.remove(tmp_file_assemble)
