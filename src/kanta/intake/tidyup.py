"""
Tidy-up the raw data into a subset of necessary column, and apply sorting
and deduplication.


Differences from the WDL implementation
=======================================
- No logging of duplicates/err lines.
- Outputs to a single parquet file, no .txt.gz, as this is very slow.


VM choice and performance
=========================
Best config: 32 CPUs / 32 GB RAM and use 24 buckets. Runs in 2-3 min.

For lower specs, run with 16 or 8 CPUs and allocate 2 GB RAM per CPU, use 24
buckets. Runs in 5-8 min.

Lowest tested working spec: 8 CPUs / 8 GB RAM, 32 buckets. Runs in 6-12 min.

If failing due to OOM in the sort+dedup stage, try increasing the bucket count.

The GCP VM type appears to matter. N2D is about 2x faster than E2.
"""

import tempfile
import shutil
from argparse import ArgumentParser
from datetime import date
from pathlib import Path

import polars as pl


COLUMNS_UNIQUENESS_SORT = [
    "FINNGENID",
    "APPROX_EVENT_DAY",
    "TIME",
    "laboratoriotutkimusnimike",
    "paikallinentutkimusnimike_koodi",
    "tutkimusvastauksentila",
    "tutkimustulosarvo",
    "tutkimustulosyksikko",
]


def main(
    assembled_file: Path,
    phenotype_file: Path,
    output_dir: Path,
    *,
    partition_n_buckets: int,
    keep_intermediate_files: bool,
):
    # Set up output file and temporary directory for intermediate files
    today = date.today()
    output_file = (
        output_dir
        / f"finngen_R14_kanta_laboratory_responses_internal_1.0_{today}.parquet"
    )

    tmp_dir = Path(tempfile.mkdtemp())

    print("# Run info")
    print(f"- Partition into N buckets: {partition_n_buckets}")
    print(f"- Directory for intermediate files: {tmp_dir}")
    print(f"- Output directory: {output_dir}")
    print()

    tmp_file_consolidate = tmp_dir / "consolidated.parquet"

    tmp_dir_partition = tmp_dir / "partition"
    tmp_dir_partition.mkdir()

    tmp_dir_sort_dedup = tmp_dir / "sort_dedup"
    tmp_dir_sort_dedup.mkdir()

    print("# Consolidate")
    consolidated_file = consolidate_columns(assembled_file, tmp_file_consolidate)

    print("# Partition")
    partition(consolidated_file, tmp_dir_partition, partition_n_buckets)

    print("# Sort + Dedup")
    for bucket_file in tmp_dir_partition.glob("bucket_id__*.parquet"):
        (
            pl.scan_parquet(bucket_file)
            .pipe(sort_dedup)
            .sink_parquet(tmp_dir_sort_dedup / bucket_file.name)
        )

    df_pheno = pl.scan_csv(
        phenotype_file,
        infer_schema=False,
        separator="\t",
    ).select("FINNGENID", "SEX")

    print("# Concatenate + Unique + SEX join")
    bucket_files = []
    for bucket_id in range(partition_n_buckets):
        bucket_files.append(tmp_dir_sort_dedup / f"bucket_id__{bucket_id}.parquet")

    (
        pl.scan_parquet(bucket_files)
        # Join SEX
        .join(
            df_pheno,
            left_on="FINNGENID",
            right_on="FINNGENID",
            how="left",
            maintain_order="left",
        )
        .with_row_index(name="_rowid", offset=1)
        .drop("_rowid_consolidate_debug")
        .sink_parquet(output_file)
    )

    if not keep_intermediate_files:
        shutil.rmtree(tmp_dir)


def init_cli():
    parser = ArgumentParser()
    parser.add_argument(
        "--assembled-file",
        help="Path to assembled file from the intake.assemble step (Parquet)",
        required=True,
        type=Path,
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

    return args


def consolidate_columns(assembled_file: Path, output_file: Path) -> Path:
    """Remove unecessary columns form the assembled file and rename the ones we will keep."""
    columns = {
        "main.FINNGENID": "FINNGENID",
        "main.EVENT_AGE": "EVENT_AGE",
        "main.tutkimuskoodistonjarjestelma": "tutkimuskoodistonjarjestelma",
        "main.paikallinentutkimusnimike_selite": "paikallinentutkimusnimike_selite",
        "main.tutkimustulosarvo": "tutkimustulosarvo",
        "main.tutkimustulosyksikko": "tutkimustulosyksikko",
        "main.tutkimusvastauksentila": "tutkimusvastauksentila",
        "main.tuloksenpoikkeavuus": "tuloksenpoikkeavuus",
        "main.viitearvoryhma": "viitearvoryhma",
        "main.viitevalialkuarvo": "viitevalialkuarvo",
        "main.viitevalialkuyksikko": "viitevalialkuyksikko",
        "main.viitevaliloppuarvo": "viitevaliloppuarvo",
        "main.viitevaliloppuyksikko": "viitevaliloppuyksikko",
        "freetext.tutkimustulosteksti": "tutkimustulosteksti",
        "main.paikallinentutkimusnimike_koodi": "paikallinentutkimusnimike_koodi",
        "main.laboratoriotutkimusnimike": "laboratoriotutkimusnimike",
        "main.APPROX_EVENT_DAY": "APPROX_EVENT_DAY",
        "main.TIME": "TIME",
    }

    (
        pl.scan_parquet(assembled_file)
        .with_columns(
            (
                pl.col("main._rowid").cast(pl.String)
                + "@"
                + pl.col("main._filename")
                + "|"
                + pl.col("freetext._rowid").cast(pl.String)
                + "@"
                + pl.col("freetext._filename")
            ).alias("_rowid_consolidate_debug")
        )
        .select(pl.col(list(columns.keys()) + ["_rowid_consolidate_debug"]))
        .rename(columns)
        .sink_parquet(output_file)
    )

    return output_file


def partition(assembled_file: Path, tmp_dir: Path, n_buckets):
    for bucket_id in range(n_buckets):
        (
            pl.scan_parquet(assembled_file)
            .filter(pl.col("FINNGENID").hash() % n_buckets == bucket_id)
            .sink_parquet(tmp_dir / f"bucket_id__{bucket_id}.parquet")
        )


def sort_dedup(frame: pl.LazyFrame | pl.DataFrame):
    return (
        frame.sort(by=COLUMNS_UNIQUENESS_SORT)
        # Dedup rows
        # NOTE(Vincent 2026-05-20) The previous implementation (WDL/Python) was
        # doing the dedup on adjacent lines. Here the deduplication is not done
        # explicitely on adjacent lines (since polars `unique` does it on the
        # full data), though the result should be the same.
        .unique(subset=COLUMNS_UNIQUENESS_SORT, keep="first")
    )


if __name__ == "__main__":
    args = init_cli()
    main(args)
