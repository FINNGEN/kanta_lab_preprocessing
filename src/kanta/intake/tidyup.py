import tempfile
import shutil
from argparse import ArgumentParser
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


def main(args):
    temp_dir = Path(tempfile.mkdtemp())
    print(f">> {temp_dir=}")

    temp_dir_partition = temp_dir / "partition"
    temp_dir_partition.mkdir()

    temp_dir_tidyup = temp_dir / "tidyup"
    temp_dir_tidyup.mkdir()

    print("# Consolidate")
    consolidated_file = consolidate_columns(args.assembled_file, args.output_dir)

    print("# Partition")
    partition(consolidated_file, temp_dir_partition, args.partition_n_buckets)

    print("# Tidy-up")
    for bucket_file in temp_dir_partition.glob("bucket_id__*.parquet"):
        (
            pl.scan_parquet(bucket_file)
            .pipe(tidy_up)
            .sink_parquet(temp_dir_tidyup / bucket_file.name)
        )

    df_pheno = pl.scan_csv(
        args.phenotype_file,
        infer_schema=False,
        separator="\t",
    ).select("FINNGENID", "SEX")

    print("# Concatenate + Unique + SEX join")
    bucket_files = []
    for bucket_id in range(args.partition_n_buckets):
        bucket_files.append(temp_dir_tidyup / f"bucket_id__{bucket_id}.parquet")

    (
        # TODO: verify the file order of `bucket_files` is kept
        pl.scan_parquet(bucket_files)
        # Join SEX
        .join(
            df_pheno,
            left_on="FINNGENID",
            right_on="FINNGENID",
            how="left",
            maintain_order="left",
        )
        .sink_parquet("/tmp/out.parquet")
    )

    # TODO validation
    #

    if not args.keep_intermediate_files:
        shutil.rmtree(temp_dir)
    
    print("<< end")


def consolidate_columns(assembled_file: Path, output_dir: Path) -> Path:
    """Remove unecessary columns form the assembled file and rename the ones we will keep."""
    output_file = output_dir / "consolidated.parquet"

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
            ).alias("_rowid")
        )
        .select(pl.col(list(columns.keys()) + ["_rowid"]))
        .rename(columns)
        .sink_parquet(output_file)
    )

    return output_file


def partition(assembled_file: Path, temp_dir: Path, n_buckets):
    for bucket_id in range(n_buckets):
        (
            pl.scan_parquet(assembled_file)
            .filter(pl.col("FINNGENID").hash() % n_buckets == bucket_id)
            .sink_parquet(temp_dir / f"bucket_id__{bucket_id}.parquet")
        )


def tidy_up(frame: pl.LazyFrame | pl.DataFrame):
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
    parser = ArgumentParser()
    parser.add_argument(
        "--assembled-file",
        help="Path to assembled file from the intake.assemble step (Parquet)",
        required=True,
        type=Path,
    )
    parser.add_argument(
        "--phenotype-file",
        help="Path to phenotype file with SEX column (.txt.gz)",
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
        help="How many buckets to partition the data into to spread the sort+unique computations.",
        required=False,
        type=int,
        default=32
    )
    parser.add_argument(
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
    )
    args = parser.parse_args()

    main(args)
