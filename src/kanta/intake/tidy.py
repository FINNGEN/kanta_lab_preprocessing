# TODO: in assemble: get rid of main. / freetext. prefixes for columns that are in common, since we check they have the same values (right?), then change it here.
from argparse import ArgumentParser
from pathlib import Path

import polars as pl


COLUMNS_OUTPUT = [
    "main.FINNGENID",
    "main.EVENT_AGE",
    "main.tutkimuskoodistonjarjestelma",
    "main.paikallinentutkimusnimike_selite",
    "main.tutkimustulosarvo",
    "main.tutkimustulosyksikko",
    "main.tutkimusvastauksentila",
    "main.tuloksenpoikkeavuus",
    "main.viitearvoryhma",
    "main.viitevalialkuarvo",
    "main.viitevalialkuyksikko",
    "main.viitevaliloppuarvo",
    "main.viitevaliloppuyksikko",
    "freetext.tutkimustulosteksti",
    "main.paikallinentutkimusnimike_koodi",
    "main.laboratoriotutkimusnimike",
    "main.APPROX_EVENT_DAY",
    "main.TIME",
    "main._rowid",
]

COLUMNS_UNIQUENESS_SORT = [
    "main.FINNGENID",
    "main.APPROX_EVENT_DAY",
    "main.TIME",
    "main.laboratoriotutkimusnimike",
    "main.paikallinentutkimusnimike_koodi",
    "main.tutkimusvastauksentila",
    "main.tutkimustulosarvo",
    "main.tutkimustulosyksikko",
]


def main(args):
    df_pheno = pl.scan_csv(
        args.phenotype_file,
        infer_schema=False,
        separator="\t",
    ).select("FINNGENID", "SEX")

    (
        pl.scan_parquet(args.assembled_file)
        .select(COLUMNS_OUTPUT)
        # Dedup rows
        # NOTE(Vincent 2026-05-20) Here the deduplication is done on whole data,
        # not just on adjacent lines as was done in the previous implementation.
        .unique(subset=COLUMNS_UNIQUENESS_SORT)
        # Sort
        .sort(by=COLUMNS_UNIQUENESS_SORT)
        # join SEX
        .join(df_pheno, left_on="main.FINNGENID", right_on="FINNGENID", how="left")
        .sink_parquet(args.output_file)
    )

    # TODO validation


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
        "--output-file",
        help="Path to write the tidied up output file (Parquet)",
        required=True,
        type=Path,
    )
    args = parser.parse_args()

    main(args)
