"""
Merges the incoming Kanta Lab data from THL into one coherent file.

Note: needed ~128GB memory to run on R14 data.
"""
import gzip
from argparse import ArgumentParser
from itertools import zip_longest
from pathlib import Path

import polars as pl
pl.Config.set_verbose(True)  


# TODO
# 4. Validate that shared column match
# 5. Post WDL sort-dup: subset columns, join SEX, sort, output unique/duplicates/error rows

EXPECTED_COLUMNS_RESPONSES = [
    "FINNGENID",
    "EVENT_AGE",
    "APPROX_EVENT_DAY",
    "TIME",
    "asiakirjaoid_pseudo",
    "merkintaoid_pseudo",
    "entryoid_pseudo",
    "load_id_pseudo",
    "file_name_pseudo",
    "laboratoriotutkimusoid",
    "laboratoriotutkimusnimike",
    "paikallinentutkimusnimike_koodi",
    "paikallinentutkimusnimike_selite",
    "tutkimuskoodistonjarjestelma",
    "tiedonlahde",
    "tutkimusvastauksentila",
    "tutkimustulosarvo",
    "tutkimustulosyksikko",
    "tutkimuksennaytelaatu",
    "tutkimuksentekotapa",
    "tuloksenpoikkeavuus",
    "viitearvoryhma",
    "viitevalialkuarvo",
    "viitevalialkuyksikko",
    "viitevaliloppuarvo",
    "viitevaliloppuyksikko",
]

EXPECTED_COLUMNS_FREETEXT = [
    "FINNGENID",
    "EVENT_AGE",
    "APPROX_EVENT_DAY",
    "TIME",
    "asiakirjaoid_pseudo",
    "merkintaoid_pseudo",
    "entryoid_pseudo",
    "load_id_pseudo",
    "file_name_pseudo",
    "tutkimustulosteksti",
]


def validate_input_pairs(list_file: Path, *, separator="\t") -> list[tuple[Path, Path]]:
    pairs = []
    with open(list_file) as fp:
        for line in fp:
            values = line.split(separator, maxsplit=2)

            responses = validate_tsv_gz(values[0], list_file.parent)
            freetext = validate_tsv_gz(values[1], list_file.parent)

            pairs.append((responses, freetext))

    for responses, freetext in pairs:
        check_columns(responses, EXPECTED_COLUMNS_RESPONSES, "responses")
        check_columns(freetext, EXPECTED_COLUMNS_FREETEXT, "freetext")

    return pairs


def merge_by_pair(pairs: list[tuple[Path, Path]], parquet_output: str | Path) -> None:
    to_concat = []
    for path_responses, path_freetext in pairs:
        print(f"Processing {path_responses} & {path_freetext}")

        df_resp = (
            pl.scan_csv(path_responses, infer_schema=False, separator="\t")
            .with_row_index(name="_rn", offset=1)
        )

        df_freetext = (
            pl.scan_csv(path_freetext, infer_schema=False, separator="\t")
            .with_row_index(name="_rn", offset=1)
        )

        df_merged = df_resp.join(df_freetext, on="_rn", how="full")
        to_concat.append(df_merged)

    pl.concat(to_concat).sink_parquet(parquet_output)


def validate_tsv_gz(filename: str, in_dir: Path) -> Path:
    """Check if path exists and is a proper TSV & gz"""
    full_path = (in_dir / filename.strip()).resolve()

    if not full_path.exists():
        raise FileNotFoundError(f"File does not exist: {full_path}")

    # Check it's readable as a gzip file
    try:
        with gzip.open(full_path, "rt", encoding="utf-8") as ff:
            first_line = ff.readline()
    except OSError as ee:
        raise ValueError(f"File is not a valid gzip: {full_path}") from ee

    # Check it's actual TSV
    if "\t" not in first_line:
        raise ValueError(
            f"File does not appear to be TSV (no \\t on first line): {full_path}"
        )

    return full_path


def check_columns(file_path: Path, expected_columns: list[str], label: str) -> None:
    actual_columns = get_columns(file_path)

    if actual_columns != expected_columns:
        if len(actual_columns) == 0:
            raise Exception(f"No columns in {file_path}")

        if len(expected_columns) == 0:
            raise Exception(
                f"Misconfigured expected columns ({label}): no columns listed"
            )

        if set(actual_columns) != set(expected_columns):
            message = f"Columns differ for {label}:\n"
            message += f"Only in expected columns: {list(set(expected_columns) - set(actual_columns))}\n"
            message += f"Only in actual columns: {list(set(actual_columns) - set(expected_columns))}"
            raise Exception(message)

        # Else it's the same columns but in different order
        message = "Column order differ:\n"
        for col_expected, col_actual in zip_longest(expected_columns, actual_columns):
            comp = "==" if col_expected == col_actual else "=!=/!\\=!="
            message += f"{col_expected} {comp} {col_actual}\n"
        raise Exception(message)


def get_columns(input_path: Path) -> list[str]:
    # We checked that the file is a proper TSV gz beforehand, so we now explicitely specify the separator
    df = pl.read_csv(
        input_path, has_header=True, separator="\t", infer_schema=False, n_rows=0
    )
    return df.columns


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--list-file",
        required=True,
        type=Path,
        help="File containing pair of paths to responses & freetext data, one pair per line (TSV without header).",
    )

    args = parser.parse_args()

    pairs = validate_input_pairs(args.list_file)

    merge_by_pair(pairs, "/tmp/out.parquet")
