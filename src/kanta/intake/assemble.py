"""
Merges the incoming Kanta Lab data from THL into one coherent file.


Differences from the WDL implementation
=======================================
- Uses CSV-aware parsing, robust to edge cases like new-line character inside
  CSV values.
"""

import gzip
import os
import tempfile
from argparse import ArgumentParser
from itertools import zip_longest
from pathlib import Path

import polars as pl

from kanta import output

EXPECTED_COLUMNS_MAIN = [
    "FINNGENID",
    "EVENT_AGE",
    "APPROX_EVENT_DAY",
    "TIME",
    "asiakirjaoid",
    "merkintaoid",
    "entryoid",
    "load_id",
    "file_name",
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
    "palvelutuottaja_organisaatio",
]

EXPECTED_COLUMNS_FREETEXT = [
    "FINNGENID",
    "EVENT_AGE",
    "APPROX_EVENT_DAY",
    "TIME",
    "asiakirjaoid",
    "merkintaoid",
    "entryoid",
    "load_id",
    "file_name",
    "tutkimustulosteksti",
    "viitevaliteksti",
]

COL_PREFIX_MAIN = "main."
COL_PREFIX_FREETEXT = "freetext."


def main(source_list_file: Path, output_file: Path) -> None:
    print()
    print("=== ASSEMBLE STAGE ===")
    pairs = validate_input_pairs(source_list_file)

    print("# Merge by pair")
    merge_by_pair(pairs, output_file)

    print("# Checking merge consistency")
    is_consistent = check_merge_consistency(output_file)
    print("All good." if is_consistent else "!!! Inconsistent merge !!!")


def readable_source_list(source_list_file: Path) -> Path:
    """The source list as a regular file that can be read more than once. A pipe (e.g. <(head -n2 list.tsv)) can be
    read only once, so it is copied to a temporary file, with relative paths made absolute (from the current folder)."""
    source_list_file = Path(source_list_file)
    if source_list_file.is_file():
        return source_list_file
    lines = [
        "\t".join(str(Path(p.strip()).resolve()) if p.strip() else p for p in line.split("\t"))
        for line in source_list_file.read_text().splitlines()
    ]
    with tempfile.NamedTemporaryFile("w", suffix=".tsv", delete=False) as tmp:
        tmp.write("\n".join(lines) + "\n")
    return Path(tmp.name)


def shared_root(source_list_file: Path) -> str:
    """The start the source files' names all share, without trailing separators, e.g.
    finngen_R15_kanta_laboratory_responses for finngen_R15_kanta_laboratory_responses_2014_internal_1.0.txt.gz and
    finngen_R15_kanta_laboratory_responses_freetext_2014_internal_1.0.txt.gz. Names the outputs by default."""
    names = [
        Path(path.strip()).name
        for line in Path(source_list_file).read_text().splitlines() if line.strip()
        for path in line.split("\t")
    ]
    if not names:
        raise ValueError(f"{source_list_file}: no source files listed")
    root = os.path.commonprefix(names).rstrip("_.-")
    if not root:
        raise ValueError(f"{source_list_file}: the source files share no common name start; give the output name")
    return root


def default_output_file(source_list_file: Path) -> Path:
    """<shared root>_assembled.parquet, in the current folder."""
    return Path(f"{shared_root(source_list_file)}_assembled.parquet")


def validate_input_pairs(
    source_list_file: Path, *, separator="\t"
) -> list[tuple[Path, Path]]:
    pairs = []
    with open(source_list_file) as fp:
        for line_number, line in enumerate(fp, start=1):
            # Skip blank lines (e.g. a trailing newline at end of file).
            if not line.strip():
                continue

            values = line.split(separator, maxsplit=2)
            if len(values) != 2:
                raise ValueError(
                    f"Malformed source-list line {line_number} in {source_list_file}: "
                    f"expected a main and a freetext path separated by {separator!r}, "
                    f"got: {line.strip()!r}"
                )

            main = validate_tsv_gz(values[0], source_list_file.parent)
            freetext = validate_tsv_gz(values[1], source_list_file.parent)

            pairs.append((main, freetext))

    if not pairs:
        raise ValueError(f"{source_list_file}: no main / freetext pairs (an empty file, or a pipe already read)")

    for main, freetext in pairs:
        check_columns(main, EXPECTED_COLUMNS_MAIN, "main")
        check_columns(freetext, EXPECTED_COLUMNS_FREETEXT, "freetext")

    return pairs


def merge_by_pair(pairs: list[tuple[Path, Path]], parquet_output: str | Path) -> None:
    to_concat = []
    for path_main, path_freetext in pairs:
        print(f"Adding horizontal merge: {path_main} & {path_freetext}")

        df_main = (
            pl.scan_csv(
                path_main,
                infer_schema=False,
                separator="\t",
                row_index_name="_rowid",
                row_index_offset=1,
            )
            .with_columns(pl.lit(path_main.name).alias("_filename"))
            .select(pl.all().name.prefix(COL_PREFIX_MAIN))
        )

        df_freetext = (
            pl.scan_csv(
                path_freetext,
                infer_schema=False,
                separator="\t",
                row_index_name="_rowid",
                row_index_offset=1,
            )
            .with_columns(pl.lit(path_freetext.name).alias("_filename"))
            .select(pl.all().name.prefix(COL_PREFIX_FREETEXT))
        )

        df_merged = pl.concat([df_main, df_freetext], how="horizontal")

        to_concat.append(df_merged)

    (
        pl.concat(to_concat)
        .with_row_index(name="_rowid_source", offset=1)
        .pipe(reorder_columns)
        .sink_parquet(parquet_output)
    )


def reorder_columns(frame: pl.LazyFrame | pl.DataFrame) -> pl.LazyFrame | pl.DataFrame:
    column_order = (
        ["_rowid_source"]
        # Columns for main
        + [COL_PREFIX_MAIN + "_rowid", COL_PREFIX_MAIN + "_filename"]
        + [COL_PREFIX_MAIN + cc for cc in EXPECTED_COLUMNS_MAIN]
        # Columns for freetext
        + [COL_PREFIX_FREETEXT + "_rowid", COL_PREFIX_FREETEXT + "_filename"]
        + [COL_PREFIX_FREETEXT + cc for cc in EXPECTED_COLUMNS_FREETEXT]
    )
    return frame.select(column_order)


def check_merge_consistency(data_path: str | Path) -> bool:
    # First check: all shared columns have the same values
    shared_cols = set(EXPECTED_COLUMNS_MAIN).intersection(EXPECTED_COLUMNS_FREETEXT)

    check_shared_columns_same_values = (
        pl.scan_parquet(data_path)
        .select(
            pl.all_horizontal(
                pl.col(COL_PREFIX_MAIN + cc) == pl.col(COL_PREFIX_FREETEXT + cc)
                for cc in shared_cols
            ).all()
        )
        .collect(engine="streaming")
        .item()
    )

    assert check_shared_columns_same_values

    # Second check: main and freetext have same height.
    # This is done by checking the absence of null in _rowid, which happens iif
    # the main and freetext data are of different height.
    check_same_height = (
        pl.scan_parquet(data_path)
        .select(
            pl.all_horizontal(pl.selectors.ends_with("._rowid").is_not_null().all())
        )
        .collect(engine="streaming")
        .item()
    )

    assert check_same_height

    return check_shared_columns_same_values and check_same_height


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
            raise ValueError(f"No columns in {file_path}")

        if len(expected_columns) == 0:
            raise ValueError(
                f"Misconfigured expected columns ({label}): no columns listed"
            )

        if set(actual_columns) != set(expected_columns):
            message = f"Columns differ for {label}:\n"
            message += f"Only in expected columns: {list(set(expected_columns) - set(actual_columns))}\n"
            message += f"Only in actual columns: {list(set(actual_columns) - set(expected_columns))}"
            raise ValueError(message)

        # Else it's the same columns but in different order
        message = "Column order differ:\n"
        for col_expected, col_actual in zip_longest(expected_columns, actual_columns):
            comp = "==" if col_expected == col_actual else "=!=/!\\=!="
            message += f"{col_expected} {comp} {col_actual}\n"
        raise ValueError(message)


def get_columns(input_path: Path) -> list[str]:
    # We checked that the file is a proper TSV gz beforehand, so we now explicitely specify the separator
    df = pl.read_csv(
        input_path, has_header=True, separator="\t", infer_schema=False, n_rows=0
    )
    return df.columns


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "--source-list-file",
        required=True,
        type=Path,
        help="File containing pair of paths to main & freetext data, one pair per line (TSV without header).",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        default=None,
        help="Path to output the intermediary file from this stage "
        "(default: <the source files' shared name start>_assembled.parquet, in the current folder).",
    )

    args = parser.parse_args()
    args.source_list_file = readable_source_list(args.source_list_file)
    args.output_file = args.output_file or default_output_file(args.source_list_file)

    output.check_safe_write(args.output_file)

    main(args.source_list_file, args.output_file)
