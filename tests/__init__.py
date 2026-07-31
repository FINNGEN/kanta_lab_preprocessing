import datetime
import gzip
import json
import shutil
from pathlib import Path

import polars as pl

from kanta import config


def fill_templates_with_mocks(
    filler_main: list[dict],
    filler_freetext: list[dict],
    filler_pheno_sex: list[dict],
    tmp_dir: Path,
) -> tuple[Path, Path, Path]:
    default_age = "12.34"
    default_day = "2000-01-23"
    default_time = "01:23"

    template_main = {
        "FINNGENID": None,
        "EVENT_AGE": default_age,
        "APPROX_EVENT_DAY": default_day,
        "TIME": default_time,
        "asiakirjaoid_pseudo": "NA",
        "merkintaoid_pseudo": "NA",
        "entryoid_pseudo": "NA",
        "load_id_pseudo": "NA",
        "file_name_pseudo": "NA",
        "laboratoriotutkimusoid": "NA",
        "laboratoriotutkimusnimike": "NA",
        "paikallinentutkimusnimike_koodi": "NA",
        "paikallinentutkimusnimike_selite": "NA",
        "tutkimuskoodistonjarjestelma": "NA",
        "tiedonlahde": "NA",
        "tutkimusvastauksentila": "NA",
        "tutkimustulosarvo": "NA",
        "tutkimustulosyksikko": "NA",
        "tutkimuksennaytelaatu": "NA",
        "tutkimuksentekotapa": "NA",
        "tuloksenpoikkeavuus": "NA",
        "viitearvoryhma": "NA",
        "viitevalialkuarvo": "NA",
        "viitevalialkuyksikko": "NA",
        "viitevaliloppuarvo": "NA",
        "viitevaliloppuyksikko": "NA",
    }

    template_freetext = {
        "FINNGENID": None,
        "EVENT_AGE": default_age,
        "APPROX_EVENT_DAY": default_day,
        "TIME": default_time,
        "asiakirjaoid_pseudo": "NA",
        "merkintaoid_pseudo": "NA",
        "entryoid_pseudo": "NA",
        "load_id_pseudo": "NA",
        "file_name_pseudo": "NA",
        "tutkimustulosteksti": None,
    }

    # NOTE(Vincent 2026-08-14) Inputs are TSV files which are untyped strings, so make sure we are
    # not setting typed-values here.
    check_values_all_strings(filler_main, "main")
    check_values_all_strings(filler_freetext, "freetext")
    check_values_all_strings(filler_pheno_sex, "pheno_sex")

    template_pheno_sex = {"FINNGENID": None, "SEX": None}

    lines_main = fill_template(template_main, filler_main, "main")
    lines_freetext = fill_template(template_freetext, filler_freetext, "freetext")
    lines_pheno_sex = fill_template(template_pheno_sex, filler_pheno_sex, "pheno_sex")

    path_main = tmp_dir / "main.txt.gz"
    path_freetext = tmp_dir / "freetext.txt.gz"
    path_pheno_sex = tmp_dir / "pheno_sex.txt.gz"

    lines_to_gzip(lines_main, path_main)
    lines_to_gzip(lines_freetext, path_freetext)
    lines_to_gzip(lines_pheno_sex, path_pheno_sex)

    return path_main, path_freetext, path_pheno_sex


def check_values_all_strings(filler: list[dict], filler_name: str):
    errors = []
    for ii, row in enumerate(filler):
        for col, val in row.items():
            if not isinstance(val, str):
                errors.append(f" {filler_name} [{ii}]: value for {col} is not a string (is {type(val)})")

    assert not errors, (
        "All values must be strings, but some are not:\n\n" +
        "\n".join(errors)
    )


def fill_template(template: dict, filler: list[dict], filler_name: str):
    lines = []

    for ii, fill in enumerate(filler):
        new_line = template | fill
        missing_vals = [col for col, val in new_line.items() if not val]
        assert not missing_vals, (
            f"Missing values for this required columns in {filler_name}[{ii}]: {missing_vals}"
        )
        lines.append(new_line)

    return lines


def lines_to_gzip(filled_lines: list[dict], output_path: str | Path):
    (
        pl.DataFrame(filled_lines).write_csv(
            file=output_path, separator="\t", compression="gzip"
        )
    )


def gzip_file(existing_file: Path, output_dir: Path) -> Path:
    output_file = output_dir / f"{existing_file.name}.gz"

    with open(existing_file, "rb") as f_in, gzip.open(output_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    return output_file


def generate_source_list(
    main_file: Path, freetext_file: Path, output_dir: Path
) -> Path:
    source_list_file = Path(output_dir) / "source_list.tsv"

    with open(source_list_file, "x") as ff:
        ff.write(f"{main_file}\t{freetext_file}\n")

    return source_list_file


def parquet_to_ppjson(file: Path) -> Path:
    output_ppjson = file.with_suffix(".json")

    rows = read_parquet(file)
    with open(output_ppjson, "x") as ff:
        json.dump(
            rows,
            ff,
            indent=2,
            # Convert non-native JSON types to strings
            default=to_json_compatible
        )

    return output_ppjson


def to_json_compatible(val) -> str:
    if isinstance(val, datetime.datetime):
        return val.strftime(config.DATE_TIME_FORMAT)
    return str(val)


def read_parquet(file: Path) -> list[dict]:
    return pl.read_parquet(file).to_dicts()
