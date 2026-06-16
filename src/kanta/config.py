# Shared config across the project.


# ╭────────────────────────────────────────────────────────────────────────────╮
# │ COMMON                                                                     │
# ╰────────────────────────────────────────────────────────────────────────────╯
# Renaming of column names (e.g. translation from Finnish) to more descriptive names used in this
# codebase.
RENAME_COLUMNS = {
    "tutkimuskoodistonjarjestelma": "CODING_SYSTEM",
    "paikallinentutkimusnimike_selite": "TEST_NAME_ABBREVIATION",
    "tutkimustulosarvo": "MEASUREMENT_VALUE",
    "tutkimustulosyksikko": "MEASUREMENT_UNIT",
    "tutkimusvastauksentila": "MEASUREMENT_STATUS",
    "tuloksenpoikkeavuus": "TEST_OUTCOME",
    "viitearvoryhma": "REFERENCE_RANGE_GROUP",
    "viitevalialkuarvo": "REFERENCE_RANGE_LOWER_VALUE",
    "viitevalialkuyksikko": "REFERENCE_RANGE_LOWER_UNIT",
    "viitevaliloppuarvo": "REFERENCE_RANGE_UPPER_VALUE",
    "viitevaliloppuyksikko": "REFERENCE_RANGE_UPPER_UNIT",
    "tutkimustulosteksti": "MEASUREMENT_FREE_TEXT",
}


# ╭────────────────────────────────────────────────────────────────────────────╮
# │ ENGINE                                                                     │
# ╰────────────────────────────────────────────────────────────────────────────╯
# Which columns to read from the input file. This is used very early in the pipeline to limit the
# amount of data read, so the column renaming has not been done yet, hence using the original
# column names.
ENGINE_READ_COLUMNS = [
    "FINNGENID",
    "EVENT_AGE",
    "tutkimuskoodistonjarjestelma",
    "paikallinentutkimusnimike_selite",
    "tutkimustulosarvo",
    "tutkimustulosyksikko",
    "tutkimusvastauksentila",
    "tuloksenpoikkeavuus",
    "viitearvoryhma",
    "viitevalialkuarvo",
    "viitevalialkuyksikko",
    "viitevaliloppuarvo",
    "viitevaliloppuyksikko",
    "tutkimustulosteksti",
    "paikallinentutkimusnimike_koodi",
    "laboratoriotutkimusnimike",
    "APPROX_EVENT_DAY",
    "TIME",
    "_rowid",
    "_rowid_source",
    "SEX",
]

# Number of rows per chunk when streaming the input Parquet file.
# The value is independent of the number of CPUs: the memory used by the engine
# is already proportional to the number of workers, so scaling the number of
# rows per chunk by the number of workers would make the memory use scale by
# (N workers × N workers).
ENGINE_N_LINES_PER_CHUNK = 200_000
ENGINE_CHUNKS_FILE_TEMPLATE = "chunk_{index:06d}.parquet"
ENGINE_CHUNKS_FILE_GLOB = "chunk_*.parquet"
