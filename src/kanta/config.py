# Shared config across the project.

# ╭────────────────────────────────────────────────────────────────────────────╮
# │ ENGINE                                                                     │
# ╰────────────────────────────────────────────────────────────────────────────╯
# Which columns to read
# - (key) from the input file (output of intake stage)
# - (value) to its renamed name during processing
ENGINE_INPUT_COLUMNS_MAPPING = {
    #
    "FINNGENID": "FINNGENID",
    "EVENT_AGE": "EVENT_AGE",
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
    "paikallinentutkimusnimike_koodi": "paikallinentutkimusnimike_koodi",
    "laboratoriotutkimusnimike": "laboratoriotutkimusnimike",
    "APPROX_EVENT_DAY": "APPROX_EVENT_DAY",
    "TIME": "TIME",
    "_rowid": "_rowid",
    "_rowid_source": "_rowid_source",
    "SEX": "SEX",
}

# Number of rows per chunk when streaming the input Parquet file.
ENGINE_CHUNK_N_LINES = 1_000_000
