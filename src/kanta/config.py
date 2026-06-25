# Shared config across the project.

# Aliases mapping column name from input (dict keys) to a name used in the code (dict values).
# The purpose is to expose easier column names that can be referenced in the data processing code.
# For more column names, check: https://github.com/FINNGEN/Kanta_lab_QC#column-description
COLUMN_ALIASES = {
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
