# Shared config across the project.

from pathlib import Path

# Directory for static reference/mapping data files used by the engine's filters.
DATA_DIR = Path(__file__).parent / "engine" / "data"

# Which columns to read from the input file, using the original (pre-alias) column names.
# An empty list means all columns are read.
READ_COLUMNS = []

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
    "tutkimuksentekotapa":"MEASUREMENT_METHOD",
    "tutkimustulosteksti": "MEASUREMENT_FREE_TEXT",
    "_rowid": "ROWID",
}

# Which columns to keep in the output, using post-alias (renamed) column names.
# An empty list means all columns are kept.
OUT_COLUMNS = [
    "FINNGENID",
    "APPROX_EVENT_DATETIME",
    "EVENT_AGE",
    "paikallinentutkimusnimike_koodi",
    "laboratoriotutkimusnimike",
    "TEST_ID",
    "TEST_ID_IS_NATIONAL",
    "TEST_NAME_ABBREVIATION",
    "MEASUREMENT_VALUE",
    "MEASUREMENT_UNIT",
    "MEASUREMENT_STATUS",
    "MEASUREMENT_METHOD",
    "TEST_OUTCOME",
    "CODING_SYSTEM",
    "CODING_SYSTEM_MAP",
    "MEASUREMENT_FREE_TEXT",
    "ROWID",
    "_rowid_source",
    "SEX",
]

# Format used to parse APPROX_EVENT_DATETIME (APPROX_EVENT_DAY + "T" + TIME).
DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M"

# Columns of the error table that filters append to when they drop/flag rows.
# ERR is a fixed tag naming which check produced the row (e.g. "DATE"), and
# ERR_VALUE is the specific offending value for that check.
ERR_COLUMNS = ["ROWID", "_rowid_source", "ERR", "ERR_VALUE"]

# Columns of the abbreviation-change table that fix_abbreviation appends to. Kept as
# separate OLD_ABBR/NEW_ABBR columns rather than a single combined value.
ABBR_COLUMNS = ["ROWID", "_rowid_source", "ERR", "OLD_ABBR", "NEW_ABBR"]

# Columns to leave untouched when stripping whitespace (e.g. free text, where spaces are
# meaningful), using post-alias (renamed) column names.
COLUMNS_WITH_SPACES = ["MEASUREMENT_FREE_TEXT"]

# Keyword tokens that mean "missing" and get normalized to the literal "NA" string.
# Applies to all columns by default; see NA_KEYWORDS_OVERRIDES for column-specific lists.
NA_KEYWORDS = ["Puuttuu", '""', "TYHJÄ", "_", "NULL", "-1"]

# Per-column override of NA_KEYWORDS, using post-alias (renamed) column names.
# MEASUREMENT_VALUE excludes "-1" since it can be a legitimate measurement value there,
# unlike everywhere else where it signals missingness.
NA_KEYWORDS_OVERRIDES = {
    "MEASUREMENT_VALUE": ["Puuttuu", '""', "TYHJÄ", "_", "NULL"],
}

# MEASUREMENT_STATUS codes considered too problematic to keep the row (AR/LABRA exception
# message classification).
PROBLEMATIC_MEASUREMENT_STATUS = ["K", "W", "X", "I", "D", "P"]

# National (THL) lab id -> abbreviation mapping (2-column TSV, header "CodeId\tAbbreviation").
THL_LAB_ID_ABBREVIATION_FILE = DATA_DIR / "thl_lab_id_abbrv_map.tsv"

# tutkimuksentekotapa (MEASUREMENT_METHOD) codes -> short English label.
# Source: THL codeserver classification (CodeId;Abbreviation columns).
MEASUREMENT_METHOD_MAP = {
    "1": "LAB",
    "2": "POC",
    "3": "SELF",
}

# National (THL) organization id -> lab/organization name mapping (gzipped TSV with an
# OrganizationId and a LAB_NAME column, among others).
THL_SOTE_MAP_FILE = DATA_DIR / "thl_sote_map_named.tsv.gz"

# Manual mapping from a short numeric code (derived from CODING_SYSTEM) to a coding system
# name, for cases not covered by THL_SOTE_MAP_FILE. TSV with a CODE/NAME header (plus COUNT/
# SOURCE columns from coding_map.py that this loader ignores).
THL_CODING_MANUAL_MAP_FILE = DATA_DIR / "thl_coding_manual_mapping.txt"

# Regex fragments (joined with "|") for characters/patterns stripped from
# TEST_NAME_ABBREVIATION.
ABBREVIATION_DELETION_PATTERNS = [
    r"_|\*|#|%",
    r"^\d{4},",
    r",\d{4}$",
]

# (pattern, replacement) pairs applied to TEST_NAME_ABBREVIATION after deletions.
ABBREVIATION_REPLACEMENTS = [
    ("–", "-"),
]
