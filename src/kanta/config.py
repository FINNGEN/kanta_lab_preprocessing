# Shared config across the project.

from pathlib import Path

# Directory for static reference/mapping data files used by the engine's filters.
DATA_DIR = Path(__file__).parent / "engine" / "data"

# Output of the separate unit-injection pipeline (scripts/injection/), referenced in place
# rather than copied into DATA_DIR, since it's this repo's own output, not an external source.
REPO_ROOT = Path(__file__).parent.parent.parent
INJECTION_RESULTS_FILE = REPO_ROOT / "scripts" / "injection" / "data" / "injection_results.tsv"

# Test-specific unit corrections (e.g. osuus -> ratio for b-hkr), built by
# scripts/injection/build_omop_injection_table.py from the legacy finngen_qc abbreviation-fix
# table, filtered down to entries not already superseded by INJECTION_RESULTS_FILE.
OMOP_INJECTION_FILE = DATA_DIR / "omop_injection.tsv"

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
    "source::MEASUREMENT_VALUE",
    "source::MEASUREMENT_UNIT",
    "source::TEST_NAME_ABBREVIATION",
    "source::TEST_OUTCOME",
    "harmonization_omop::IS_UNIT_VALID",
    "harmonization_omop::OMOP_ID",
    "harmonization_omop::OMOP_QUANTITY",
    "harmonization_omop::MEASUREMENT_VALUE",
    "harmonization_omop::MEASUREMENT_UNIT",
    "harmonization_omop::CONVERSION_FACTOR",
    "extracted::IS_POS",
    "extracted::TEST_OUTCOME_TEXT",
    "imputed::TEST_OUTCOME",
    "IS_VALUE_EXTRACTED",
    "IS_UNIT_EXTRACTED",
    "cleaned-pre-fix::MEASUREMENT_UNIT",
    "QC_NOTES",
    "QC_PASS",
]

# Columns snapshotted into a source::<col> output column immediately after renaming and
# before any filter modifies them, so the raw pre-cleaning value survives in the output.
SOURCE_COLUMNS = ["MEASUREMENT_VALUE", "MEASUREMENT_UNIT", "TEST_NAME_ABBREVIATION",'TEST_OUTCOME']

# Format used to parse APPROX_EVENT_DATETIME (APPROX_EVENT_DAY + "T" + TIME).
DATE_TIME_FORMAT = "%Y-%m-%dT%H:%M"

# Columns of the error table that filters append to when they drop/flag rows.
# ERR is a fixed tag naming which check produced the row (e.g. "DATE"), and
# ERR_VALUE is the specific offending value for that check.
ERR_COLUMNS = ["ROWID", "_rowid_source", "ERR", "ERR_VALUE"]

# Columns of the abbreviation-change table that fix_abbreviation appends to. Kept as
# separate OLD_ABBR/NEW_ABBR columns rather than a single combined value.
ABBR_COLUMNS = ["ROWID", "_rowid_source", "ERR", "OLD_ABBR", "NEW_ABBR"]

# Columns of the unit-change table that fix_measurement_unit/extract_measurement append to.
# Kept as separate OLD_UNIT/NEW_UNIT columns rather than a single combined value.
UNIT_COLUMNS = ["ROWID", "_rowid_source", "TEST_NAME_ABBREVIATION", "ERR", "OLD_UNIT", "NEW_UNIT"]

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

# National (THL) lab id -> abbreviation mapping. Raw THL codeserver export: ";"-delimited,
# ISO-8859-1 encoded, with a "CodeId;Abbreviation;..." header among other columns we don't use.
THL_LAB_ID_ABBREVIATION_FILE = DATA_DIR / "thl_lab_id_abbrv_map.txt"

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

# Manually curated MEASUREMENT_UNIT correction table: raw/dirty unit string -> corrected
# unit. TSV with an OLD_UNIT/MEASUREMENT_UNIT header (plus a COUNT column this loader
# ignores).
UNIT_MAP_FILE = DATA_DIR / "unit_mapping.txt"

# Stray characters stripped from MEASUREMENT_UNIT before mapping/regex fixes.
UNIT_STRIP_CHARS = [" ", "_", ",", ".", "-", "(", ")", "{", "}", "\\", "?", "!"]

# Ordered (pattern, replacement) regex pairs applied to whatever MEASUREMENT_UNIT values
# UNIT_MAP_FILE didn't already resolve. Order matters: earlier patterns can create the
# text later patterns match on.
UNIT_REPLACEMENTS = [
    (r"(^\*+$|^$)", "NA"),
    (r"\bc\b", "°c"),
    (r"(^(\b)?\d+(?=e\d+))", ""),
    (r"(à?x?(10)?e0?(?=\d)|x?10(\^|\*)|^\^(?=[0-9]+.?l))", "e"),
    (r"(y|µ)ks(ikkö)?", "u"),
    (r"y", "u"),
    (r"lµ", "ly"),
    (r"tehtµ", "tehty"),
    (r"µg", "ug"),
    (r"m([a-z]?)µ", "mu"),
    (r"^mµ.?l$", "mu/l"),
    (r"^µ.?l$", "u/l"),
    (r"^u.?l$", "u/l"),
    (r"µmol", "umol"),
    (r"^µmol.?l$", "umol/l"),
    (r"^(µ|u)g.?l$", "ug/l"),
    (r"^(m)?mmo(l)?/", "mmol/"),
    (r"(mo(t|l|i)?(l)?)(?=$)|nol", "mol"),
    (r"^mmol.?(l|i).?$", "mmol/l"),
    (r"krea", ""),
    (r"^mmol.?mol.?$", "mmol/mol"),
    (r"(^(m)?m(h)?/h$|^mh.?h$)", "mm/h"),
    (r"^.?mg.?l$", "mg/l"),
    (r"^ml/min.*", "ml/min/173m2"),
    (r"^inrarvo$", "inr"),
    (r"^mg/lfeu$", "mg/l"),
    (r"^mo(l)?sm/kg.*$", "mosm/kgh2o"),
    (r"(^tilo(s)?$|^(til)osuu(s)$)", "osuus"),
    (r"(kopio(t)?(a)?|klp|sol(y|µ|u)|sol(y|µ|u)a|pisteet)", "kpl"),
    (r"(n(ä)?kö(ke)?k(enttä|entt)?|s(y|µ)n(fält|f)?$)", "nk"),
    (r"(^(kpla)/nk|^kpl.?nk$|/nk$)", "kpl/nk"),
    (r"^.*ti(i)?t(t)?er(i)?.*$", "titre"),
    (r"^elia(u|µ)", "eliau"),
    (r"^eliau/m$", "eliau/ml"),
    (r"^a(u|µ)/ml$", "au/ml"),
    (r"(gulos(t.*)$|gulo)", "gstool"),
    (r"((u|µ)g/g(\s+)?stool|(u|µ)g/g(f)?)", "ug/g"),
    (r"(^promil(l)?$|^o/oo$)", "promille"),
    (r"(^\-$|^negat$|^neg$)", "N"),
    (r"(^pos$|^\+$)", "A"),
    (r"^p.?g$", "pg"),
    (r"^f.?l$", "fl"),
    (r"\/\/", "/"),
    (r"(c)?aste(c)?", "aste"),
    (r"sek", "s"),
    (r"ve/", "responseequivalent/"),
    (r"^ve$", "responseequivalent"),
    (r"aru", "au"),
    (r"liter", "l"),
    (r"(/d$|/vrk$)", "/24h"),
    (r"nk$", "field"),
    (r"kpl", "u"),
    (r"(lausunto|lomake)", "form"),
    (r"indeksi", "index"),
    (r"arvio", "estimate"),
    (r"suhde", "ratio"),
    (r"krt", "times"),
    (r"/100le(uk)$", "/100leuk"),
    (r"/l(/|)?(4|37c|ph7|ph74)+", "/l"),
    (r"nmol(bce)?/mmol", "nmol/mmol"),
    (r"^ku/l$", "u/ml"),
    (r"^pg/ml$", "ng/l"),
    (r"^(µ|u)g/ml$", "mg/l"),
    (r"(^\s+$|^$)", "NA"),
]

# TEST_OUTCOME codes rewritten to their standard AR/LABRA equivalent. Every other raw
# value (including e.g. "POS"/"NEG") passes through unchanged.
TEST_OUTCOME_MAP = {
    "<": "L",
    ">": "H",
}

# Public repo hosting Usagi (OMOP) harmonization reference tables.
HARMONIZATION_REPO_BRANCH = "kanta_v4"
HARMONIZATION_REPO_URL = (
    "https://raw.githubusercontent.com/FINNGEN/kanta_lab_harmonisation_public/"
    f"refs/heads/{HARMONIZATION_REPO_BRANCH}/MAPPINGS/"
)

# Usagi-approved MEASUREMENT_UNIT source codes, filtered to unique-for-lab units.
# Refreshed from HARMONIZATION_REPO_URL on load; falls back to this local snapshot if offline.
USAGI_UNITS_FILE = DATA_DIR / "UNITSfi.tsv"
USAGI_UNITS_URL = HARMONIZATION_REPO_URL + "UNITSfi.tsv"

# Usagi lab-test mapping table (MAPPING_STATUS/OMOP concept id per TEST_NAME_ABBREVIATION).
# Refreshed from HARMONIZATION_REPO_URL on load; falls back to this local snapshot if offline.
USAGI_MAPPING_FILE = DATA_DIR / "LABfi.tsv"
USAGI_MAPPING_URL = HARMONIZATION_REPO_URL + "LABfi.tsv"

# Target MEASUREMENT_UNIT per OMOP concept (chosen destination unit for harmonization).
HARMONIZATION_COUNTS_FILE = DATA_DIR / "harmonization_counts.tsv"
HARMONIZATION_COUNTS_URL = HARMONIZATION_REPO_URL + "harmonization_counts.tsv"

# Per-OMOP-quantity unit conversion factors (source unit -> target unit).
UNIT_CONVERSION_FILE = DATA_DIR / "quantity_source_unit_conversion.tsv"
UNIT_CONVERSION_URL = HARMONIZATION_REPO_URL + "quantity_source_unit_conversion.tsv"

# Free-text prefixes stripped from the start of MEASUREMENT_FREE_TEXT before numeric extraction.
FREE_TEXT_RESULT_STRINGS = [
    "tutkimuksentulos:",
    "resultat:",
    "provresultat:",
    "tutkimuksen tulos:",
    "tulos:",
    "vastaus:",
]

# (pattern, replacement) pairs applied to MEASUREMENT_FREE_TEXT before numeric extraction.
FREE_TEXT_MEASUREMENT_REPLACEMENTS = [
    (r"\*", ""),
    (r",", "."),
]

# Tokens in MEASUREMENT_FREE_TEXT that signal an out-of-range comparison (Finnish "yli"/"alle"
# mean "over"/"under" and get normalized to ">"/"<").
STATUS_INDICATORS = ("<", ">", "yli", "alle")

# Free-text -> pos/neg (extracted::IS_POS) mapping, e.g. "NEGAT" -> "0".
POSNEG_MAP_FILE = DATA_DIR / "negpos_mapping.tsv"

# Per-OMOP_ID abnormality reference range: LOW_LIMIT/HIGH_LIMIT plus LOW_PROBLEM/HIGH_PROBLEM
# flags (whether crossing that limit is itself a problem, e.g. "L*" vs "L").
AB_LIMITS_FILE = DATA_DIR / "abnormality_estimation.table.tsv"

# Per-OMOP_ID QC threshold rules: SIDE ("<"/"<="/">"/"==") + THRESHOLD flags
# harmonization_omop::MEASUREMENT_VALUE as a QC failure, tagged with QC_NOTES (e.g.
# "IMPLAUSIBLE_VALUE"). A row can carry multiple rules; some rows are placeholder
# "register this OMOP_ID as checked" entries with no SIDE/THRESHOLD at all.
OMOP_QC_FILE = DATA_DIR / "omop_qc.tsv"

# (TEST_OUTCOME, extracted::IS_POS) pairs considered a logical conflict (e.g. a categorical
# "Normal" outcome alongside a text-extracted positive result) -> QC_PASS flagged as failed.
OUTCOME_MISMATCH = [("N", "1"), ("A", "0")]
