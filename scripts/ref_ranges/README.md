# ref_ranges

Turns reference-range texts into numeric limits for the engine, then uses the ranges to flag rows whose unit is wrong (the ref-qc stage). The engine fills a row's missing limits from the curated table (`src/kanta/engine/data/ref_text_limits.tsv`) right after the minimal step; see the main README.

## Main (run in order)

1. **`ref_text_limits.py`**: parses each TEST_NAME × reference-text pair into lower/upper limits (plus sex and age when the text gives them). It then checks the limits against the actual values, sexes and ages.
   ```
   python3 ref_text_limits.py --input <engine_output>.parquet [--out DIR] [--min-n 1000] [--test]
   ```
   Writes `ref_text_table.tsv`, `ref_text_table_LIMITS.tsv` (every pair, with `approved` and the limits in use) `ref_text_table_UNDECIDED.tsv` (not approved, a decision could bring them in) and `ref_text_table_REJECTED.tsv` (out for good: `use` = no, or no limit in the text):
   ```
   test_name  ref_text     n       lower  upper  status  sex_text  value_check  flags
   p-krea     50-90 umol/l 120000  50     90     range   F         value_ok
   ```
   Each run is reconciled with the curated table the engine uses (`kanta.config.REF_TEXT_LIMITS_FILE`, the `--curated` default): manual columns carried over, `NEW_ENTRY` / `PARSE_CHANGED` / `NOT_SEEN` marked. It never writes the table: review UNDECIDED, edit the manual columns (`use` yes/no, `lower_override`, `upper_override`, `NOTES`) in the LIMITS output and copy it over the table by hand. A pair is approved when parsed with no failed check, or with `use` = yes or an override; `use` = no keeps it out. The engine recomputes the approval when it loads the table (and refreshes the stored `approved` column), so a hand edit of `use` counts at once. The key (`ref_key`) and the rule (`approve`) are in `kanta.engine.reference_data`, shared with the engine; the script needs the kanta environment (the Docker image has it).

   Run on a `--test` engine output (or with a low `--min-n`) only to check the loop: its checks run on few rows, so don't copy that LIMITS output into the engine.

2. **`ref_units.py`**: for each test × OMOP pair, learns which reference ranges go with which unit and flags rows whose range points to another unit. It reads the limits as the engine filled them (`REFERENCE_RANGE_SOURCE`: num = sent by the lab, text = from the curated table), so it needs no LIMITS file. There are three modes:
   - `cache`: builds the shared cache only
   - `sets`: learns the unit ranges per pair
   - `flag`: runs both of the above if needed, then flags the rows
   ```
   python3 ref_units.py flag results --top 2
   python3 ref_units.py flag results --test b-hkr --omop 3009542
   python3 ref_units.py flag results/b-hkr/3009542        # rerun one pair
   ```
   Writes one folder per pair (`results/<test>/<omop>/`) with `ref_unit_sets.tsv`, `ref_unit_flags.tsv`, `ref_unit_check.png`, …, plus `results/flag_summary.tsv`:
   ```
   test   omop     rows_checked  flagged  pct_flagged  unit_mismatch  unit_missing
   b-hkr  3009542  2500000       1800     0.07         1500           300
   ```
