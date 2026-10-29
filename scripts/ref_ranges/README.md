# ref_ranges

Turns reference-range texts into numeric limits, then uses those ranges to flag rows whose unit is wrong (the ref-qc stage).

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

2. **`ref_units.py`**: for each test × OMOP pair, learns which reference ranges go with which unit and flags rows whose range points to another unit. It takes the `*_LIMITS.tsv` from step 1 as `--limits`. There are three modes:
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
