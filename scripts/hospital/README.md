# hospital

Compares how often each lab category appears in samples drawn during a hospital stay vs. outside one.

## Main (run in order)

1. **`build_intervals.py`**: builds merged per-person inpatient episodes from the FinnGen detailed longitudinal file.
   ```
   python3 build_intervals.py [--input <longitudinal>.txt.gz] [--source INPAT] [--out DIR] [--test]
   ```
   Writes `hospitalization_intervals_inpat.tsv`:
   ```
   FINNGENID  START_AGE  END_AGE
   FG…        54.21      54.25
   ```

2. **`prevalence_analysis.py`**: for each value of the `--columns` columns, computes the in-hospital fraction, the odds ratio with its CI, and an FDR-corrected G-test.
   ```
   python3 prevalence_analysis.py [--kanta-input <RELEASE>.parquet] [--intervals-input hospitalization_intervals_inpat.tsv] \
       [--columns TEST_NAME,OMOP_CONCEPT_ID] [--min-count 100] [--test]
   ```
   Writes `prevalence_<column>.tsv` and `prevalence_<column>_hist.png`:
   ```
   VALUE   N_IN    N_OUT    FRAC_IN  LOG2_OR  ...  LOG10_P_ADJ_FDR
   p-krea  812000  4100000  0.165    0.42     ...  -300.1
   ```
