# summaries

Shareable summary tables per TEST_NAME × unit (the unit after primary injection). Every row has more than `--min-subjects` people.

## Main

**`summaries.py`**: a Python/duckdb port of `old/summaries.R`.

```
python3 summaries.py --input <engine_output>.parquet [--out DIR] [--min-subjects 5] [--test]
```

Writes these tables (examples in `results/`):

- `summaryTest`
- `summaryUnitSource`
- `summaryValuesSource`
- `summaryValues` (deciles)
- `summaryOutcomes`
- `summaryReference`

```
TEST_NAME                  MEASUREMENT_UNIT  n_records  n_subjects
!pak.maitoigavasta-aineet  %                 131        130
```

## Other

- `results/`: the output of the last run
- `old/`: the original R script and its TODO
