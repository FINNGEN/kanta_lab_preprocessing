# counts

Per-OMOP counts of the engine output, and comparisons between releases. The pos/neg free-text tallies are in `../posneg/`.

## Main

**`counts.py`**: counts the non-NA values (`-c`), zeros (`-b`), TRUEs (`-t`) and rows where at least one of a group of columns is non-NA (`-a NAME=COL1|COL2`, e.g. a lower or an upper reference limit) per OMOP ID, plus distinct people. Run it on a `_RELEASE.parquet` (release column names, real nulls). A column the input lacks counts as all missing, so two releases' tables keep the same columns.

```
python3 counts.py <release>.parquet -c MEASUREMENT_VALUE_CLEANED,REFERENCE_RANGE_LOW_VALUE -b QC_PASS -a 'REFERENCE_RANGE=REFERENCE_RANGE_LOW_VALUE|REFERENCE_RANGE_HIGH_VALUE' -o out/ [-p kanta] [--top-n 10] [--test]
```

Writes `out/<prefix>_omop_analysis.md` and `.tsv`:

```
NAME     FINNGENID  MEASUREMENT_VALUE_COUNT  ...
ALL      450000     250000000
3000963  400000     12000000
```

## Utils

- **`count_plot.py`**: compares two releases' counts. `--new` / `--old` take `counts.py` TSVs, or the `_RELEASE.parquet` files themselves: each is then counted first (values, outcomes, failed QC, rows with a reference limit) into `new_`/`old_<parquet name>_omop_analysis.tsv` in the current folder, reused while newer than the parquet (`--force` to recount). The WDL's `release_counts` task runs it when `engine.old_release_parquet` is given. It writes `relative_change.tsv`, a histogram PNG, and `appeared_omop_ids.tsv` / `disappeared_omop_ids.tsv`.
  ```
  python3 count_plot.py --new <new>_RELEASE.parquet --old <old>_RELEASE.parquet
  ```
