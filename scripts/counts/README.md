# counts

Per-OMOP counts of the engine output, comparisons between releases, and pos/neg free-text tallies.

## Main

**`counts.py`**: counts the non-NA values (`-c`), zeros (`-b`) and TRUEs (`-t`) per OMOP ID, plus distinct people.

```
python3 counts.py <engine_output>.parquet -c MEASUREMENT_VALUE,MEASUREMENT_UNIT -t SOME_FLAG -o out/ [-p kanta] [--top-n 10] [--test]
```

Writes `out/<prefix>_omop_analysis.md` and `.tsv`:

```
NAME     FINNGENID  MEASUREMENT_VALUE_COUNT  ...
ALL      450000     250000000
3000963  400000     12000000
```

**`extract_pos_counts_parquet.py`**: tallies the free texts containing pos/neg or `+`, and compares them with the engine's current mapping tables. The output is curated and copied back as `negpos_mapping.tsv` / `kanta_plusplus_abnormality.tsv`.

```
python3 extract_pos_counts_parquet.py <engine_output>.parquet [--test]
```

Writes `pos_neg_summary.tsv` and `plusplus_summary.tsv`, plus `*_pasteable.tsv` versions that are safe to paste into Sheets:

```
MEASUREMENT_FREE_TEXT  extracted::IS_POS  COUNT  ratio_COUNT  Npeople  ratio_Npeople  NOTES
positiivinen           1                  52000  1.04         31000    1.02           NA
```

## Utils

- **`count_plot.py`**: compares two `counts.py` TSVs (the new and the old release). It writes `relative_change.tsv`, a histogram PNG, and `appeared_omop_ids.tsv` / `disappeared_omop_ids.tsv`.
  ```
  python3 count_plot.py --new new.tsv --old old.tsv
  ```
