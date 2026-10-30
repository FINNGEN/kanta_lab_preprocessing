# posneg

Pos/neg and `+` free-text tallies, to curate the engine's mapping tables.

## Main

**`extract_pos_counts_parquet.py`**: tallies the free texts containing pos/neg or `+`, and compares them with the engine's current mapping tables. The output is curated and copied back as `negpos_mapping.tsv` / `kanta_plusplus_abnormality.tsv`. Run by the WDL's `pos_tables` task after each engine run.

```
python3 extract_pos_counts_parquet.py <engine_output>.parquet [--test]
```

Writes `pos_neg_summary.tsv` and `plusplus_summary.tsv`, plus `*_pasteable.tsv` versions that are safe to paste into Sheets:

```
MEASUREMENT_FREE_TEXT  extracted::IS_POS  COUNT  ratio_COUNT  Npeople  ratio_Npeople  NOTES
positiivinen           1                  52000  1.04         31000    1.02           NA
```
