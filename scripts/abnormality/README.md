# abnormality

Estimates per-OMOP low/high abnormality limits from the `H/HH/L/LL` flags in the engine output.

## Main

**`abnormality.py`**: for each OMOP ID, sorts values and walks in from each end until the share of abnormal flags drops below each `--thresholds` value. It also reports the 5th/95th percentile of the H and L values.

```
python3 abnormality.py --parquet_file <engine_output>.parquet [--thresholds 0.9 0.95 0.99] [--min-count 1] [--test]
```

Outputs (in the current dir):

- `abnormality_estimation.txt` (`--ab_ranges`): all thresholds
- `abnormality_estimation.table.tsv` (`--ab_table`): the compact table

```
ID       LOW_LIMIT  HIGH_LIMIT  LOW_PROBLEM  HIGH_PROBLEM  NAME
3000963  117        167         ...          ...           Hemoglobin [Mass/volume] in Blood
```
