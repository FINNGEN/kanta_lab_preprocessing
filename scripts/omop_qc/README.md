# omop_qc

QC tools for a single OMOP concept, and for the engine's `omop_qc.tsv`.

## Main

**`explore_omop.py`**: plots one OMOP ID's value distributions at three pipeline stages, split by unit and by test name. The stages are:

- SOURCE
- post-injection
- harmonized

It also KS-tests each test name against the others to catch mismapped tests. Results are cached under `--dump-dir`, so reruns go straight to plotting.

```
python3 explore_omop.py <engine_output>.parquet 3000963 [--out-dir DIR] [--top-n-units N] [--ks-d-threshold 0.3]
```

Writes `omop_3000963_report.png`: KDE panels per stage, with the KS mismatch table at the bottom.

## Utils

- **`sort_omop_qc.py`**: re-sorts `src/kanta/engine/data/omop_qc.tsv` in place. Rows with a threshold come first, then the `NA` placeholders. Run it after hand-editing the file; CI checks the order.
  ```
  python3 scripts/omop_qc/sort_omop_qc.py
  ```
