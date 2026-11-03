# qc_checks

Value QC on the engine output: per test and service provider (`qc_build.py`), for a single OMOP concept (`explore_omop.py`), and for the engine's `omop_qc.tsv`.

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

**`qc_build.py`**: in one pass over all tests, builds a slim row-level base (kept in the cache) and the shareable aggregates per OMOP × test × SP × year.

```
python3 qc_build.py --input <engine_output>.parquet [--out DIR] [--out-cache DIR] [--min-test-people 20] [--test]
```

Exportable outputs: `qc_tests`, `qc_lookup`, `qc_hist`, `qc_national` and `qc_mix`:

```
qc_national:  omop     n         p001  ...  p50  ...  p999  fence_lo  fence_hi
              3000963  12000000  70    ...  140  ...  185   ...       ...
```

## Utils

- **`sort_omop_qc.py`**: re-sorts `src/kanta/engine/data/omop_qc.tsv` in place. Rows with a threshold come first, then the `NA` placeholders. Run it after hand-editing the file; CI checks the order.
  ```
  python3 scripts/qc_checks/sort_omop_qc.py
  ```
