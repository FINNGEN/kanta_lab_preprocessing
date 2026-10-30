# sote

Service-provider (SP) analyses: value QC per provider, and assigning a region to providers and people.

## qc_values/

**`qc_build.py`**: in one pass over all tests, builds a slim row-level base (kept in the cache) and the shareable aggregates per OMOP × test × SP × year.

```
python3 qc_values/qc_build.py --input <engine_output>.parquet [--out DIR] [--out-cache DIR] [--min-test-people 20] [--test]
```

Exportable outputs: `qc_tests`, `qc_lookup`, `qc_hist`, `qc_national` and `qc_mix`:

```
qc_national:  omop     n         p001  ...  p50  ...  p999  fence_lo  fence_hi
              3000963  12000000  70    ...  140  ...  185   ...       ...
```

## region/

### Main (run in order)

1. **`sp_region_cs.py`**: links each SP to a maakunta/hyvinvointialue. The chain is coding-system OID → org code → `data/sote.txt.gz` → `data/municipality_to_region.tsv`.
   ```
   python3 region/sp_region_cs.py --input <engine_output>.parquet [--out DIR]
   ```
   Main output: `sp_region.tsv` (the top region per SP, with its purity). It also writes `sp_x_codingsystem.tsv`, `cs_region.tsv` and `sp_best_org.tsv`.

2. **`region_people.py`**: assigns each person their most likely maakunta from their SPs. It then validates the assignment against region of birth, cohort, the vitamin D seasonal cycle and THL disease references.
   ```
   python3 region/region_people.py --input <engine_output>.parquet [--threshold 50] [--pheno PATH]
   ```
   Main output: `person_maakunta_t50.tsv` (row-level, stays on the data side). The aggregates are `vitd_*_t50.tsv`, `disease_vs_thl_*.tsv` and the figures:
   ```
   disease_vs_thl:  maakunta         ...  n_people  n_cases  finngen_index  thl_index
                    Pohjois-Karjala  ...  21000     2400     112            115
   ```

### Utils

- **`fetch_thl_reference.py`**: downloads the THL reference tables into `data/` (Sotkanet health index, infectious disease register incidence), so `region_people.py` runs offline.
  ```
  python3 region/fetch_thl_reference.py [--year 2024]
  ```
- `data/`: static lookup tables (the sote register, municipality → region, the THL references)
- `tests/count_coding_replacements.py`: counts rows with old-form coding-system OIDs, and how many the engine mapped
- `tests/upload_shareable.sh`: checks that the aggregate outputs hold no FinnGen IDs or small cells, then uploads them to the share bucket (`--dry-run` to check only)
