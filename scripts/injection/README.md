# Unit injection exploration

The goal is to identify lab measurements that have a numeric value but are missing a unit, and to characterise the unit distribution of the matching records that do have a unit — so that a unit can be confidently assigned to the missing ones.

## Summary

![scatter](data/test_names_exploration_scatter.png)

*Active threshold: 98% — min count: 500 — 715 TEST_NAMEs, 31,962,504 measurements*

| Threshold | UNAMBIGUOUS test names | UNAMBIGUOUS measurements | AMBIGUOUS test names | AMBIGUOUS measurements | NO_DATA test names | NO_DATA measurements |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 95% | 434 (60.7%) | 26,508,342 (82.9%) | 79 (11.0%) | 4,345,264 (13.6%) | 202 (28.3%) | 1,108,898 (3.5%) |
| 98% \* | 413 (57.8%) | 24,979,594 (78.2%) | 100 (14.0%) | 5,874,012 (18.4%) | 202 (28.3%) | 1,108,898 (3.5%) |
| 99% | 392 (54.8%) | 22,954,996 (71.8%) | 121 (16.9%) | 7,898,610 (24.7%) | 202 (28.3%) | 1,108,898 (3.5%) |
| 100% | 275 (38.5%) | 12,745,902 (39.9%) | 238 (33.3%) | 18,107,704 (56.7%) | 202 (28.3%) | 1,108,898 (3.5%) |
| **TOTAL** | **715** | | | || |

---

## Usage

```bash
python3 explore_test_name.py <parquet_file> [options]
```

- `<parquet_file>`: path to the input parquet file (INJECT.parquet)
- `--min-count INT`: minimum no-unit records a TEST_NAME must have to be included in plots and injection (default: 1000)
- `--prevalence-threshold FLOAT`: min dominant-unit prevalence (%) to classify a TEST_NAME as UNAMBIGUOUS (default: 98)
- `--min-target-n INT`: minimum reference records a unit must have; tests below this are classified NO_DATA (default: 30)
- `--dump-dir PATH`: cache directory for per-test `.npy` arrays (default: `/mnt/disks/data/kanta/inject/tmp/`)
- `--inject`: run the injection engine (unambiguous + ambiguous + no_data passes)
- `--dip-threshold FLOAT`: Hartigan dip test p-value threshold for bimodality (default: 0.05)
- `--test`: small-sample mode — one test per COUNT decile (unambiguous) and top 10 by volume (ambiguous)

### Caching behaviour

`test_name_counts.tsv` and `test_name_details.tsv` are cached after the first ClickHouse query and reused on subsequent runs — delete them to force a re-query. They are always queried with a fixed baseline of `COUNT > 50`; `--min-count` is applied as a post-load filter in Python, so changing it never requires a re-query.

`plot_name_level.tsv`, `test_names_exploration_scatter.png`, and `summary_table.md` are always recomputed from the filtered data.

Per-test `.npy` arrays are always cached in `--dump-dir` and reused across runs.

---

## Step 1 — Injection targets (`test_name_counts.tsv`)

Counts records where at least one value is present (`MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_SOURCE IS NOT NULL`) but the unit prefix is absent (`MEASUREMENT_UNIT_PRE_FIX IS NULL`), grouped by `TEST_NAME`. Cached with `COUNT > 50`; further filtered to `--min-count` at runtime.

Columns: `TEST_NAME`, `COUNT`

---

## Step 2 — Reference population (`test_name_details.tsv`)

For every TEST_NAME in Step 1, describes the unit distribution of the records that already have both a value and a unit (`MEASUREMENT_VALUE_SOURCE IS NOT NULL AND MEASUREMENT_UNIT_PRE_FIX IS NOT NULL`).

Columns: `TEST_NAME`, `COUNT`, `UNIT`, `PREVALENCE_DICT`

- `COUNT`: total reference records for this TEST_NAME (across all units)
- `UNIT`: the most frequent unit
- `PREVALENCE_DICT`: top-3 units and their percentage share, e.g. `{mmol/l:98.5,umol/l:1.5}`

---

## Step 3 — Plotting table, scatter plot, and classification

Steps 1 and 2 are merged in Python to produce `plot_name_level.tsv`, a scatter plot, and a summary table. Each TEST_NAME is assigned a `CATEGORY` that drives which injection pass it enters.

### `plot_name_level.tsv`

One row per TEST_NAME. Columns include `COUNT`, `N_WITH_UNIT`, `top_prevalence`, `CATEGORY`, and exploratory `CATEGORY_{95,98,99,100}` columns.

- `COUNT`: no-unit record count
- `N_WITH_UNIT`: total reference records with any unit
- `top_prevalence`: prevalence (%) of the dominant unit; 0 if no reference records exist

### Classification (`CATEGORY`)

| Category | Condition |
|---|---|
| `NO_DATA` | `N_WITH_UNIT < --min-target-n` OR `top_prevalence == 0` |
| `UNAMBIGUOUS` | `top_prevalence >= --prevalence-threshold` |
| `AMBIGUOUS` | `0 < top_prevalence < --prevalence-threshold` |

The injection engine only runs tests classified as UNAMBIGUOUS or AMBIGUOUS. NO_DATA tests are written directly to `no_data_results.tsv`.

---

## Step 4 — Injection engine (`--inject`)

With `--inject`, three passes are run and a coverage check validates that every TEST_NAME in `plot_name_level.tsv` appears in exactly one output file.

### Unambiguous pass → `unambiguous_results.tsv`

TEST_NAMEs with `CATEGORY == UNAMBIGUOUS`. The candidate distribution (no-unit records) is compared against the reference distribution for the dominant unit.

One row per TEST_NAME. Columns: `TEST_NAME`, `UNIT`, `PREVALENCE_DICT`, `N_CANDIDATE`, `N_TARGET`, `CAND_DECILES`, `TARG_DECILES`, `KS_STAT`, `KS_MLOGP`, `KS_PASS`, `T_STAT`, `T_MLOGP`, `T_PASS`, `MAD_DIST`, `MAD_THRESHOLD`, `MAD_PASS`, `OUTCOME`, `NOTES`.

### Ambiguous pass → `ambiguous_results.tsv`

TEST_NAMEs with `CATEGORY == AMBIGUOUS`. Only units with prevalence > 1% and at least `--min-target-n` reference records are considered.

Pipeline per TEST_NAME:

1. **Pre-check**: run the full (unsplit) candidate distribution against each qualifying unit. If any unit passes, record those results and skip bimodality entirely.
2. **Bimodality check** (only if pre-check fails): test the candidate distribution for bimodality (see below). If bimodal, split into low/high sub-distributions and re-run the engine on each sub-distribution × unit.

One row per `(TEST_NAME, SUB_DIST)` — the best unit only. Best unit selection: PASS > FAIL > SKIP, then highest `UNIT_PREVALENCE` as tiebreaker — we prefer the dominant clinical unit when two units both pass.

Columns: `TEST_NAME`, `BIMODAL_STATUS`, `BIMODAL_SEP`, `BIMODAL_BC`, `BIMODAL_DIP_P`, `SUB_DIST`, `UNIT`, `UNIT_PREVALENCE`, `PREVALENCE_DICT`, `BEST_UNIT`, `N_CANDIDATE`, `N_TARGET`, `CAND_DECILES`, `TARG_DECILES`, `KS_STAT`, `KS_MLOGP`, `KS_PASS`, `T_STAT`, `T_MLOGP`, `T_PASS`, `MAD_DIST`, `MAD_THRESHOLD`, `MAD_PASS`, `OUTCOME`, `NOTES`.

### No-data pass → `no_data_results.tsv`

TEST_NAMEs with `CATEGORY == NO_DATA`. No engine is run; unit is left blank.

Columns: `TEST_NAME`, `UNIT`

### Unified output → `injection_results.tsv`

Merge of the unambiguous and ambiguous results with a `TYPE` column (`unambiguous` / `ambiguous`). Does not include no_data rows.

### Coverage check

After all three passes, a checksum validates that the union of the three output files equals the full set of TEST_NAMEs in `plot_name_level.tsv`, with no overlaps and no missing entries.

---

## Injection engine pipeline

Each comparison runs three tests in order. All three always run; the first to decide the outcome wins.

1. **KS test** — two-sample Kolmogorov–Smirnov. PASS = stat < 0.3 AND p < 0.05. Uses a fast binned approximation (100k bins, Hodges-corrected asymptotic p) above 500k samples; exact scipy otherwise.
2. **Welch t-test** — fallback if KS fails. PASS = p ≥ 0.05 (means not significantly different).
3. **MAD test** — last resort. PASS = |median(candidate) − median(target)| ≤ 3 × MAD(target).

Decision rule: KS PASS → PASS. KS FAIL, T PASS → PASS. Both fail → MAD decides. `NOTES` records the deciding test and how many of the three passed, e.g. `PASS_at_T_(2/3)`.

P-values are stored as −log10(p) throughout (`KS_MLOGP`, `T_MLOGP`).

Each comparison produces a 3-panel diagnostic plot saved to `--dump-dir`:
- **Panel 1**: ECDFs + KS distance marked in red + KS annotation
- **Panel 2**: KDE (linear scale) + dotted mean lines + t-test annotation
- **Panel 3**: KDE (log scale) + MAD band (green = PASS, salmon = FAIL) + median lines + distance arrow

KDE/ECDF rendering downsamples to 50k points. All statistics use the full arrays.

---

## Bimodal check

Before the ambiguous sub-distribution pass, the candidate distribution is tested for bimodality using two statistics:

**Hartigan's dip test** (primary gate): p-value < `--dip-threshold` (default 0.05) declares non-unimodal.

**Bimodality coefficient** (BC): `(skew² + 1) / (excess_kurtosis + 3(n−1)²/((n−2)(n−3)))`. Values above ~0.555 suggest bimodality.

Both are computed in the space (linear or log) where a 2-component GMM achieves lower BIC. The GMM separator is used to split the candidate distribution.

| dip p | BC | Status |
|---|---|---|
| ≥ threshold | — | `unimodal` |
| < threshold | ≥ 0.555 | `bimodal` — split into low/high |
| < threshold | < 0.555 | `bimodal_cautious` — split, modes overlap |

A diagnostic plot (`bimodal_{tag}.png`) is saved to `--dump-dir`.
