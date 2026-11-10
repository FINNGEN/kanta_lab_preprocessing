# qc_checks

Value QC on the engine output: per test and service provider (`sp_qc.py`, `qc_build.py`), for a single OMOP concept (`omop_ref_audit.py`), and for the engine's `omop_qc.tsv`.

## Main

**`omop_ref_audit.py`**: audits one OMOP ID with the reference ranges: which rows went wrong in injection or harmonization, how, and what their unit should be. Each row's value, reference range and unit are checked against each other:

1. unit vs range: the range, converted with the row's harmonization factor, against the concept's canonical range (learned per sex × age group from the rows where everything agrees)
2. value vs range: the value against its own range, which needs no unit
3. value vs unit: the harmonized value against the concept median

Together they give a verdict per row: `ok`, `wrong unit` (value and range agree, the unit doesn't), `range off scale`, `value mis-scaled`, `value extreme`, `inconsistent` (the range differs from the others, the value is fine: often another assay), or, for rows without a harmonized value, `unharmonized: fixable` and the like. Each test name's harmonized values are also KS-tested against the rest of the concept (D ≥ `--ks-d`, 0.3): a high D with few rows flagged is another analyte or specimen on a similar scale, which the scale checks miss. A wrong unit gets a suggested unit from the conversion table: the unit whose factor puts the range on the canonical one. Rows without a range (`value off (no range)` when off by a unit factor, `unharmonized: no range`) get one from the value alone: the unit that brings it to the concept median, a weaker hint. Each flagged group also gets the stage its unit came from: lab, injected, rewritten (injection changed it) or no unit.

```
python3 omop_ref_audit.py 3019550 [--input <engine_output>.parquet] [--out-dir DIR] [--cache-dir DIR]
```

`--input` defaults to the most recent run in `/mnt/disks/data/kanta/engine`; reports go to `/mnt/disks/data/kanta/qc/omop` (`sp_qc.py` writes next to it, in `qc/sp`). The OMOP's rows are aggregated in duckdb and cached under `--cache-dir`. Writes `omop_3019550_ref_audit.png` (the three checks per test, a decision map, verdicts per test, flagged rows per stage, the flagged groups), `_groups.tsv`, `_canonical.tsv` and `_tests.tsv` (the KS test per test name), and prints the flagged groups as Markdown. Groups under `--min-n` (5) rows are written as `BELOW_THRESHOLD`. `% rows` is each group's share of the rows (of the groups shown, so the masked ones can't be worked out), and the share of all flagged rows is printed above the table as the overall impact.

```
test   source -> unit -> harmonized  stage  verdict     rows  % rows  range (source scale)  median pre-harm.  median harmonized  range x  value x  suggested unit
b-hkr  % -> % -> %                   lab    wrong unit  6000  1.7     0.35-0.46             0.417             0.417              /100     /100     ratio
```

**`qc_build.py`**: in one pass over all tests, builds a slim row-level base (kept in the cache) and the shareable aggregates per OMOP × test × SP × year.

```
python3 qc_build.py --input <engine_output>.parquet [--out DIR] [--out-cache DIR] [--min-test-people 20] [--test]
```

Exportable outputs: `qc_tests`, `qc_lookup`, `qc_hist`, `qc_national` and `qc_mix`:

```
qc_national:  omop     n         p001  ...  p50  ...  p999  fence_lo  fence_hi
              3000963  12000000  70    ...  140  ...  185   ...       ...
```

**`sp_qc.py`**: which service providers (SPs), and when, deviate from the national picture for an OMOP ID, and why. It needs nothing run before it. Each OMOP's rows are read from the engine output once and cached in `/mnt/disks/data/kanta/tmp/sp_qc/rows_<input name>/omop=<id>/` (one row per result with a value, only the columns it needs, the person hashed); the OMOPs not cached yet are read together in one pass (duckdb, a partitioned write, no sort). The cache is dropped and rebuilt only when the input file changes (or with `--force`). Each OMOP's aggregates are cached too. Each flag gets a likely cause: the test, unit, decimals, QC note or range source whose share changed most in the flagged period. The kinds of flag are:

- `shift`: an SP × year whose median, adjusted for sex and age, is off national
- `step`: a lasting change in an SP's monthly median, and whether its reference limit changed with it (method change) or not (drift); only over runs of consecutive months, and marked when it comes after a gap in the data
- `episode`: a run of odd months that returns to normal
- `outliers`, `qc_fail`, `person_jump` (values far from the same person's other values): rates against expected
- `range`: an SP whose typical reference limit, converted to the harmonized unit, is off national (adults); `range_change`: an SP's limit changed between years. The report writes the actual ranges out: national per sex × age, and each SP's own, with the share of its limits from the curated text table
- `outcome`: `TEST_OUTCOME` disagrees with the row's own range

```
python3 sp_qc.py [--omop 3020564 3023602 ...] [--all] [--input <engine_output>.parquet] [--out DIR] [--cache DIR]
```

`--input` defaults to the most recent run in `/mnt/disks/data/kanta/engine` (a release file works too, without the missing-unit cause). `--omop` defaults to the QC-thread concepts (creatinine, ALT, ferritin, HDL, TPO-Ab, TSH, holo-TC, sodium). Writes into `/mnt/disks/data/kanta/qc/sp` (next to `omop_ref_audit.py`'s `qc/omop`) `sp_qc_flags.tsv` (every flag, most severe first) and, per OMOP, `sp_qc_<omop>.png` (SP × year heatmap, monthly series of the flagged SPs with their reference limit, limits per SP, flags), `_flags.tsv`, `_sp_year.tsv` and `_ranges.tsv`. `SERVICE_PROVIDER` is a pseudonym in R15, so SPs are named by the organisation most of their rows' coding systems belong to (`CODING_SYSTEM_MAP`), plus the end of the pseudonym; cells under `--min-n` (5) rows are written as `BELOW_THRESHOLD`.

```
kind  sp                      period        rows   effect  detail                                                         likely cause
step  Fimlab Laboratoriot Oy  from 2017-03  31022  /1.09   median /1.09 from 2017-03; upper limit changed too: method change?
```

## Utils

- **`sort_omop_qc.py`**: re-sorts `src/kanta/engine/data/omop_qc.tsv` in place. Rows with a threshold come first, then the `NA` placeholders. Run it after hand-editing the file; CI checks the order.
  ```
  python3 scripts/qc_checks/sort_omop_qc.py
  ```
