# sote

Service-provider (SP) analyses: assigning a region to providers and people, and organisation OIDs to maakunnat. The value QC per provider is in `scripts/qc_checks/qc_build.py`.

## region/

### Main (run in order)

1. **`sp_region_cs.py`**: links each SP to a maakunta/hyvinvointialue. The chain is coding-system OID → org code → `data/sote.txt.gz` → `data/municipality_to_region.tsv` (both in `scripts/sote/data/`).
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

## THL

- **`anonymize_sote_ids.py`**: copies a tab-separated (.gz) file, putting in place of an organisation OID column (`--column`, default `antaja_organisaatioid`) the organisation's maakunta (`antaja_organisaatio_maakunta`), then how it was resolved (`_how`), the original OID and the register OID the maakunta came from (`_match`). Made for the statements file. It reads the sote register and the municipality → region table from `data/`. Only exact matches get a maakunta: the OID must be a register unit with a known municipality, and no guessing is done from parents, organisations or similar OIDs. A unit recorded with a merged-away municipality (e.g. `838 Tarvasjoki`) gets that municipality's maakunta from `data/municipality_history.tsv` (`exact_old_municipality`). Everything else is `NA`, with the reason in `_how`: `head_office_multi` (a QC rule: the exact match is the head office of an organisation whose units are in several maakunnat, e.g. Terveystalo, Mehiläinen, Fimlab, so its municipality says nothing about where the statement was given), `unit_no_region` (a register unit whose municipality code isn't a municipality, current or old: `999 Ei tiedossa`, `200 Ulkomaat`, a mistyped code), `professional` (`1.2.246.537.28.<n>`, an individual self-employed healthcare professional), `not_found` (not a register unit) or `missing` (empty, `NA`, `Puuttuu`). The log counts the rows resolved each way and lists the top OIDs not found. The rest of each line is copied byte for byte.
  ```
  python3 THL/anonymize_sote_ids.py --input <reports>.txt.gz [--column antaja_organisaatioid] [--output out.txt.gz] [--test [N]]
  ```
  The file is streamed line by line (memory doesn't grow with its size); `--test [N]` stops after the first N lines (default 100k) and writes `<name>_maakunta_test.txt.gz`.

The rest are THL-side scripts used elsewhere, unrelated to the sote analyses: `out.R`, `Real_kanta_labs_2016_DF14_toFG.R`, `parametrize_r.py`, `generate_sample.py`, `BUGS.md`.

## data

The lookup tables the sote scripts read, and the scripts that generate them:

- `sote.txt.gz`: THL's sote organisation register (`;`-separated, ISO-8859-1); no generating script.
- `municipality_to_region.tsv`: municipality → maakunta / hyvinvointialue, Statistics Finland's 2024 classification; no generating script.
- `municipality_history.tsv`: every municipality code since 1990 → maakunta.
- `sotkanet_health_index_<year>.tsv`, `thl_ttr_incidence_<first>_<last>.tsv`: THL's regional disease references for `region/region_people.py`.

- **`fetch_thl_reference.py`**: downloads the THL reference tables next to itself (Sotkanet health index, infectious disease register incidence), so `region_people.py` runs offline.
  ```
  python3 data/fetch_thl_reference.py [--year 2024]
  ```
- **`fetch_municipality_history.py`**: downloads from Statistics Finland's classification service every municipality code used since 1990, with the maakunta it belonged to in its last valid year (translated to today's maakunnat), into `municipality_history.tsv` next to itself. It covers the merged-away municipalities that `municipality_to_region.tsv` (the 2024 classification) doesn't have.
  ```
  python3 data/fetch_municipality_history.py
  ```
