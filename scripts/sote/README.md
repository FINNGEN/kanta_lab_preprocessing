# sote

Service-provider (SP) analyses: assigning a region to providers and people, and organisation OIDs to maakunnat. The value QC per provider is in `scripts/qc_checks/sp_qc.py`.

The scripts that read the engine output (`region/sp_region_cs.py`, `region/region_people.py`, `region/sp_names.py`, `data/coding_system_map.py`) default `--input` to the most recent run in `/mnt/disks/data/kanta/engine/`. Runs are stored as `<prefix>_<YYYY_MM_DD>/<prefix>_<YYYY_MM_DD>.parquet`, and the latest date wins. Each script prints the file it uses.

## region/

### Main (run in order)

1. **`sp_region_cs.py`**: links each SP to a maakunta.
   - **Input:** each row's coding-system maakunta, `CODING_SYSTEM_MAAKUNTA` in the engine output. The engine looks it up in `src/kanta/engine/data/coding_system_map.tsv`, which `data/coding_system_map.py` builds. Only about a third of rows have one.
   - **Vote:** per SP, the rows with a maakunta vote. The SP gets the top maakunta, and `region_people.py` uses it for all of that SP's rows.
   ```
   python3 region/sp_region_cs.py [--input <engine_output>.parquet] [--out DIR]
   ```
   Main output: `sp_region.tsv` (the top maakunta per SP, with its purity). It also writes `sp_x_codingsystem.tsv` (SP × coding system counts), for diagnostics.

2. **`region_people.py`**: assigns each person their most likely maakunta from their SPs. It then validates the assignment against region of birth, cohort, the vitamin D seasonal cycle and THL disease references.
   ```
   python3 region/region_people.py [--input <engine_output>.parquet] [--threshold 50] [--sp-min-region-pct 5] [--pheno PATH]
   ```
   `--sp-min-region-pct` (default 5) skips SPs whose maakunta rests on less than that % of their own rows (`pct_region_of_total` in `sp_region.tsv`). In the full run checked so far, these were nationwide private chains (Terveystalo, Mehiläinen, …) assigned a maakunta from a few stray rows.

   Main output: `person_maakunta_t50_reg5.tsv` (row-level, stays on the data side). The aggregates are `vitd_*_t50_reg5.tsv`, `disease_vs_thl_*.tsv` and the figures. The settings are in every output's name, so a run with other settings doesn't reuse the cached files:
   ```
   disease_vs_thl:  maakunta         ...  n_people  n_cases  finngen_index  thl_index
                    Pohjois-Karjala  ...  21000     2400     112            115
   ```

3. **`sp_names.py`**: names each SP the way the QC reports do (`scripts/qc_checks/sp_qc.py` and `qc_portal.py` import its `Labels`, so the names agree everywhere). An SP is named after its top coding-system organisation (`CODING_SYSTEM_MAP`). If that organisation has no maakunta of its own (a private company such as Fimlab, a lab serving several regions such as NordLab) and the SP has a region, the SP is named after the region instead (e.g. `Pirkanmaa`). The region is worked out as in `sp_region.tsv`: the top maakunta among the regional rows, which must be at least `--region-min` (5%) of the SP's rows with an organisation. The script writes `kanta_sp_names.tsv`, one row per SP with an organisation: `name`, `label` (the name as the reports print it, plus the pseudonym's last 6 characters), `named_after` (organisation / region / oid), `top_org` and its share, `maakunta`, `pct_maakunta`, `pct_regional`, and `n_org_rows`. The file holds pseudonyms and row counts, so it stays on the data side. It is one group-by over the engine output, and `wdl/engine.wdl` runs it (task `sp_names`, output `service_provider_names`). To use the pipeline's table in the reports instead of counting again, pass `sp_qc.py --sp-names kanta_sp_names.tsv` (and the same to `qc_portal.py`).
   ```
   python3 region/sp_names.py [--input <engine_output>.parquet] [--out DIR]
   ```

## THL

- **`anonymize_sote_ids.py`**: copies a tab-separated (.gz) file, putting in place of an organisation OID column (`--column`, default `antaja_organisaatioid`) the organisation's maakunta (`antaja_organisaatio_maakunta`), then how it was resolved (`_how`), the original OID and the register OID the maakunta came from (`_match`). Made for the statements file. It reads the sote register and the municipality → region table from `data/`. Only exact matches get a maakunta: the OID must be a register unit with a known municipality, and no guessing is done from parents, organisations or similar OIDs. A unit recorded with a merged-away municipality (e.g. `838 Tarvasjoki`) gets that municipality's maakunta from `data/municipality_history.tsv` (`exact_old_municipality`). The one override: an OID that isn't in the register (a unit missing from the export) gets its organisation's maakunta when the organisation, by the Y-tunnus in the OID, has all its units, at least 20, in one maakunta (`org_all_units`, with `_match` = the organisation's top-level entry; ~99.9% right when tested on the register's own units). Everything else is `NA`, with the reason in `_how`: `head_office_multi` (a QC rule: the exact match is the head office of an organisation whose units are in several maakunnat, e.g. Terveystalo, Mehiläinen, Fimlab, so its municipality says nothing about where the statement was given), `unit_no_region` (a register unit whose municipality code isn't a municipality, current or old: `999 Ei tiedossa`, `200 Ulkomaat`, a mistyped code), `professional` (`1.2.246.537.28.<n>`, an individual self-employed healthcare professional), `not_found` (not a register unit, and its organisation isn't entirely in one maakunta) or `missing` (empty, `NA`, `Puuttuu`). The log counts the rows resolved each way and lists the top OIDs not found. The rest of each line is copied byte for byte.
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
- **`fetch_maakunta_map.py`**: downloads the maakunta boundaries from Statistics Finland's regional-division service (1:1 000 000, EPSG:3067, CC BY 4.0), drops islands under `--min-area` km² and simplifies each outline (Douglas–Peucker, `--tolerance` metres), and writes `maakunta_map.json` next to itself: one SVG path per maakunta (~30 kB). The QC portal (`scripts/qc_checks/qc_portal.py`) draws a service provider's region with it.
  ```
  python3 data/fetch_maakunta_map.py [--tolerance 1500] [--min-area 30]
  ```
- **`fetch_maakunta_population.py`**: downloads the population of each maakunta on 31 December of `--year` (default 2024) from Statistics Finland's StatFin table 11ra (CC BY 4.0) into `maakunta_population.tsv` next to itself (`maakunta`, `code`, `year`, `population`). The QC portal compares each region's people with it (coverage).
  ```
  python3 data/fetch_maakunta_population.py [--year 2024]
  ```
- **`fetch_municipality_history.py`**: downloads from Statistics Finland's classification service every municipality code used since 1990, with the maakunta it belonged to in its last valid year (translated to today's maakunnat), into `municipality_history.tsv` next to itself. It covers the merged-away municipalities that `municipality_to_region.tsv` (the 2024 classification) doesn't have.
  ```
  python3 data/fetch_municipality_history.py
  ```
- **`coding_system_map.py`**: maps each `CODING_SYSTEM` OID of an engine output to its organisation and region using `sote.txt.gz`.
  - **Counts:** it first counts the distinct coding systems that have more than `--min-count` rows (default 5) and caches them in `<out>/coding_system_counts.tsv` (`CODING_SYSTEM`, `CODING_SYSTEM_MAP`, `CNT`). When the cache exists the engine output isn't read again; `--force` recounts.
  - **Lookup order:** national code lists and `NA` have no organisation. CGI-hosted systems are decoded to their customer's Y-tunnus (customers that don't decode are `cgi_undecoded`). The org code is then looked up as the exact register unit (`node`), then the `<code>.10.0` entry (`org_root`), then the register's `A:Y-Tunnus` column (`ytunnus`), then a short manual list (`manual`). The name is the organisation's register `LongName`; for a `node` hit the unit's name goes in `unit_name`.
  - **Region:** an organisation gets one only if it is public, is not a lab serving several regions (NordLab, ISLAB), and its municipality maps to a maakunta.
  - **Output:** `<out>/coding_system_map.tsv`, one row per coding system. `--out` is a folder (default: the current directory) and holds the cache too. To update the engine's mapping, copy the table to `src/kanta/engine/data/coding_system_map.tsv` and commit it. That's the table the engine maps `CODING_SYSTEM` with (to `CODING_SYSTEM_MAP`, released as `CODING_SYSTEM_ORG`, and the engine-only `CODING_SYSTEM_MAAKUNTA`).
  - **In the pipeline:** `wdl/engine.wdl`'s `extract_regions_limits` task runs it on each engine output, next to the reference-limit extraction, and returns `coding_system_map.tsv` and `coding_system_counts.tsv` for the next release. The Docker image carries all of `scripts/sote/`, register included; update `data/sote.txt.gz` there when THL publishes a new one.
  ```
  python3 data/coding_system_map.py [--input <engine_output>.parquet] [--out DIR] [--min-count 5] [--force]
  ```
