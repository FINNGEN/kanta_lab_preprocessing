# scripts/qc_checks: rules for editing

These scripts build reports and a static web portal from individual-level health data (Kanta lab results). Every
change, including small page tweaks, must keep the rules below. Read `README.md` here for what each script does.

## Files
- `sp_qc.py`: Service Provider × time QC per OMOP. Owns the shared row cache, the people sketches and the gating
  helpers that the other scripts import (`import sp_qc as S`).
- `omop_ref_audit.py`: unit / reference-range audit per OMOP. Reads sp_qc's row cache.
- `qc_portal.py`: the static portal. It only collects what the two scripts produced, in two stages: `tables`
  (gated TSVs) and `site` (HTML from those tables only).
- `scripts/test/run_qc_all.sh`: runs both scripts in parallel over many OMOPs (`--prepare`, then one job per OMOP; `--skip-done`).
- The Service Provider naming rule lives in `scripts/sote/region/sp_names.py` (`Labels`). Change it there only;
  sp_qc and the portal import it.

## Privacy: hard rules (never relax them)
- **Every published number covers at least 5 distinct people** (`MIN_N = 5`, fixed in the portal). It applies to
  people, not rows, and to every count: table cells, histogram bins, chart points, shares, counts inside text, and
  numbers embedded in page source, `data-*` attributes or JSON, even if they're never displayed.
- **No back-calculation.** If a total and its parts are shown, the remainder must be 0 or ≥ 5 people, so use
  secondary suppression (`S.suppress_group`). A share needs the part and the rest each to be nobody or ≥ 5 people
  (`S.share_ok`). Pool small bins or years with their neighbours (`S.pool_adjacent`, `S.safe_bins`).
- **People counts come from sketches.** Each aggregate row carries the 5 smallest distinct person hashes; merge
  them with `S.merge_sketch` for the ≥ 5 test. Never count distinct people per group in duckdb (see Memory).
- Below the threshold, write the marker `BELOW_THRESHOLD` (`S.BELOW`) or leave the item out. Never write "fewer
  than 5" or anything else that hints at a 1–4 count.
- **Service Provider (lab) tables publish shares and people, not row counts.** Row counts summed across concepts
  could be differenced against other tables. The one exception is a lab's rows in a concept, which is the sum of
  the lab-years sp_qc already publishes in `_sp_year.tsv`. Region tables use percentages and official population
  only, no people counts.
- Every TSV goes through `S.publish_tsv(df, path, count_cols=...)`, which refuses counts of 1–4. Every count shown
  in HTML goes through `count_td` (it sets `data-n`). `scan_site` checks every TSV and every `data-n` before the
  site is swapped in; keep new outputs inside what it scans.
- Private files (`labs/ids.parquet`, the caches, pseudonyms, person hashes) are never copied to the site.
- Explanatory figures (e.g. the naming diagram) use made-up names, codes and shares, never real records.

## Architecture
- **Tables first, figures follow.** Gate the numbers in the tables stage, and draw every chart in the site stage
  from those gated tables only, never from raw rows.
- **The portal is static and offline:** plain HTML, CSS and vanilla JS with inline SVG. No CDN, no external
  libraries, no server code, no PNGs on the pages (draw natively; the scripts still write their own PDFs). The map
  comes from `scripts/sote/data/maakunta_map.json` via `assets/finland.js`.
- Caches are keyed on inputs and versions: bump `VERSION` / `LABS_VERSION` (portal) or `BASE_VERSION` (sp_qc row
  cache) when a cached table's content changes. Write caches atomically (`S.copy_atomic`, `S.parquet_ok`).
- One OMOP's failure is logged and skipped; it never stops the run.

## Memory (the data VM has 16 GB and freezes on out-of-memory)
- No per-group `count(distinct …)` or quantiles over the full data in duckdb. Use two-stage group-bys, sketches and
  the per-OMOP row cache. Keep duckdb's `--memory-limit`, and in `run_qc_all.sh` split it across jobs.
- Anything over the whole input goes through cached slim parquets (hashes, counts), never row-level strings in
  duckdb hash tables.

## Page conventions
- Say "Service Provider" in page text, never "lab" (sp_qc's own PDF texts may say lab).
- Colours are CSS tokens on `:root` with dark-mode variants (`--s1..--s8` categorical, `--up` / `--down` for
  more / less than expected, `--other`, `--none`). Never hard-code hex colours in markup. Every page must work in
  light and dark mode and at phone width (no sideways page scroll).
- Every table is sortable, searchable when long, and paged (`data-limit`). Every chart has a short "How to read".
- `[hidden]` uses `display:none!important`; keep it when adding grid or flex containers.

## Workflow
- No raw data on the dev machine. Code syncs to the data VM, where the user runs it and reports back.
- Test on synthetic data only, generated and kept in `/mnt/disks/data/claude/` or `/mnt/disks/data/kanta/tmp/`,
  never in this repo. Plant privacy traps (2-person groups, a 3-person lab, a 2-person year) and check they never
  appear.
- For new pages or figures, build a demo on fictitious data first and show it before wiring in real data.
- Check pages in a headless browser (screenshots in light, dark and phone width; no console errors).
- Write no outputs, caches, `__pycache__` or figures into the repo. Run Python with `PYTHONDONTWRITEBYTECODE=1`.
- Before committing anything derived from data, scan all output files (TSV, JSON, embedded page data) for counts
  under 5.
