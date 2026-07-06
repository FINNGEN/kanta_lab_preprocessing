# coding_map.py

Resolves the raw `tutkimuskoodistonjarjestelma` (CODING_SYSTEM) values in a Kanta Lab
parquet file to human-readable organization names, and writes a `CODE\tNAME\tCOUNT\tSOURCE`
table that can be dropped straight into `src/kanta/engine/data/thl_coding_manual_mapping.txt`
for `kanta.engine` to consume (see "Relationship to kanta.engine" below).

## Usage

```
python3 coding_map.py <input_file.parquet>
```

Run it from the directory you want the cache files written to — everything is cached in the
current working directory (see "Caching" below), not next to the script.

## What it does

1. **Count distinct values.** Runs a `clickhouse` query against the parquet file to get every
   distinct `tutkimuskoodistonjarjestelma` value with its row count (values with a count ≤ 10
   are dropped as noise).

2. **Extract an organization code.** Most values are OIDs of the form
   `1.2.246.10.<code>.<...>` or `1.2.246.537.10.<code>.<...>`, where `<code>` is a Y-tunnus
   (Finnish business id) with the leading zero and dash stripped out. `extract_code()` pulls
   that digit run out via regex; values that don't match either root (a handful of fixed
   values, plus the literal `"NA"`) are kept as their own row, keyed by the raw string, so
   nothing is silently dropped. Values sharing the same code (e.g. appearing under both OID
   roots, or with/without a trailing arc) are merged and their counts summed.

3. **Resolve a name for each code**, trying these sources in order until one has it:

   | # | Source | Covers | `SOURCE` value |
   |---|--------|--------|-----------------|
   | 1 | PRH's `all_companies` bulk export (Finnish Trade Register) | Private companies currently on the register | `PRH` |
   | 2 | PTV (Palvelutietovaranto), Finland's open public-service catalog | Municipalities, kuntayhtymät, hyvinvointialueet, state agencies | `PTV` |
   | 3 | `MANUAL_NAMES_BY_CODE`, hand-maintained in this file | hyvinvointialueet + their pre-2023 kuntayhtymä/sairaanhoitopiiri predecessors (public-law bodies never on the Trade Register), plus a few private companies dissolved/merged before the bulk export was taken | `MANUAL` |
   | — | none of the above | genuinely unresolvable, or not an OID at all | `NA` |

   PRH's bulk export only contains companies *currently* on the Trade Register, so a company
   that was dissolved or merged before the export was taken won't be found there even though
   it's a real historical entity — that's why some private companies end up needing a
   `MANUAL` entry too, not just public bodies.

   Every entry added to `MANUAL_NAMES_BY_CODE` was checked individually (live PRH per-company
   lookup and/or a public business-registry search, e.g. asiakastieto.fi) before being
   hardcoded — see the comment above the dict. Its names are the *historical* trade name in
   use at the time, which can differ from an entity's current name after a later sale, rename,
   or municipal merger (e.g. a small municipal department's Y-tunnus later absorbed into a
   regional kuntayhtymä).

4. **Write the result** to `<input_file>.coding_table.txt`, a tab-separated file with columns
   `CODE`, `NAME`, `COUNT`, `SOURCE`, sorted by `COUNT` descending.

## Caching

Everything is cached in the current working directory, keyed off the input filename where it
makes sense to, so re-running the script (e.g. after adding a `MANUAL_NAMES_BY_CODE` entry, or
against a newer data release) doesn't redo work:

| File | What it holds | Notes |
|------|----------------|-------|
| `<input_file>.coding_map_counts.tsv` | Raw value counts from clickhouse | Delete to re-run the query (e.g. against updated data) |
| `<input_file>.coding_table.txt` | The final resolved table | Delete to force full re-resolution; also needs deleting after any change to `MANUAL_NAMES_BY_CODE`, `CODE_ROOTS`/`CODE_PATTERNS`, or the resolution order, since it's read back as a cache of prior answers |
| `all_companies.zip` | Raw PRH bulk export (~1.4GB uncompressed) | Shared across input files; delete to force a fresh download |
| `companies.pkl` | `business_id -> name` index built from `all_companies.zip` | Delete together with `all_companies.zip` to refresh against PRH's current registry |
| `ptv.pkl` | `business_id -> name or None` answers from PTV | Expanded incrementally; confirmed misses are cached too so they aren't re-queried |

`companies.pkl` and `ptv.pkl` are answer caches, not raw data — once built, the script never
re-queries a business id that's already in them, even across different input files/runs. If
PRH or PTV's data changes (e.g. a company you previously found missing gets registered later),
you need to delete the relevant `.pkl` to see it.

## Relationship to kanta.engine

`kanta.engine`'s `get_thl_manual_map()` (`src/kanta/engine/reference_data.py`) reads
`src/kanta/engine/data/thl_coding_manual_mapping.txt` as a `CODE`/`NAME` TSV (with a header,
`COUNT`/`SOURCE` ignored) to resolve `CODING_SYSTEM` values that the primary official THL
mapping (`THL_SOTE_MAP_FILE`) doesn't cover. That file's format is exactly this script's output
format, so `coding_table.txt` can be copied over it directly to update the engine's mapping —
just make sure the header is present and columns are named `CODE`/`NAME`/`COUNT`/`SOURCE`
(older versions of that file were headerless 2-column `code`/`label` and are **not**
compatible: reading a 4-column file with the old loader silently misassigns columns instead of
erroring, so don't mix old and new formats).
