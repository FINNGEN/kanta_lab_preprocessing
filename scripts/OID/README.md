# OID

ROWID → record OID lookup table, a bridge from the lab rows to the statement data ([#67](https://github.com/FINNGEN/kanta_lab_preprocessing/issues/67)).

## Main

**`oid_map.py`**: the intake drops the record OIDs (`asiakirjaoid`, `merkintaoid`, `entryoid`) and `load_id` / `file_name` in tidyup. Both the assembled and the tidyup parquet keep `_rowid_source`, the row's position in the assembled file. The script joins the two files on it, so each ROWID gets its OIDs back. The engine doesn't change ROWID, so the table joins on `ROWID` to the tidyup, engine and release outputs alike.

```
python3 oid_map.py --assembled <x>_assembled.parquet --tidyup <x>.parquet [--output out.parquet] [--lookup x.npy] [--batch-size 10000000] [--test]
```

The intake WDL (`wdl/intake.wdl`) runs it at the end of the `tidyup` task and outputs `<prefix>_oid_map.parquet` as `oid_map`.

Writes `<tidyup name>_oid_map.parquet` (`_oid_map_test.parquet` with `--test`) in the current dir: one row per tidyup row, sorted by `ROWID_SOURCE`:

```
ROWID  ROWID_SOURCE  asiakirjaoid  merkintaoid  entryoid  load_id  file_name
```

- `--tidyup` also takes the engine output (columns `ROWID` / `_rowid_source`). Rows the engine filtered out are then missing.
- The two files must come from the same run, because `_rowid_source` is only meaningful within it. The script checks that every tidyup row finds its assembled row and exits with status 1 otherwise.
- Rows dropped as duplicates in tidyup have no ROWID, so they are not in the table. Their OIDs, possibly from other documents, are in `<x>_duplicates.parquet`, keyed by `_rowid_source`.
- The OIDs are pseudonymised record IDs, so the table needs the same handling as the source data.

### How it works

`_rowid_source` values are unique and cover 1…N with no gaps (they are the assembled row positions), so the join is an array lookup:

1. **Lookup array**: one pass over the two integer columns of the tidyup file fills `rowid_by_source[_rowid_source] = ROWID`, with 0 for rows not in tidyup. It's int32, so about 1.2 GB for 300M assembled rows, plus a temporary 1-byte-per-row bitmap that catches repeated ROWIDs.
2. **OIDs**: one pass over the assembled OID columns, in batches of `--batch-size` rows (default 10M). Each batch looks up its ROWIDs, keeps the rows that have one and adds them to one zstd parquet. The output is already in `ROWID_SOURCE` order, with no join or sort.

Memory is the array plus one batch of OID strings.

### Cached lookup array

The array is saved as `<tidyup name>_oid_lookup.npy` next to the output (`--lookup` to change it). A `.json` sidecar holds the pass-1 counts and a fingerprint of the inputs: paths, sizes, modification times, the assembled row count and a format version.

- Later runs, test or full, load the array memory-mapped and skip pass 1.
- If the inputs no longer match the fingerprint, the array is rebuilt, and the log says what changed.
- Delete the `.npy` and the `.json` to force a rebuild.
- The array always covers all assembled rows, so the cache from a `--test` run is reused by the full run.

### Test mode

`--test` writes only the first 100k assembled rows, in batches of 25k. Pass 1 still reads the whole tidyup file in big batches, because those 100k source rows are spread across all of it. Pass 1 is the slow part, and only runs when there is no valid cache.

The log prints progress for both passes and a summary:

- source rows not in tidyup (duplicates)
- unmatched rows (should be 0)
- repeated ROWID / ROWID_SOURCE (should be 0)
- null/`NA` counts per OID column

## Selecting kanta rows by OID

**`oid_select.py`**: the kanta rows whose record OIDs appear in another file, e.g. the statement data (`finngen_R12_kanta_laboratory_reports_internal_1.0.txt.gz`, with `asiakirjaoid_pseudo`, `merkintaoid_pseudo`, `entryoid_pseudo`, ...).

```
python3 oid_select.py --map <x>_oid_map.parquet --kanta <x>_RELEASE.parquet --statements <statements>.txt.gz [--keys ...] [--output out.parquet|out.tsv] [--links links.parquet] [--test]
```

1. Reads the distinct key values of the statements file (tab-separated, read like the intake reads the raw files). Key `k` comes from column `k_pseudo`, or `k` if there is none. Null / `NA` keys are dropped.
2. Finds the map rows with those values, which gives the matching ROWIDs. All `--keys` must match the same statement row. The default is all five OIDs, the key the old pre-merge WDL joined responses and reports on (`wdl/pre-merge.wdl` at commit `13e8e74`). Each key's own counts are printed too, showing which OIDs link the two files.
3. Writes the kanta rows with those ROWIDs, with all their columns and the header, to `--output`. The default is `<statements name>_kanta_rows.parquet`; `.tsv` writes tab-separated text.

- `--kanta` is the tidyup, engine or release parquet of the **same intake run** as the map.
- `--links` also writes the ROWID ↔ key table, to join the selected rows back to the statements.
- `--test` reads only the first 1k statement rows. The map and kanta passes still read the whole files.
- Two things show up in the summary: statement keys that aren't in the map (other individuals, documents not in this intake, or a different pseudonymisation), and matched ROWIDs that aren't in the kanta file (filtered by the engine or not released).

## Temporary: `oid_shapes.py`

A diagnostic for when `oid_select.py` finds no match. For each OID column it prints the *shape* of the values in the map and in the statements file (digits → `9`, letters → `a`, first 40 characters, plus the length), and the null/`NA` counts, but never the values themselves. If the map columns are all null, the map build lost the OIDs. If the shapes differ, the formats differ. If the shapes are the same and nothing matches, the files were pseudonymised differently. To be deleted once the link is sorted.

```
python3 oid_shapes.py --map <x>_oid_map.parquet --statements <statements>.txt.gz [--map-rows 100000] [--statement-rows 1000]
```

## Temporary: `oid_key_check.py`

Checks whether the R12 statements and the R15 lab data share OIDs, on events known to have a statement. It takes the first `--n` (default 100) rows with a statement in the old v2 release (responses + reports, no OID columns), then:
- finds the same events in the new release by `FINNGENID` + `APPROX_EVENT_DAY` + `TIME`, and gets their R15 OIDs from the map;
- finds the statements in the R12 reports file by organisation + status + first statement text (not by date, which can differ), and gets their R12 OIDs;
- compares the two per key and as a full tuple, and also checks whether the R15 OIDs appear anywhere in the reports file.

```
python3 oid_key_check.py --old <R12 v2>.tsv.gz --new <R15>_RELEASE.parquet --map <R15>_oid_map.parquet --reports <R12 reports>.txt.gz [--n 100] [--output details.tsv]
```

It prints counts only. `--output` holds FINNGENIDs and OIDs, so it stays on the data side.

Statements are sparse, so the sample rows are cached in `--cache` (default `<old name>_statement_sample.parquet` in the current folder, plus a `.json` sidecar). The cache is reused when the old file is unchanged and `--n` is the same or smaller; delete both files to rebuild it. It holds FINNGENIDs and statement texts, so it stays on the data side too.
