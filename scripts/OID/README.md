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
