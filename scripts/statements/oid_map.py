#!/usr/bin/env python3
"""ROWID -> record OID lookup table, a bridge from the lab rows to the statement data (issue #67).

The intake drops the record OIDs (asiakirjaoid, merkintaoid, entryoid) and load_id / file_name in tidyup, but both
the assembled and the tidyup parquet keep _rowid_source, the row's position in the assembled file. This script joins
the two on it:

    assembled:  _rowid_source, main.asiakirjaoid, main.merkintaoid, main.entryoid, main.load_id, main.file_name
    tidyup:     _rowid (ROWID), _rowid_source
    ->          ROWID, ROWID_SOURCE, asiakirjaoid, merkintaoid, entryoid, load_id, file_name

ROWID is kept unchanged by the engine, so the table works with the tidyup, engine and release outputs alike. One
row per tidyup row (ROWID and ROWID_SOURCE are both unique), sorted by ROWID_SOURCE.

Notes
-----
- The assembled and tidyup files must come from the same run: _rowid_source is only meaningful within it.
- Rows dropped as duplicates in tidyup have no ROWID, so their OIDs (possibly from other documents) are not in the
  table. They are in <tidyup>_duplicates.parquet, keyed by _rowid_source.
- The engine output also works as --tidyup (it has ROWID and _rowid_source); rows it filtered out are then missing.

How it works (lookup array, no join)
-----------------------------------
_rowid_source is unique and dense (1..N, the assembled row positions), so the join is an array lookup:
1. One pass over the two integer columns of the tidyup file fills rowid_by_source[_rowid_source] = ROWID
   (0 = not in tidyup). int32 while N < 2^31: ~4 bytes per assembled row, e.g. ~1.2 GB for 300M rows, plus a
   1-byte-per-row bitmap to catch repeated ROWIDs.
2. One pass over the assembled OID columns, in batches of --batch-size rows: each batch looks up its ROWIDs, keeps
   the rows that have one and appends them to the output parquet. The assembled file is in _rowid_source order, so
   the output is too, with no sort.
Memory: the arrays plus one batch of OID strings.

The lookup array is cached as <tidyup name>_oid_lookup.npy (+ .json with the pass-1 counts and the inputs'
fingerprint) next to the output, and reused (memory-mapped) by later runs, test or full. It is rebuilt when the
inputs no longer match the fingerprint; delete both files to force a rebuild.

Usage
-----
  python3 scripts/statements/oid_map.py --assembled <x>_assembled.parquet --tidyup <x>.parquet [--test]

--test only writes the first 100k rows of the assembled file (_rowid_source <= 100k), in small batches. The lookup
array is always built in full (the first 100k source rows are spread over the whole tidyup file anyway), so a test
run leaves a cache the full run reuses.
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

TEST_ROWS = 100_000
TEST_BATCH_SIZE = 25_000
DEFAULT_BATCH_SIZE = 10_000_000

SOURCE_ID = "_rowid_source"
OID_COLUMNS = ["asiakirjaoid", "merkintaoid", "entryoid", "load_id", "file_name"]
# Main and freetext carry the same OIDs (assemble checks they are equal), so the main ones are used.
ASSEMBLED_PREFIX = "main."

# Column names in the tidyup output, or in the engine output.
ROWID_NAMES = ["_rowid", "ROWID"]
SOURCE_ID_NAMES = ["_rowid_source", "ROWID_SOURCE"]

OUT_SCHEMA = pa.schema(
    [("ROWID", pa.int64()), ("ROWID_SOURCE", pa.int64())] + [(c, pa.string()) for c in OID_COLUMNS]
)


def pick_column(names: list[str], wanted: list[str], path: Path) -> str:
    """The first of the wanted names present in the file's columns."""
    for name in wanted:
        if name in names:
            return name
    sys.exit(f"{path}: none of the columns {wanted} found")


def check_assembled(path: Path) -> None:
    names = pq.ParquetFile(path).schema_arrow.names
    missing = [c for c in [SOURCE_ID] + [ASSEMBLED_PREFIX + c for c in OID_COLUMNS] if c not in names]
    if missing:
        sys.exit(f"{path}: missing columns {missing} (is it the assembled file?)")


def to_int64(column: pa.ChunkedArray | pa.Array, name: str) -> np.ndarray:
    if column.null_count:
        sys.exit(f"{name}: {column.null_count:,} null values")
    return np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.int64)


def build_lookup(tidyup: Path, rowid_col: str, source_col: str, n_rows: int, batch_size: int):
    """rowid_by_source[s] = ROWID of the tidyup row with _rowid_source s (0: none), for s in 1..n_rows.

    ROWID can't exceed the tidyup row count, which can't exceed the assembled row count, so n_rows bounds both."""
    dtype = np.int32 if n_rows < 2**31 else np.int64
    rowid_by_source = np.zeros(n_rows + 1, dtype=dtype)
    rowid_seen = np.zeros(n_rows + 1, dtype=bool)
    stats = {"tidyup_rows": 0, "in_range": 0, "source_out_of_range": 0, "rowid_out_of_range": 0, "rowid_repeated": 0}

    t0 = time.time()
    next_log = batch_size
    for batch in pq.ParquetFile(tidyup).iter_batches(batch_size=batch_size, columns=[rowid_col, source_col]):
        rid = to_int64(batch.column(rowid_col), rowid_col)
        src = to_int64(batch.column(source_col), source_col)
        stats["tidyup_rows"] += len(rid)

        bad_rid = (rid < 1) | (rid > n_rows)
        stats["rowid_out_of_range"] += int(bad_rid.sum())
        # Repeated ROWIDs are counted at the end (valid ROWIDs - bits set): no per-batch sort (np.unique took
        # longer than the rest of the pass).
        rowid_seen[rid[~bad_rid]] = True

        bad_src = (src < 1) | (src > n_rows)
        stats["source_out_of_range"] += int(bad_src.sum())
        in_range = ~bad_src & ~bad_rid
        stats["in_range"] += int(in_range.sum())
        rowid_by_source[src[in_range]] = rid[in_range]
        if stats["tidyup_rows"] >= next_log:
            print(f"  tidyup rows {stats['tidyup_rows']:,} ({time.time() - t0:.0f}s)", flush=True)
            next_log = (stats["tidyup_rows"] // batch_size + 1) * batch_size

    stats["rowid_repeated"] = stats["tidyup_rows"] - stats["rowid_out_of_range"] - int(np.count_nonzero(rowid_seen))
    filled = int(np.count_nonzero(rowid_by_source))
    # A slot assigned twice means the same _rowid_source on two tidyup rows.
    stats["source_repeated"] = stats["in_range"] - filled
    stats["filled"] = filled
    del rowid_seen
    return rowid_by_source, stats


# Bump when build_lookup changes, so older caches are rebuilt.
LOOKUP_FORMAT = 1


def fingerprint(assembled: Path, tidyup: Path, rowid_col: str, source_col: str) -> dict:
    """What the lookup array depends on: the tidyup file and the assembled row count (its bounds)."""
    st_t, st_a = tidyup.stat(), assembled.stat()
    return {
        "format": LOOKUP_FORMAT,
        "tidyup": str(tidyup.resolve()),
        "tidyup_size": st_t.st_size,
        "tidyup_mtime_ns": st_t.st_mtime_ns,
        "rowid_col": rowid_col,
        "source_col": source_col,
        "assembled": str(assembled.resolve()),
        "assembled_size": st_a.st_size,
        "assembled_mtime_ns": st_a.st_mtime_ns,
        "assembled_rows": pq.ParquetFile(assembled).metadata.num_rows,
    }


def load_lookup(cache: Path, expected: dict):
    """The cached array (memory-mapped) and pass-1 counts, or None if missing or built from other inputs."""
    meta_file = cache.with_suffix(".json")
    if not (cache.exists() and meta_file.exists()):
        print(f"  no cache at {cache}, building")
        return None
    meta = json.loads(meta_file.read_text())
    changed = [k for k, v in expected.items() if meta.get("fingerprint", {}).get(k) != v]
    if changed:
        print(f"  cache {cache} is stale ({', '.join(changed)} changed), rebuilding")
        return None
    rowid_by_source = np.load(cache, mmap_mode="r")
    if len(rowid_by_source) != expected["assembled_rows"] + 1:
        print(f"  cache {cache} has the wrong length, rebuilding")
        return None
    print(f"  using cache {cache} (delete it and {meta_file.name} to force a rebuild)")
    return rowid_by_source, meta["stats"]


def save_lookup(cache: Path, rowid_by_source: np.ndarray, stats: dict, fp: dict) -> None:
    """Write the array, then the .json: a cache counts only once its .json exists. Temporary names + rename, so an
    interrupted run never leaves a half-written cache."""
    tmp_npy = cache.with_name(cache.name + ".tmp.npy")
    np.save(tmp_npy, rowid_by_source)
    os.replace(tmp_npy, cache)
    meta_file = cache.with_suffix(".json")
    tmp_json = meta_file.with_name(meta_file.name + ".tmp")
    tmp_json.write_text(json.dumps({"fingerprint": fp, "stats": stats}, indent=2) + "\n")
    os.replace(tmp_json, meta_file)
    print(f"  cached to {cache} ({cache.stat().st_size / 2**30:.2f} GiB)")


def count_missing(column: pa.Array) -> int:
    """Null or literal "NA" values."""
    return column.null_count + int(pc.sum(pc.fill_null(pc.equal(column, "NA"), False)).as_py() or 0)


def write_map(assembled: Path, output: Path, rowid_by_source: np.ndarray, n_rows: int, batch_size: int):
    """Stream the assembled OID columns, keep the rows with a ROWID, write them out."""
    columns = [SOURCE_ID] + [ASSEMBLED_PREFIX + c for c in OID_COLUMNS]
    stats = {"read": 0, "written": 0, "unsorted": False}
    missing = dict.fromkeys(OID_COLUMNS, 0)
    prev_max = 0
    next_log = batch_size
    t0 = time.time()
    with pq.ParquetWriter(output, OUT_SCHEMA, compression="zstd") as writer:
        for batch in pq.ParquetFile(assembled).iter_batches(batch_size=batch_size, columns=columns):
            src = to_int64(batch.column(SOURCE_ID), SOURCE_ID)
            if len(src) == 0:
                continue
            # The assembled file is written in _rowid_source order; early stopping in test mode relies on it.
            if src[0] <= prev_max or np.any(np.diff(src) <= 0):
                stats["unsorted"] = True
            prev_max = max(prev_max, int(src.max()))

            in_range = (src >= 1) & (src <= n_rows)
            rid = np.zeros(len(src), dtype=np.int64)
            rid[in_range] = rowid_by_source[src[in_range]]
            keep = rid > 0
            stats["read"] += int(in_range.sum())

            if keep.any():
                mask = pa.array(keep)
                table = pa.table(
                    [pa.array(rid[keep]), pa.array(src[keep])]
                    + [pc.filter(batch.column(ASSEMBLED_PREFIX + c), mask).cast(pa.string()) for c in OID_COLUMNS],
                    schema=OUT_SCHEMA,
                )
                writer.write_table(table)
                stats["written"] += table.num_rows
                for c in OID_COLUMNS:
                    missing[c] += count_missing(table.column(c).combine_chunks())

            if stats["read"] >= next_log or prev_max >= n_rows:
                print(
                    f"  assembled rows {stats['read']:,}/{n_rows:,}, written {stats['written']:,}"
                    f" ({time.time() - t0:.0f}s)",
                    flush=True,
                )
                next_log = (stats["read"] // batch_size + 1) * batch_size
            if prev_max >= n_rows and not stats["unsorted"]:
                break
    return stats, missing


def main(args) -> None:
    started = time.time()
    check_assembled(args.assembled)
    tidyup_names = pq.ParquetFile(args.tidyup).schema_arrow.names
    rowid_col = pick_column(tidyup_names, ROWID_NAMES, args.tidyup)
    source_col = pick_column(tidyup_names, SOURCE_ID_NAMES, args.tidyup)

    n_assembled = pq.ParquetFile(args.assembled).metadata.num_rows
    n_tidyup = pq.ParquetFile(args.tidyup).metadata.num_rows
    n_rows = min(n_assembled, TEST_ROWS) if args.test else n_assembled
    # The lookup pass always reads the whole tidyup file, so it always uses big batches; only the output pass is cut
    # down in test mode.
    lookup_batch_size = args.batch_size or DEFAULT_BATCH_SIZE
    batch_size = args.batch_size or (TEST_BATCH_SIZE if args.test else DEFAULT_BATCH_SIZE)

    print("=== OID MAP ===")
    print(f"assembled: {args.assembled} ({n_assembled:,} rows)")
    print(f"tidyup:    {args.tidyup} ({n_tidyup:,} rows; columns {rowid_col}, {source_col})")
    print(f"output:    {args.output}")
    print(f"lookup:    {args.lookup}")
    print(f"mode:      {'TEST, ' if args.test else ''}_rowid_source 1..{n_rows:,}, batches of {batch_size:,}")

    print(f"# 1/2 Lookup array from tidyup (all {n_assembled:,} source rows)")
    t0 = time.time()
    fp = fingerprint(args.assembled, args.tidyup, rowid_col, source_col)
    cached = load_lookup(args.lookup, fp)
    if cached is None:
        rowid_by_source, lk = build_lookup(args.tidyup, rowid_col, source_col, n_assembled, lookup_batch_size)
        save_lookup(args.lookup, rowid_by_source, lk, fp)
    else:
        rowid_by_source, lk = cached
    print(
        f"  {lk['tidyup_rows']:,} tidyup rows, {lk['filled']:,} with a source row"
        f" ({rowid_by_source.dtype}, {rowid_by_source.nbytes / 2**30:.2f} GiB, {time.time() - t0:.0f}s)"
    )

    print("# 2/2 OIDs from assembled")
    wr, missing = write_map(args.assembled, args.output, rowid_by_source, n_rows, batch_size)
    # Tidyup rows whose source row is in the covered range but wasn't found in the assembled file.
    filled = lk["filled"] if n_rows == n_assembled else int(np.count_nonzero(rowid_by_source[1 : n_rows + 1]))
    unmatched = filled - wr["written"]

    print("# Summary")
    print(f"rows written:                         {wr['written']:,}")
    print(f"source rows covered:                  {wr['read']:,} of {n_rows:,}")
    print(f"source rows not in tidyup (dups etc): {wr['read'] - wr['written']:,}")
    print(f"tidyup rows with no assembled match:  {unmatched:,}  (should be 0)")
    print(f"tidyup _rowid_source out of 1..N:     {lk['source_out_of_range']:,}  (should be 0)")
    print(f"repeated ROWID_SOURCE:                {lk['source_repeated']:,}  (should be 0)")
    print(f"repeated ROWID:                       {lk['rowid_repeated']:,}  (should be 0)")
    print(f"ROWID out of range:                   {lk['rowid_out_of_range']:,}  (should be 0)")
    for c in OID_COLUMNS:
        print(f"null/NA {c + ':':<29} {missing[c]:,}")

    problems = []
    if wr["read"] != n_rows:
        problems.append(f"assembled file covers {wr['read']:,} of 1..{n_rows:,}")
    if wr["unsorted"]:
        problems.append("assembled file not in _rowid_source order")
    if unmatched or lk["source_repeated"] or lk["rowid_repeated"] or lk["rowid_out_of_range"]:
        problems.append("see counts above")
    if lk["source_out_of_range"]:
        problems.append("tidyup _rowid_source outside the assembled rows: the files are probably not from the same run")
    if problems:
        for p in problems:
            print(f"!!! {p}")
        print("!!! Checks failed !!!")
        sys.exit(1)
    print(f"Done in {time.time() - started:.0f}s.")


def init_cli():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--assembled", required=True, type=Path, help="Assembled parquet (intake.assemble output)")
    parser.add_argument(
        "--tidyup", required=True, type=Path, help="Tidyup parquet (intake.tidyup output) or the engine output"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output parquet (default: <tidyup name>_oid_map[_test].parquet in the current folder)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help=f"Rows read per batch (default {DEFAULT_BATCH_SIZE:,}, test {TEST_BATCH_SIZE:,})",
    )
    parser.add_argument(
        "--lookup",
        type=Path,
        default=None,
        help="Cached lookup array (default: <tidyup name>_oid_lookup.npy next to the output, with a .json sidecar)."
        " Reused when the inputs match; delete to force a rebuild.",
    )
    parser.add_argument("--test", action="store_true", help=f"Only the first {TEST_ROWS:,} assembled rows")
    args = parser.parse_args()
    if args.output is None:
        args.output = Path(f"{args.tidyup.stem}_oid_map{'_test' if args.test else ''}.parquet")
    if args.lookup is None:
        args.lookup = args.output.parent / f"{args.tidyup.stem}_oid_lookup.npy"
    if args.lookup.suffix != ".npy":
        sys.exit("--lookup must end in .npy")
    if args.output.exists():
        sys.exit(f"{args.output} already exists. Aborting.")
    if args.batch_size is not None and args.batch_size < 1:
        sys.exit("--batch-size must be positive")
    return args


if __name__ == "__main__":
    main(init_cli())
