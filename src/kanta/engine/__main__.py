import math
import multiprocessing as mp
import os
from argparse import ArgumentParser
from functools import partial
from pathlib import Path

from tqdm import tqdm

from kanta import output
from kanta.engine import chunking, processing, reference_data
from kanta.engine.errors import ABBR_EMPTY_SCHEMA, EMPTY_SCHEMA, UNIT_EMPTY_SCHEMA


def main(
    input_file: Path,
    output_file: Path,
    errors_file: Path,
    abbr_file: Path,
    unit_file: Path,
    tmp_dir: Path,
    *,
    is_test_run=False,
    n_workers=1,
    chunk_size=chunking.N_LINES_PER_CHUNK,
    verbose=False,
):
    # Setup
    processing.configure_pandas()

    # Per-run pickle cache for reference tables (see reference_data.py's module docstring):
    # created fresh here, populated once below by warm_all(), then read (never written) by
    # worker processes — regenerated every run, never reused across separate invocations.
    cache_dir = tmp_dir / "refcache"
    cache_dir.mkdir()
    reference_data.set_cache_dir(cache_dir)

    # Load every reference table once up front, regardless of --verbose, so remote-fetch
    # fallback warnings (and basic load confirmation) always surface exactly once, here — not
    # potentially once per worker process later. Actual chunk processing loads these tables
    # quietly (see reference_data.warm_all()'s docstring).
    reference_data.warm_all()

    # Per-chunk filter debugging output is only meaningful for a single chunk (--test): in a
    # full run it would print once per chunk, since it reports business-logic details of the
    # chunk currently being processed, not something a one-time warm-up can cover.
    verbose = verbose and is_test_run

    chunks_dir = tmp_dir / "chunks"
    chunks_dir.mkdir()

    errors_dir = tmp_dir / "errors"
    errors_dir.mkdir()

    abbr_dir = tmp_dir / "abbr"
    abbr_dir.mkdir()

    unit_dir = tmp_dir / "unit"
    unit_dir.mkdir()

    # Iterate over each chunk. Wrapped to count actual input rows consumed as a side effect —
    # equals the full file's row count normally, but only the first chunk's size for --test,
    # where the row-conservation check below needs to compare against what was *actually* fed
    # into the pipeline, not the whole file.
    n_input_rows = [0]

    def _counted_chunks(iterable):
        for indexed_chunk in iterable:
            n_input_rows[0] += len(indexed_chunk[1])
            yield indexed_chunk

    iter_indexed_chunks = _counted_chunks(
        chunking.chunk_iterator(input_file, is_test_run=is_test_run, chunk_size=chunk_size)
    )
    num_rows = chunking.count_rows(input_file)
    total_chunks = 1 if is_test_run else math.ceil(num_rows / chunk_size)
    print(f"Input: {num_rows:,} rows -> {total_chunks:,} chunks of up to {chunk_size:,} rows each")

    process_func = partial(
        processing.process_chunk,
        chunks_dir=chunks_dir,
        errors_dir=errors_dir,
        abbr_dir=abbr_dir,
        unit_dir=unit_dir,
        verbose=verbose,
    )

    if n_workers > 1:
        process_in_parallel(
            process_func, iter_indexed_chunks, n_workers=n_workers, total=total_chunks, cache_dir=cache_dir
        )
    else:
        for indexed_chunk in tqdm(iter_indexed_chunks, total=total_chunks, desc="Processing chunks"):
            process_func(indexed_chunk)

    chunking.concatenate_chunks(chunks_dir, output_file)
    chunking.concatenate_chunks(errors_dir, errors_file, empty_schema=EMPTY_SCHEMA)
    chunking.concatenate_chunks(abbr_dir, abbr_file, empty_schema=ABBR_EMPTY_SCHEMA)
    chunking.concatenate_chunks(unit_dir, unit_file, empty_schema=UNIT_EMPTY_SCHEMA)

    # Every input row must land in exactly one of output_file (kept) or errors_file (dropped) —
    # abbr_file/unit_file are side-channel change-logs, not exclusive of the main output, so
    # they're not part of this count.
    n_output = chunking.count_rows(output_file)
    n_errors = chunking.count_rows(errors_file)
    print(f"Output: {n_output:,} rows + {n_errors:,} dropped/errored = {n_output + n_errors:,}")
    assert n_input_rows[0] == n_output + n_errors, (
        f"Row count mismatch: {n_input_rows[0]:,} input rows but {n_output:,} output + "
        f"{n_errors:,} errors = {n_output + n_errors:,} rows — rows were lost or duplicated"
    )


def process_in_parallel(
    func, indexed_chunks, *, n_workers: int, cache_dir: Path, total: int | None = None
):
    """Process the chunks in parallel using `n_workers` spawned processes."""
    # Explicitly use the "spawn" method to create workers for consistent behavior across OSes
    # and Python versions.
    ctx = mp.get_context("spawn")

    # Setting the `initializer` here is required since we used the "spawn" method above to
    # start workers: they start with no configuration, so we must provide it (pandas options,
    # and where to find the reference-data pickle cache main() already populated).
    #
    # NOTE(Vincent 2026-06-17):
    # It looks like `multiprocessing.Pool` has a non-trivial silent failure mode: if a
    # worker process get OOM-killed (i.e. because available memory is low) then the killed
    # worker will fail silently and the Pool will hang forever.
    # IMHO we should leave as it is for now and make sure to monitor memory usage. The future
    # rewrite to Polars will remove the use of `multiprocessing` and this problem.
    # See: https://bugs.python.org/issue22393
    with ctx.Pool(n_workers, initializer=processing.init_worker, initargs=(cache_dir,)) as pool:
        for _result in tqdm(
            pool.imap_unordered(func, indexed_chunks), total=total, desc="Processing chunks"
        ):
            # Consume the iterator, discard the result since it's written to disk.
            pass


def init_cli():
    parser = ArgumentParser(
        description="Kanta Lab preprocessing pipeline: raw data ⇒ clean data."
    )
    parser.add_argument(
        "--input-file",
        type=Path,
        help="Path to the Kanta Lab data file coming from the intake stage (Parquet)",
        required=True,
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Process only the first chunk (useful for debugging). "
            "This overwrites --n-workers to 1."
        ),
        required=False,
    )
    parser.add_argument(
        "--output-prefix",
        type=Path,
        help=(
            "Prefix for output file paths (Parquet). Produces <prefix>.parquet for the "
            "cleaned data, <prefix>_errors.parquet for rows dropped/flagged by filters, "
            "<prefix>_abbr.parquet for TEST_NAME_ABBREVIATION changes, and "
            "<prefix>_unit.parquet for MEASUREMENT_UNIT changes. Future output files "
            "follow the same <prefix>_<name>.parquet convention."
        ),
        required=True,
    )
    parser.add_argument(
        "--n-workers",
        type=int,
        default=os.process_cpu_count() or 1,
        help=(
            "Number of worker processes used to process chunks in parallel. "
            "Defaults to the number of available CPUs. Use 1 to run serially "
            "(useful for debugging)."
        ),
        required=False,
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=chunking.N_LINES_PER_CHUNK,
        help=(
            f"Number of rows per chunk when streaming the input Parquet file. "
            f"Defaults to {chunking.N_LINES_PER_CHUNK}. Independent of --n-workers "
            "(see chunking.py for why scaling it by worker count is a bad idea)."
        ),
        required=False,
    )
    parser.add_argument(
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
    )
    parser.add_argument(
        "--verbose",
        help=(
            "Print per-filter debugging output (e.g. mapping counts) to screen. Only takes "
            "effect together with --test — ignored in a full run, since it reports the "
            "currently-processed chunk's own business-logic details, which would otherwise "
            "print once per chunk. Reference-table load diagnostics are separate: those always "
            "print once at startup regardless of this flag (see reference_data.warm_all())."
        ),
        action="store_true",
    )

    args = parser.parse_args()

    if args.n_workers < 1:
        raise ValueError("--n-workers must be 1 or more")

    if args.chunk_size < 1:
        raise ValueError("--chunk-size must be 1 or more")

    if args.test:
        args.n_workers = 1

    return args


if __name__ == "__main__":
    args = init_cli()

    output_file = output.derive_output_path(args.output_prefix)
    errors_file = output.derive_output_path(args.output_prefix, "_errors")
    abbr_file = output.derive_output_path(args.output_prefix, "_abbr")
    unit_file = output.derive_output_path(args.output_prefix, "_unit")

    output.check_safe_write(output_file)
    output.check_safe_write(errors_file)
    output.check_safe_write(abbr_file)
    output.check_safe_write(unit_file)
    tmp_dir = output.create_tmp_dir()

    main(
        args.input_file,
        output_file,
        errors_file,
        abbr_file,
        unit_file,
        tmp_dir,
        is_test_run=args.test,
        n_workers=args.n_workers,
        chunk_size=args.chunk_size,
        verbose=args.verbose,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
