import logging
import math
import multiprocessing as mp
import time
from functools import partial
from pathlib import Path

from tqdm import tqdm

from kanta import log_utils
from kanta.engine import chunking, processing, reference_data, release
from kanta.engine.errors import ABBR_EMPTY_SCHEMA, EMPTY_SCHEMA, UNIT_EMPTY_SCHEMA

logger = logging.getLogger(__name__)


def main(
    input_file: Path,
    output_file: Path,
    errors_file: Path,
    abbr_file: Path,
    unit_file: Path,
    tmp_dir: Path,
    *,
    release_file: Path | None = None,
    is_test_run=False,
    n_workers=1,
    chunk_size=chunking.N_LINES_PER_CHUNK,
    verbose=False,
):
    # Setup
    processing.configure_pandas()

    # Per-run pickle cache for reference tables, populated below by warm_all().
    cache_dir = tmp_dir / "refcache"
    cache_dir.mkdir()
    reference_data.set_cache_dir(cache_dir)

    # Load every reference table once, up front, before workers start.
    reference_data.warm_all()

    # Per-filter debug output only makes sense for a single chunk (--test).
    verbose = verbose and is_test_run

    chunks_dir = tmp_dir / "chunks"
    chunks_dir.mkdir()

    errors_dir = tmp_dir / "errors"
    errors_dir.mkdir()

    abbr_dir = tmp_dir / "abbr"
    abbr_dir.mkdir()

    unit_dir = tmp_dir / "unit"
    unit_dir.mkdir()

    release_dir = None
    if release_file is not None:
        release_dir = tmp_dir / "release"
        release_dir.mkdir()

    # Counts rows actually consumed (only the first chunk under --test), for the
    # row-conservation check below.
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
    logger.info(f"Input: {num_rows:,} rows -> {total_chunks:,} chunks of up to {chunk_size:,} rows each")

    process_func = partial(
        processing.process_chunk,
        chunks_dir=chunks_dir,
        errors_dir=errors_dir,
        abbr_dir=abbr_dir,
        unit_dir=unit_dir,
        release_dir=release_dir,
        verbose=verbose,
    )

    chunking_start = time.perf_counter()
    if n_workers > 1:
        process_in_parallel(
            process_func, iter_indexed_chunks, n_workers=n_workers, total=total_chunks, cache_dir=cache_dir
        )
    else:
        for indexed_chunk in tqdm(iter_indexed_chunks, total=total_chunks, desc="Processing chunks"):
            process_func(indexed_chunk)
    logger.info(f"Processed {total_chunks:,} chunks in {log_utils.format_duration(time.perf_counter() - chunking_start)}")

    chunking.concatenate_chunks(chunks_dir, output_file)
    chunking.concatenate_chunks(errors_dir, errors_file, empty_schema=EMPTY_SCHEMA)
    chunking.concatenate_chunks(abbr_dir, abbr_file, empty_schema=ABBR_EMPTY_SCHEMA)
    chunking.concatenate_chunks(unit_dir, unit_file, empty_schema=UNIT_EMPTY_SCHEMA)
    if release_file is not None:
        chunking.concatenate_chunks(release_dir, release_file, empty_schema=release.RELEASE_EMPTY_SCHEMA)

    # Every input row must land in exactly one of output_file or errors_file.
    n_output = chunking.count_rows(output_file)
    n_errors = chunking.count_rows(errors_file)
    logger.info(f"Output: {n_output:,} rows + {n_errors:,} dropped/errored = {n_output + n_errors:,}")
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
