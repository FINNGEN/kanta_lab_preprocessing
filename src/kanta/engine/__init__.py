import multiprocessing as mp
from functools import partial
from pathlib import Path

from kanta.engine import chunking, processing


def main(
    input_file: Path,
    output_file: Path,
    tmp_dir: Path,
    *,
    is_test_run=False,
    n_workers=1,
):
    # Setup
    processing.configure_pandas()

    chunks_dir = tmp_dir / "chunks"
    chunks_dir.mkdir()

    # Iterate over each chunk
    iter_indexed_chunks = chunking.chunk_iterator(input_file, is_test_run=is_test_run)

    process_func = partial(processing.process_chunk, chunks_dir=chunks_dir)

    if n_workers > 1:
        process_in_parallel(process_func, iter_indexed_chunks, n_workers=n_workers)
    else:
        for indexed_chunk in iter_indexed_chunks:
            process_func(indexed_chunk)

    chunking.concatenate_chunks(chunks_dir, output_file)


def process_in_parallel(func, indexed_chunks, *, n_workers: int):
    """Process the chunks in parallel using `n_workers` spawned processes."""
    # Explicitly use the "spawn" method to create workers for consistent behavior across OSes
    # and Python versions.
    ctx = mp.get_context("spawn")

    # Setting the `initializer` here is required since we used the "spawn" method above to
    # start workers: they start with no configuration, so we must provide it.
    #
    # NOTE(Vincent 2026-06-17):
    # It looks like `multiprocessing.Pool` has a non-trivial silent failure mode: if a
    # worker process get OOM-killed (i.e. because available memory is low) then the killed
    # worker will fail silently and the Pool will hang forever.
    # IMHO we should leave as it is for now and make sure to monitor memory usage. The future
    # rewrite to Polars will remove the use of `multiprocessing` and this problem.
    # See: https://bugs.python.org/issue22393
    with ctx.Pool(n_workers, initializer=processing.configure_pandas) as pool:
        for _result in pool.imap_unordered(func, indexed_chunks):
            # Consume the iterator, discard the result since it's written to disk.
            pass
