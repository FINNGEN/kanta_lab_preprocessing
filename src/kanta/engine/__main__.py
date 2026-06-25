import multiprocessing as mp
import os
from argparse import ArgumentParser
from functools import partial
from pathlib import Path

from kanta import output
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
        "--output-file",
        type=Path,
        help="Output file path (Parquet)",
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
        "--keep-intermediate-files",
        help="Keep intermediate files, useful for debugging.",
        action="store_true",
    )

    args = parser.parse_args()

    if args.n_workers < 1:
        raise ValueError("--n-workers must be 1 or more")

    if args.test:
        args.n_workers = 1

    return args


if __name__ == "__main__":
    args = init_cli()

    output.check_safe_write(args.output_file)
    tmp_dir = output.create_tmp_dir()

    main(
        args.input_file,
        args.output_file,
        tmp_dir,
        is_test_run=args.test,
        n_workers=args.n_workers,
    )

    if not args.keep_intermediate_files:
        output.teardown_dir(tmp_dir)
