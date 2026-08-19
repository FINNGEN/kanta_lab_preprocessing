import warnings
from pathlib import Path

import pandas as pd

from kanta.engine import chunking, pipes, reference_data, release
from kanta.engine.errors import AbbrSink, ErrorSink, UnitSink


def init_worker(cache_dir: Path) -> None:
    """Pool initializer for spawned worker processes: applies the pandas config (spawn doesn't
    inherit it) and points this worker at the per-run reference-data pickle cache that main()
    already populated via reference_data.warm_all(), before any worker started.
    """
    configure_pandas()
    reference_data.set_cache_dir(cache_dir)


def configure_pandas():
    """Set preferred Pandas behavior via options.

    IMPORTANT: This function must be called when initializing workers for multiprocessing, since
    creating them with the 'spawn' method doesn't carry over the Pandas configuration.
    """
    # Treat NaN (a real float value) and NA (a missing value) as distinct.
    pd.options.future.distinguish_nan_and_na = True

    # Default to the pyarrow engine for all Parquet reads/writes, so we don't have to pass
    # engine="pyarrow" on every call.
    pd.options.io.parquet.engine = "pyarrow"

    # Chained assignment (e.g. `df[df["A"] > 0]["B"] = 1`) silently no-ops under
    # Copy-on-Write — promote pandas' warning to a hard error instead. Use `.loc`/`.iloc`
    # to assign correctly.
    warnings.filterwarnings("error", category=pd.errors.ChainedAssignmentError)


def process_chunk(
    indexed_chunk: tuple[int, pd.DataFrame],
    chunks_dir: Path,
    errors_dir: Path,
    abbr_dir: Path,
    unit_dir: Path,
    release_dir: Path | None = None,
    verbose: bool = False,
) -> Path:
    chunk_index, df_chunk = indexed_chunk

    errors = ErrorSink()
    abbr_changes = AbbrSink()
    unit_changes = UnitSink()
    df_chunk = pipes.run_all(df_chunk, errors, abbr_changes, unit_changes, verbose=verbose)

    if errors.frames:
        errors_df = pd.concat(errors.frames, ignore_index=True)
        chunking.write_chunk(errors_df, errors_dir, chunk_index)

    if abbr_changes.frames:
        abbr_df = pd.concat(abbr_changes.frames, ignore_index=True)
        chunking.write_chunk(abbr_df, abbr_dir, chunk_index)

    if unit_changes.frames:
        unit_df = pd.concat(unit_changes.frames, ignore_index=True)
        chunking.write_chunk(unit_df, unit_dir, chunk_index)

    if release_dir is not None:
        release_df = release.build_release(df_chunk)
        chunking.write_chunk(release_df, release_dir, chunk_index)

    return chunking.write_chunk(df_chunk, chunks_dir, chunk_index)
