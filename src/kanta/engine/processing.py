import warnings
from pathlib import Path

import pandas as pd

from kanta.engine import chunking, pipes


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

    # Turn chained assignment into a hard error.
    # Chained assignments have weird behavior in that they would turn operations into no-ops.
    # For example this chained assignment *does not change any value* of df:
    #   `df[df["A"] > 0]["B"] = 1`
    # Instead, use `.loc` or `.iloc` to get correct behavior:
    #   `df.loc[df["A"] > 0, "B"] = 1`
    # We change Pandas default from just throwing a warning (ChainedAssignmentError), to actually
    # raising an error.
    # Note that unfortunately we can't just set the option
    # `mode.chained_assignment` to `"raise"` because it has no effect under
    # Copy-on-Write, so we have to resort to promoting the warning to an error.
    warnings.filterwarnings("error", category=pd.errors.ChainedAssignmentError)


def process_chunk(indexed_chunk: tuple[int, pd.DataFrame], chunks_dir: Path) -> Path:
    chunk_index, df_chunk = indexed_chunk

    df_chunk = pipes.run_all(df_chunk)

    return chunking.write_chunk(df_chunk, chunks_dir, chunk_index)
