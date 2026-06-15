import shutil
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

CHUNKS_FILE_TEMPLATE = "chunk_{index:06d}.parquet"
CHUNKS_FILE_GLOB = "chunk_*.parquet"


def write_chunk(dataframe: pd.DataFrame, chunks_dir: Path, chunk_index: int) -> Path:
    """Write one processed chunk to its own Parquet chunk file.

    Use `chunk_index` to keep track of order for later in-order concatenation.
    """
    chunk_path = chunks_dir / CHUNKS_FILE_TEMPLATE.format(index=chunk_index)
    dataframe.to_parquet(chunk_path, engine="pyarrow", compression="zstd", index=False)
    return chunk_path


def concatenate_chunks(chunks_dir: Path, output_file: Path, cleanup: bool = True):
    """Concatenate chunks in order so no sorting is required afterwards.

    The order relies on the filename, which holds the chunk index.
    """
    chunks = sorted(chunks_dir.glob(CHUNKS_FILE_GLOB), key=get_chunk_index)

    writer = None
    try:
        for chunk_path in chunks:
            table = pq.read_table(chunk_path)

            # Initialize the ParquetWriter, needs the schema.
            if writer is None:
                writer = pq.ParquetWriter(output_file, table.schema)
            writer.write_table(table)

    finally:
        if writer is not None:
            writer.close()

    if cleanup:
        shutil.rmtree(chunks_dir)


def get_chunk_index(chunk_path: Path) -> int:
    """Extract the integer chunk index from a chunk file name (e.g. chunk_000007 -> 7)."""
    return int(chunk_path.stem.split("_")[-1])
