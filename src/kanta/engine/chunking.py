import shutil
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from kanta import config


def chunk_iterator(
    input_file: Path, *, is_test_run: bool = False
) -> Iterator[pd.DataFrame]:
    # Use pyarrow to read the Parquet file in chunks.
    parquet_file = pq.ParquetFile(input_file)

    for batch in parquet_file.iter_batches(
        batch_size=config.ENGINE_N_LINES_PER_CHUNK,
        # Select only the given columns, this speeds up the read quite a lot for Parquet files.
        columns=config.ENGINE_READ_COLUMNS,
    ):
        yield batch.to_pandas(
            # Use nullable data-types backed by pyarrow.
            types_mapper=pd.ArrowDtype
        )

        # Yield only the first item when test run is ON
        if is_test_run:
            break


def write_chunk(dataframe: pd.DataFrame, chunks_dir: Path, chunk_index: int) -> Path:
    """Write one processed chunk to its own Parquet chunk file.

    Use `chunk_index` to keep track of order for later in-order concatenation.
    """
    chunk_path = chunks_dir / config.ENGINE_CHUNKS_FILE_TEMPLATE.format(
        index=chunk_index
    )
    dataframe.to_parquet(chunk_path, engine="pyarrow", compression="zstd", index=False)
    return chunk_path


def concatenate_chunks(chunks_dir: Path, output_file: Path, cleanup: bool = True):
    """Concatenate chunks in order so no sorting is required afterwards.

    The order relies on the filename, which holds the chunk index.
    """
    chunks = sorted(
        chunks_dir.glob(config.ENGINE_CHUNKS_FILE_GLOB), key=get_chunk_index
    )

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
