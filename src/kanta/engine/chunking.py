import shutil
from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq


# Which columns to read from the input file. This is used very early in the pipeline to limit the
# amount of data read, so the column renaming has not been done yet, hence using the original
# column names.
# TODO(Vincent 2026-06-17)  This should use the canonical column aliases from the config. A good
# time to do that would be when the data processing is in place so that all the column reference
# the canonical column aliases.
READ_COLUMNS = [
    "FINNGENID",
    "EVENT_AGE",
    "tutkimuskoodistonjarjestelma",
    "paikallinentutkimusnimike_selite",
    "tutkimustulosarvo",
    "tutkimustulosyksikko",
    "tutkimusvastauksentila",
    "tuloksenpoikkeavuus",
    "viitearvoryhma",
    "viitevalialkuarvo",
    "viitevalialkuyksikko",
    "viitevaliloppuarvo",
    "viitevaliloppuyksikko",
    "tutkimustulosteksti",
    "paikallinentutkimusnimike_koodi",
    "laboratoriotutkimusnimike",
    "APPROX_EVENT_DAY",
    "TIME",
    "_rowid",
    "_rowid_source",
    "SEX",
]

# Number of rows per chunk when streaming the input Parquet file.
# The value is independent of the number of CPUs: the memory used by the engine
# is already proportional to the number of workers, so scaling the number of
# rows per chunk by the number of workers would make the memory use scale by
# (N workers × N workers).
N_LINES_PER_CHUNK = 200_000
CHUNKS_FILE_TEMPLATE = "chunk_{index:06d}.parquet"
CHUNKS_FILE_GLOB = "chunk_*.parquet"


def chunk_iterator(
    input_file: Path, *, is_test_run: bool = False
) -> Iterator[tuple[int, pd.DataFrame]]:
    # Use pyarrow to read the Parquet file in chunks.
    parquet_file = pq.ParquetFile(input_file)

    chunk_index = 0
    for batch in parquet_file.iter_batches(
        batch_size=N_LINES_PER_CHUNK,
        # Select only the given columns, this speeds up the read quite a lot for Parquet files.
        columns=READ_COLUMNS,
    ):
        yield (
            chunk_index,
            batch.to_pandas(
                # Use nullable data-types backed by pyarrow.
                types_mapper=pd.ArrowDtype
            ),
        )

        chunk_index += 1

        # Yield only the first item when test run is ON
        if is_test_run:
            break


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
