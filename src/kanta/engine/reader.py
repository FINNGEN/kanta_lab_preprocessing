from collections.abc import Iterator
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from kanta.config import ENGINE_CHUNK_N_LINES, ENGINE_INPUT_COLUMNS_MAPPING


def chunk_iterator(
    input_file: Path, *, is_test_run: bool = False
) -> Iterator[pd.DataFrame]:
    # Use pyarrow to read the Parquet file in chunks.
    parquet_file = pq.ParquetFile(input_file)

    for batch in parquet_file.iter_batches(
        batch_size=ENGINE_CHUNK_N_LINES,
        # Select only the given columns, this speeds up the read quite a lot for Parquet files.
        columns=ENGINE_INPUT_COLUMNS_MAPPING.keys(),
    ):
        yield batch.to_pandas(
            # Use nullable data-types backed by pyarrow.
            types_mapper=pd.ArrowDtype
        )

        # Yield only the first item when test run is ON
        if is_test_run:
            break
