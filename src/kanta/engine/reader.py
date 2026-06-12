from pathlib import Path

import pandas as pd

from kanta.config import engine_read_columns


def chunk_iterator(input_file: Path):
    return (
        pd.read_parquet(
            input_file,
            engine="pyarrow",
            columns=engine_read_columns,
            dtype_backend="pyarrow"
        )
    )
