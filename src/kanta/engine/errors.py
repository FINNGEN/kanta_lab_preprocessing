from dataclasses import dataclass, field

import pandas as pd
import pyarrow as pa

from kanta import config

# The subset of ERR_COLUMNS that identify the source row, i.e. everything
# except the ERR/ERR_VALUE pair that each filter fills in.
ID_COLUMNS = [col for col in config.ERR_COLUMNS if col not in ("ERR", "ERR_VALUE")]

# Schema used to write an empty errors file when no chunk flagged any rows, so the errors
# output file can always be relied on to exist.
EMPTY_SCHEMA = pa.schema(
    [(col, pa.int64()) for col in ID_COLUMNS] + [("ERR", pa.string()), ("ERR_VALUE", pa.string())]
)


@dataclass
class ErrorSink:
    """Accumulates rows dropped/flagged by filters, normalized to config.ERR_COLUMNS."""

    frames: list[pd.DataFrame] = field(default_factory=list)

    def add(self, df: pd.DataFrame, *, err_name: str, err_value) -> None:
        if df.empty:
            return

        frame = df[ID_COLUMNS].copy()
        frame["ERR"] = err_name
        frame["ERR_VALUE"] = err_value
        self.frames.append(frame[config.ERR_COLUMNS])


# Schema used to write an empty abbr file when no chunk changed any abbreviation.
ABBR_EMPTY_SCHEMA = pa.schema(
    [(col, pa.int64()) for col in ID_COLUMNS]
    + [("ERR", pa.string()), ("OLD_ABBR", pa.string()), ("NEW_ABBR", pa.string())]
)


@dataclass
class AbbrSink:
    """Accumulates TEST_NAME_ABBREVIATION changes, normalized to config.ABBR_COLUMNS."""

    frames: list[pd.DataFrame] = field(default_factory=list)

    def add(self, df: pd.DataFrame, *, err_name: str, old_abbr, new_abbr) -> None:
        if df.empty:
            return

        frame = df[ID_COLUMNS].copy()
        frame["ERR"] = err_name
        frame["OLD_ABBR"] = old_abbr
        frame["NEW_ABBR"] = new_abbr
        self.frames.append(frame[config.ABBR_COLUMNS])


# Schema used to write an empty unit-change file when no chunk changed any unit.
UNIT_EMPTY_SCHEMA = pa.schema(
    [(col, pa.int64()) for col in ID_COLUMNS]
    + [("ERR", pa.string()), ("OLD_UNIT", pa.string()), ("NEW_UNIT", pa.string())]
)


@dataclass
class UnitSink:
    """Accumulates MEASUREMENT_UNIT changes, normalized to config.UNIT_COLUMNS."""

    frames: list[pd.DataFrame] = field(default_factory=list)

    def add(self, df: pd.DataFrame, *, err_name: str, old_unit, new_unit) -> None:
        if df.empty:
            return

        frame = df[ID_COLUMNS].copy()
        frame["ERR"] = err_name
        frame["OLD_UNIT"] = old_unit
        frame["NEW_UNIT"] = new_unit
        self.frames.append(frame[config.UNIT_COLUMNS])
