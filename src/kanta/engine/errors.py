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
        # Force plain object dtype rather than whatever Arrow string width err_value happened
        # to inherit from its source column: different filters build err_value from columns of
        # different Arrow string widths (string vs large_string), so leaving it as-is means
        # different chunks' error files disagree on schema and pq.ParquetWriter chokes when
        # concatenating them (only shows up across chunks, never in a single-chunk --test run).
        frame = frame.astype({"ERR": object, "ERR_VALUE": object})
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
        # See UnitSink.add's comment: normalize away Arrow string-width mismatches across chunks.
        frame = frame.astype({"ERR": object, "OLD_ABBR": object, "NEW_ABBR": object})
        self.frames.append(frame[config.ABBR_COLUMNS])


# Schema used to write an empty unit-change file when no chunk changed any unit. Unlike
# ID_COLUMNS (shared with ErrorSink/AbbrSink), UnitSink also carries TEST_NAME_ABBREVIATION,
# so it needs its own int64/string split rather than reusing the generic ID_COLUMNS list.
UNIT_EMPTY_SCHEMA = pa.schema(
    [(col, pa.int64()) for col in ID_COLUMNS]
    + [("TEST_NAME_ABBREVIATION", pa.string()), ("ERR", pa.string()),
       ("OLD_UNIT", pa.string()), ("NEW_UNIT", pa.string())]
)


@dataclass
class UnitSink:
    """Accumulates MEASUREMENT_UNIT changes, normalized to config.UNIT_COLUMNS."""

    frames: list[pd.DataFrame] = field(default_factory=list)

    def add(self, df: pd.DataFrame, *, err_name: str, old_unit, new_unit) -> None:
        if df.empty:
            return

        frame = df[ID_COLUMNS + ["TEST_NAME_ABBREVIATION"]].copy()
        frame["ERR"] = err_name
        frame["OLD_UNIT"] = old_unit
        frame["NEW_UNIT"] = new_unit
        # fix_unit.py's own changes build OLD_UNIT/NEW_UNIT from MEASUREMENT_UNIT (Arrow
        # `string`), while harmonization.py's EXTRACTION changes build them from
        # MEASUREMENT_FREE_TEXT (Arrow `large_string`) — so depending on which chunk got which
        # kind of change, the concatenated per-chunk unit file ends up with a different Arrow
        # string width for the same logical column, and pq.ParquetWriter raises a schema
        # mismatch when concatenating chunks written with a different width (only visible in a
        # full/multi-chunk run, never a single-chunk --test run). Forcing plain object dtype
        # here makes every chunk agree on schema regardless of the source column's width.
        frame = frame.astype({"ERR": object, "OLD_UNIT": object, "NEW_UNIT": object})
        self.frames.append(frame[config.UNIT_COLUMNS])
