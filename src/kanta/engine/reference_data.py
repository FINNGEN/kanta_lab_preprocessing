"""Static reference/mapping tables loaded from disk, used by the engine's filters.

Each loader is cached so the underlying file is only read once per process: once in the
main process for serial runs, once per worker process when running in parallel (since
`spawn` workers re-import modules fresh, the cache is naturally per-process).
"""

from functools import lru_cache

import pandas as pd

from kanta import config


class FallbackToKeyDict(dict):
    """Dict whose lookups (including via Series.map) fall back to the key itself when missing."""

    def __missing__(self, key):
        return key


@lru_cache(maxsize=1)
def get_thl_lab_map() -> FallbackToKeyDict:
    """National (THL) lab id -> abbreviation mapping."""
    df = pd.read_csv(config.THL_LAB_ID_ABBREVIATION_FILE, sep="\t", dtype=str)
    return FallbackToKeyDict(zip(df["CodeId"], df["Abbreviation"]))


@lru_cache(maxsize=1)
def get_thl_sote_map() -> FallbackToKeyDict:
    """National (THL) organization id -> lab/organization name mapping."""
    df = pd.read_csv(
        config.THL_SOTE_MAP_FILE,
        sep="\t",
        usecols=["OrganizationId", "LAB_NAME"],
        dtype=str,
    )
    return FallbackToKeyDict(zip(df["OrganizationId"], df["LAB_NAME"]))


@lru_cache(maxsize=1)
def get_thl_manual_map() -> dict[str, str]:
    """Manual mapping from a short numeric code (derived from CODING_SYSTEM) to a coding system name."""
    df = pd.read_csv(
        config.THL_CODING_MANUAL_MAP_FILE,
        sep="\t",
        usecols=["CODE", "NAME"],
        dtype=str,
    )
    return dict(zip(df["CODE"], df["NAME"]))
