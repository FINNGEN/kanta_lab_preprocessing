"""Static reference/mapping tables loaded from disk, used by the engine's filters.

Each loader is cached so the underlying file is only read once per process: once in the
main process for serial runs, once per worker process when running in parallel (since
`spawn` workers re-import modules fresh, the cache is naturally per-process).
"""

import shutil
import urllib.request
import warnings
from functools import lru_cache
from pathlib import Path
from urllib.error import URLError

import pandas as pd

from kanta import config


class FallbackToKeyDict(dict):
    """Dict whose lookups (including via Series.map) fall back to the key itself when missing."""

    def __missing__(self, key):
        return key


def _refresh_from_remote(url: str, local_path: Path, timeout: float = 5.0) -> None:
    """Best-effort refresh of local_path's contents from url.

    Downloads to a temp file first so a failed/partial download never corrupts the
    existing local snapshot. On any failure (no network, timeout, 404, ...), silently
    falls back to whatever is already on disk.
    """
    tmp_path = local_path.with_suffix(local_path.suffix + ".tmp")
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response, open(tmp_path, "wb") as f:
            shutil.copyfileobj(response, f)
        tmp_path.replace(local_path)
    except (URLError, OSError) as e:
        warnings.warn(f"Could not refresh {local_path.name} from {url} ({e}); using local copy.")
    finally:
        tmp_path.unlink(missing_ok=True)


@lru_cache(maxsize=1)
def get_thl_lab_map() -> FallbackToKeyDict:
    """National (THL) lab id -> abbreviation mapping, lowercased with spaces stripped."""
    df = pd.read_csv(
        config.THL_LAB_ID_ABBREVIATION_FILE,
        sep=";",
        encoding="latin-1",
        usecols=["CodeId", "Abbreviation"],
        dtype=str,
    )
    abbreviation = df["Abbreviation"].str.replace(" ", "", regex=False).str.lower()
    return FallbackToKeyDict(zip(df["CodeId"], abbreviation))


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


@lru_cache(maxsize=1)
def get_unit_map() -> dict[str, str]:
    """Raw/dirty MEASUREMENT_UNIT string -> corrected unit, from a manually curated table."""
    df = pd.read_csv(
        config.UNIT_MAP_FILE,
        sep="\t",
        usecols=["OLD_UNIT", "MEASUREMENT_UNIT"],
        dtype=str,
    )
    return dict(zip(df["OLD_UNIT"], df["MEASUREMENT_UNIT"]))


@lru_cache(maxsize=1)
def get_usagi_units() -> set[str]:
    """Usagi-approved lab MEASUREMENT_UNIT source codes, filtered to unique-for-lab units."""
    _refresh_from_remote(config.USAGI_UNITS_URL, config.USAGI_UNITS_FILE)
    df = pd.read_csv(
        config.USAGI_UNITS_FILE, usecols=["sourceCode", "ADD_INFO:UniqueForLab"]
    ).drop_duplicates()
    assert df["ADD_INFO:UniqueForLab"].dtype == bool
    return set(df.loc[df["ADD_INFO:UniqueForLab"], "sourceCode"])


@lru_cache(maxsize=1)
def get_injection_results() -> pd.DataFrame:
    """Unit-injection targets from scripts/injection/ (both PASS and FAIL rows kept).

    FAIL rows are kept here (not dropped) because a bimodal split's low/high pair can have
    one side FAIL and the other PASS (e.g. neutrofiilit) — the FAIL side's CUTOFF is still
    needed to know where the split boundary is, even though its own UNIT must not be used.
    Callers are responsible for checking OUTCOME before using a row's UNIT.
    """
    return pd.read_csv(
        config.INJECTION_RESULTS_FILE,
        sep="\t",
        usecols=["TEST_NAME", "SUB_DIST", "CUTOFF", "UNIT", "OUTCOME", "BIMODAL_BC", "BIMODAL_OVERLAP"],
        dtype={"TEST_NAME": str, "SUB_DIST": str, "UNIT": str, "OUTCOME": str},
    )


@lru_cache(maxsize=1)
def get_usagi_mapping() -> pd.DataFrame:
    """Usagi lab-test mapping table (mappingStatus/OMOP_ID per lab test)."""
    _refresh_from_remote(config.USAGI_MAPPING_URL, config.USAGI_MAPPING_FILE)
    df = pd.read_csv(
        config.USAGI_MAPPING_FILE, usecols=["mappingStatus", "conceptId"], dtype=str
    ).drop_duplicates()
    return df.rename(
        columns={
            "mappingStatus": "harmonization_omop::mappingStatus",
            "conceptId": "harmonization_omop::OMOP_ID",
        }
    )
