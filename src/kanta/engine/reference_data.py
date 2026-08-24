"""Static reference/mapping tables loaded from disk, used by the engine's filters.

Each loader is @lru_cache'd per process, plus a per-run pickle cache (set_cache_dir())
so worker processes can deserialize tables warm_all() already built, instead of
re-parsing them.
"""

import logging
import pickle
import shutil
import urllib.request
import warnings
from functools import lru_cache
from pathlib import Path
from urllib.error import URLError

import numpy as np
import pandas as pd

from kanta import config

logger = logging.getLogger(__name__)

# Set once via set_cache_dir() (by main(), before dispatching to workers). None means "no
# pickle cache available" — callers still work, just without the cross-process speedup.
_CACHE_DIR: Path | None = None


def set_cache_dir(path: Path) -> None:
    """Point the per-run pickle cache at `path` (created fresh once per run by the caller)."""
    global _CACHE_DIR
    _CACHE_DIR = path


def _cached(key: str, compute):
    """Pickle-cache compute()'s result under _CACHE_DIR, keyed by `key`.

    Computes without caching if _CACHE_DIR isn't set.
    """
    if _CACHE_DIR is None:
        return compute()

    cache_path = _CACHE_DIR / f"{key}.pkl"
    if cache_path.exists():
        return pickle.loads(cache_path.read_bytes())

    result = compute()
    cache_path.write_bytes(pickle.dumps(result))
    return result


class FallbackToKeyDict(dict):
    """Dict whose lookups (including via Series.map) fall back to the key itself when missing."""

    def __missing__(self, key):
        return key


def _sample_dict(d: dict, n: int = 3) -> dict:
    """First n (key -> value) pairs of d, for printing a content preview in verbose mode."""
    return dict(list(d.items())[:n])


def _refresh_from_remote(url: str, local_path: Path, timeout: float = 5.0, verbose: bool = False) -> None:
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
        if verbose:
            logger.info(f"[reference_data] refreshed {local_path.name} from {url}")
    except (URLError, OSError) as e:
        if verbose:
            warnings.warn(f"Could not refresh {local_path.name} from {url} ({e}); using local copy.")
    finally:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


@lru_cache(maxsize=1)
def get_thl_lab_map(verbose: bool = False) -> FallbackToKeyDict:
    """National (THL) lab id -> abbreviation mapping, lowercased with spaces stripped."""

    def compute():
        df = pd.read_csv(
            config.THL_LAB_ID_ABBREVIATION_FILE,
            sep=";",
            encoding="latin-1",
            usecols=["CodeId", "Abbreviation"],
            dtype=str,
        )
        abbreviation = df["Abbreviation"].str.replace(" ", "", regex=False).str.lower()
        return FallbackToKeyDict(zip(df["CodeId"], abbreviation))

    result = _cached("thl_lab_map", compute)
    if verbose:
        logger.info(f"[reference_data] thl_lab_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_thl_sote_map(verbose: bool = False) -> FallbackToKeyDict:
    """National (THL) organization id -> lab/organization name mapping."""

    def compute():
        df = pd.read_csv(
            config.THL_SOTE_MAP_FILE,
            sep="\t",
            usecols=["OrganizationId", "LAB_NAME"],
            dtype=str,
        )
        return FallbackToKeyDict(zip(df["OrganizationId"], df["LAB_NAME"]))

    result = _cached("thl_sote_map", compute)
    if verbose:
        logger.info(f"[reference_data] thl_sote_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_thl_manual_map(verbose: bool = False) -> dict[str, str]:
    """Manual mapping from a short numeric code (derived from CODING_SYSTEM) to a coding system name."""

    def compute():
        df = pd.read_csv(
            config.THL_CODING_MANUAL_MAP_FILE,
            sep="\t",
            usecols=["CODE", "NAME"],
            dtype=str,
        )
        return dict(zip(df["CODE"], df["NAME"]))

    result = _cached("thl_manual_map", compute)
    if verbose:
        logger.info(f"[reference_data] thl_manual_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_unit_map(verbose: bool = False) -> dict[str, str]:
    """Raw/dirty MEASUREMENT_UNIT string -> corrected unit, from a manually curated table."""

    def compute():
        df = pd.read_csv(
            config.UNIT_MAP_FILE,
            sep="\t",
            usecols=["OLD_UNIT", "MEASUREMENT_UNIT"],
            dtype=str,
        )
        return dict(zip(df["OLD_UNIT"], df["MEASUREMENT_UNIT"]))

    result = _cached("unit_map", compute)
    if verbose:
        logger.info(f"[reference_data] unit_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_usagi_units(verbose: bool = False) -> set[str]:
    """Usagi-approved lab MEASUREMENT_UNIT source codes, filtered to unique-for-lab units."""

    def compute():
        _refresh_from_remote(config.USAGI_UNITS_URL, config.USAGI_UNITS_FILE, verbose=verbose)
        df = pd.read_csv(
            config.USAGI_UNITS_FILE,
            sep="\t",
            usecols=["MEASUREMENT_UNIT", "UNIQUE_FOR_LAB"],
            dtype=str,
        ).drop_duplicates()
        return set(df.loc[df["UNIQUE_FOR_LAB"] == "TRUE", "MEASUREMENT_UNIT"]), len(df)

    result, n_total = _cached("usagi_units", compute)
    if verbose:
        sample = sorted(result)[:5]
        logger.info(f"[reference_data] usagi_units: {len(result)}/{n_total} unique-for-lab units loaded, sample {sample}")
    return result


@lru_cache(maxsize=1)
def get_injection_results(verbose: bool = False) -> pd.DataFrame:
    """Unit-injection targets from scripts/injection/ (both PASS and FAIL rows kept).

    Kept because a bimodal split's low/high pair can have one side FAIL and the other PASS —
    the FAIL side's CUTOFF is still needed. Callers must check OUTCOME before using UNIT.
    """

    def compute():
        return pd.read_csv(
            config.INJECTION_RESULTS_FILE,
            sep="\t",
            usecols=["TEST_NAME", "SUB_DIST", "CUTOFF", "UNIT", "OUTCOME", "BIMODAL_BC", "BIMODAL_OVERLAP"],
            dtype={"TEST_NAME": str, "SUB_DIST": str, "UNIT": str, "OUTCOME": str},
        )

    df = _cached("injection_results", compute)
    if verbose:
        counts = df["OUTCOME"].value_counts().to_dict()
        logger.info(f"[reference_data] injection_results: {len(df)} rows loaded, OUTCOME counts {counts}")
        logger.info(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_injection_table(verbose: bool = False) -> pd.DataFrame:
    """Unit-injection candidates indexed by TEST_NAME: CUTOFF, LOW_UNIT, HIGH_UNIT, BIMODAL_BC,
    BIMODAL_OVERLAP.

    Simple tests get CUTOFF=-inf and only HIGH_UNIT set. Split tests are included only when
    both their low and high side are PASS.
    """

    def compute():
        results = get_injection_results(verbose=verbose)

        simple = results[(results["SUB_DIST"] == "all") & (results["OUTCOME"] == "PASS")]
        simple_table = pd.DataFrame(
            {
                "CUTOFF": -np.inf,
                "LOW_UNIT": np.nan,
                "HIGH_UNIT": simple["UNIT"].values,
                "BIMODAL_BC": np.nan,
                "BIMODAL_OVERLAP": np.nan,
            },
            index=simple["TEST_NAME"],
        )

        split = results[results["SUB_DIST"] != "all"]
        low = split[split["SUB_DIST"] == "low"].set_index("TEST_NAME")
        high = split[split["SUB_DIST"] == "high"].set_index("TEST_NAME")
        usable = (low["OUTCOME"] == "PASS") & (high["OUTCOME"] == "PASS")
        split_table = pd.DataFrame(
            {
                "CUTOFF": low["CUTOFF"],
                "LOW_UNIT": low["UNIT"],
                "HIGH_UNIT": high["UNIT"],
                "BIMODAL_BC": low["BIMODAL_BC"],
                "BIMODAL_OVERLAP": low["BIMODAL_OVERLAP"],
            }
        )[usable]

        return pd.concat([simple_table, split_table]), len(simple_table), len(split_table)

    table, n_simple, n_split = _cached("injection_table", compute)
    if verbose:
        logger.info(f"[reference_data] injection_table: {n_simple} simple + {n_split} split = {len(table)} tests")
        logger.info(table.head(3).to_string())
    return table


@lru_cache(maxsize=1)
def get_omop_injection_table(verbose: bool = False) -> pd.DataFrame:
    """Test-specific unit corrections, indexed by (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT)
    for a direct row lookup of the corrected unit. Examples:
    - b-hkr "osuus" -> "ratio" (not a formal unit)
    - du-prot "g" -> "g/24h" (incomplete)
    - p-krea "mmol/l" -> "umol/l" (incorrect)
    - -l-ind "NA" -> "index" (missing)
    """

    def compute():
        df = pd.read_csv(
            config.OMOP_INJECTION_FILE,
            sep="\t",
            usecols=["TEST_NAME_ABBREVIATION", "source_unit_clean", "source_unit_clean_fix"],
            dtype=str,
            keep_default_na=False,
        )
        df[["source_unit_clean", "source_unit_clean_fix"]] = df[["source_unit_clean", "source_unit_clean_fix"]].replace(
            "", "NA"
        )
        return df.drop_duplicates(subset=["TEST_NAME_ABBREVIATION", "source_unit_clean"], keep="first").set_index(
            ["TEST_NAME_ABBREVIATION", "source_unit_clean"]
        )

    table = _cached("omop_injection", compute)
    if verbose:
        logger.info(f"[reference_data] omop_injection: {len(table)} (abbreviation, unit) corrections loaded")
        logger.info(table.head(3).to_string())
    return table


@lru_cache(maxsize=1)
def get_usagi_mapping(verbose: bool = False) -> pd.DataFrame:
    """Usagi lab-test mapping table: MAPPING_STATUS/OMOP_ID/OMOP_QUANTITY per
    (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT) pair.

    "NA"-fills all columns so join keys match the engine's "NA" (not NaN) missing-value convention.
    """

    def compute():
        _refresh_from_remote(config.USAGI_MAPPING_URL, config.USAGI_MAPPING_FILE, verbose=verbose)
        return pd.read_csv(
            config.USAGI_MAPPING_FILE,
            sep="\t",
            usecols=[
                "TEST_NAME_ABBREVIATION",
                "MEASUREMENT_UNIT",
                "harmonization_omop::OMOP_ID",
                "harmonization_omop::OMOP_QUANTITY",
                "harmonization_omop::MAPPING_STATUS",
            ],
            dtype=str,
        ).drop_duplicates().fillna("NA")

    df = _cached("usagi_mapping", compute)
    if verbose:
        logger.info(f"[reference_data] usagi_mapping: {len(df)} rows loaded")
        logger.info(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_harmonization_counts(verbose: bool = False) -> pd.DataFrame:
    """Reads in the target MEASUREMENT_UNIT per (OMOP_ID, OMOP_QUANTITY)."""

    def compute():
        _refresh_from_remote(config.HARMONIZATION_COUNTS_URL, config.HARMONIZATION_COUNTS_FILE, verbose=verbose)
        df = pd.read_csv(
            config.HARMONIZATION_COUNTS_FILE,
            sep="\t",
            usecols=[
                "harmonization_omop::OMOP_ID",
                "harmonization_omop::OMOP_QUANTITY",
                "harmonization_omop::MEASUREMENT_UNIT",
            ],
            dtype=str,
        )
        df["harmonization_omop::MEASUREMENT_UNIT"] = df["harmonization_omop::MEASUREMENT_UNIT"].fillna("NA")
        return df

    df = _cached("harmonization_counts", compute)
    if verbose:
        logger.info(f"[reference_data] harmonization_counts: {len(df)} rows loaded")
        logger.info(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_unit_conversion(verbose: bool = False) -> pd.DataFrame:
    """Reads in the conversion factor per (OMOP_QUANTITY, source unit, target unit)."""

    def compute():
        _refresh_from_remote(config.UNIT_CONVERSION_URL, config.UNIT_CONVERSION_FILE, verbose=verbose)
        df = pd.read_csv(
            config.UNIT_CONVERSION_FILE,
            sep="\t",
            usecols=[
                "harmonization_omop::OMOP_QUANTITY",
                "MEASUREMENT_UNIT",
                "TO_MEASUREMENT_UNIT",
                "CONVERSION",
                "ONLY_TO_OMOP_CONCEPTS",
            ],
            dtype=str,
        ).rename(
            columns={
                "TO_MEASUREMENT_UNIT": "harmonization_omop::MEASUREMENT_UNIT",
                "CONVERSION": "harmonization_omop::CONVERSION_FACTOR",
            }
        )
        df[["MEASUREMENT_UNIT", "harmonization_omop::MEASUREMENT_UNIT"]] = df[
            ["MEASUREMENT_UNIT", "harmonization_omop::MEASUREMENT_UNIT"]
        ].fillna("NA")
        return df

    df = _cached("unit_conversion", compute)
    if verbose:
        logger.info(f"[reference_data] unit_conversion: {len(df)} rows loaded")
        logger.info(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_conversion_table(verbose: bool = False) -> pd.DataFrame:
    """Build a per-row conversion lookup from unit_conversion + harmonization_counts.

    Input:  OMOP_QUANTITY, MEASUREMENT_UNIT (source), TARGET_UNIT, CONVERSION_FACTOR
    Output: index (OMOP_ID, OMOP_QUANTITY, MEASUREMENT_UNIT) -> TARGET_UNIT, CONVERSION_FACTOR

    E.g. (3020564, Substance Concentration, mmol/l) -> (umol/l, 1000)
    """

    def compute():
        unit_conversion = get_unit_conversion(verbose=verbose)
        harmonization_counts = get_harmonization_counts(verbose=verbose)

        merged = pd.merge(
            unit_conversion,
            harmonization_counts,
            on=["harmonization_omop::OMOP_QUANTITY", "harmonization_omop::MEASUREMENT_UNIT"],
            how="inner",
        )

        applies = merged["ONLY_TO_OMOP_CONCEPTS"].isna() | (
            merged["ONLY_TO_OMOP_CONCEPTS"] == merged["harmonization_omop::OMOP_ID"]
        )
        merged = merged[applies].copy()

        join_cols = ["harmonization_omop::OMOP_ID", "harmonization_omop::OMOP_QUANTITY", "MEASUREMENT_UNIT"]
        merged["_is_specific"] = merged["ONLY_TO_OMOP_CONCEPTS"].notna()
        return (
            merged.sort_values("_is_specific", ascending=False)
            .drop_duplicates(subset=join_cols, keep="first")
            .drop(columns=["_is_specific", "ONLY_TO_OMOP_CONCEPTS"])
            .set_index(join_cols)
        )

    table = _cached("conversion_table", compute)
    if verbose:
        logger.info(f"[reference_data] conversion_table: {len(table)} (OMOP_ID, quantity, unit) conversions ready")
        logger.info(table.head(3).to_string())
    return table


@lru_cache(maxsize=1)
def get_posneg_table(verbose: bool = False) -> dict[str, str]:
    """MEASUREMENT_FREE_TEXT -> extracted::IS_POS ("0"/"1") lookup.

    MEASUREMENT_FREE_TEXT  ->  extracted::IS_POS
    "NEGAT"                ->  "0"
    "Posit."               ->  "1"

    Built as a plain dict so a duplicate key can never fan out a row in df.
    """

    def compute():
        df = pd.read_csv(
            config.POSNEG_MAP_FILE,
            sep="\t",
            usecols=["MEASUREMENT_FREE_TEXT", "extracted::IS_POS"],
            dtype=str,
        ).dropna(subset=["extracted::IS_POS"])
        return dict(zip(df["MEASUREMENT_FREE_TEXT"].drop_duplicates(keep="first"), df["extracted::IS_POS"]))

    result = _cached("posneg_table", compute)
    if verbose:
        logger.info(f"[reference_data] posneg_table: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_ab_limits(verbose: bool = False) -> pd.DataFrame:
    """Per-OMOP_ID abnormality reference range: LOW_LIMIT/HIGH_LIMIT plus LOW_PROBLEM/HIGH_PROBLEM
    flags, indexed by OMOP_ID. See scripts/qc_scripts/abnormality.py for how it's built.

    OMOP_ID   LOW_LIMIT  HIGH_LIMIT  LOW_PROBLEM  HIGH_PROBLEM
    1002102   -inf       inf         0            0
    1175426   57.0       98.0        0            0
    """

    def compute():
        df = pd.read_csv(
            config.AB_LIMITS_FILE,
            sep="\t",
            dtype=str,
        ).rename(columns={"ID": "harmonization_omop::OMOP_ID"})
        return df.drop_duplicates(subset="harmonization_omop::OMOP_ID", keep="first").set_index(
            "harmonization_omop::OMOP_ID"
        )

    df = _cached("ab_limits", compute)
    if verbose:
        logger.info(f"[reference_data] ab_limits: {len(df)} OMOP_ID reference ranges loaded")
        logger.info(df.head(3).to_string())
    return df


@lru_cache(maxsize=1)
def get_omop_qc(verbose: bool = False) -> pd.DataFrame:
    """Per-OMOP_ID QC threshold rules: SIDE/THRESHOLD/QC_NOTES.

    OMOP_ID  THRESHOLD  SIDE  QC_NOTES
    3026361  20         >     IMPLAUSIBLE_VALUE
    3026361  0.5        <     IMPOSSIBLE_VALUE

    Not deduplicated: an OMOP_ID can carry several rules. Some rows are placeholder
    "register as checked" entries with SIDE/THRESHOLD blank.
    """

    def compute():
        return pd.read_csv(
            config.OMOP_QC_FILE,
            sep="\t",
            usecols=["harmonization_omop::OMOP_ID", "THRESHOLD", "SIDE", "QC_NOTES"],
            dtype=str,
        )

    df = _cached("omop_qc", compute)
    if verbose:
        logger.info(f"[reference_data] omop_qc: {len(df)} rules loaded, {df['SIDE'].isna().sum()} placeholder-only")
        logger.info(df.head(3).to_string())
    return df


def warm_all(verbose: bool = True) -> None:
    """Load every reference table once, up front, so load/fallback diagnostics print once at
    startup and the per-run pickle cache is populated before any worker process starts.
    """
    get_thl_lab_map(verbose=verbose)
    get_thl_sote_map(verbose=verbose)
    get_thl_manual_map(verbose=verbose)
    get_unit_map(verbose=verbose)
    get_usagi_units(verbose=verbose)
    get_injection_table(verbose=verbose)
    get_omop_injection_table(verbose=verbose)
    get_usagi_mapping(verbose=verbose)
    get_conversion_table(verbose=verbose)
    get_posneg_table(verbose=verbose)
    get_ab_limits(verbose=verbose)
    get_omop_qc(verbose=verbose)
