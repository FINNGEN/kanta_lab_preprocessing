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

import numpy as np
import pandas as pd

from kanta import config


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
            print(f"[reference_data] refreshed {local_path.name} from {url}")
    except (URLError, OSError) as e:
        warnings.warn(f"Could not refresh {local_path.name} from {url} ({e}); using local copy.")
    finally:
        tmp_path.unlink(missing_ok=True)


@lru_cache(maxsize=1)
def get_thl_lab_map(verbose: bool = False) -> FallbackToKeyDict:
    """National (THL) lab id -> abbreviation mapping, lowercased with spaces stripped."""
    df = pd.read_csv(
        config.THL_LAB_ID_ABBREVIATION_FILE,
        sep=";",
        encoding="latin-1",
        usecols=["CodeId", "Abbreviation"],
        dtype=str,
    )
    abbreviation = df["Abbreviation"].str.replace(" ", "", regex=False).str.lower()
    result = FallbackToKeyDict(zip(df["CodeId"], abbreviation))
    if verbose:
        print(f"[reference_data] thl_lab_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_thl_sote_map(verbose: bool = False) -> FallbackToKeyDict:
    """National (THL) organization id -> lab/organization name mapping."""
    df = pd.read_csv(
        config.THL_SOTE_MAP_FILE,
        sep="\t",
        usecols=["OrganizationId", "LAB_NAME"],
        dtype=str,
    )
    result = FallbackToKeyDict(zip(df["OrganizationId"], df["LAB_NAME"]))
    if verbose:
        print(f"[reference_data] thl_sote_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_thl_manual_map(verbose: bool = False) -> dict[str, str]:
    """Manual mapping from a short numeric code (derived from CODING_SYSTEM) to a coding system name."""
    df = pd.read_csv(
        config.THL_CODING_MANUAL_MAP_FILE,
        sep="\t",
        usecols=["CODE", "NAME"],
        dtype=str,
    )
    result = dict(zip(df["CODE"], df["NAME"]))
    if verbose:
        print(f"[reference_data] thl_manual_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_unit_map(verbose: bool = False) -> dict[str, str]:
    """Raw/dirty MEASUREMENT_UNIT string -> corrected unit, from a manually curated table."""
    df = pd.read_csv(
        config.UNIT_MAP_FILE,
        sep="\t",
        usecols=["OLD_UNIT", "MEASUREMENT_UNIT"],
        dtype=str,
    )
    result = dict(zip(df["OLD_UNIT"], df["MEASUREMENT_UNIT"]))
    if verbose:
        print(f"[reference_data] unit_map: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_usagi_units(verbose: bool = False) -> set[str]:
    """Usagi-approved lab MEASUREMENT_UNIT source codes, filtered to unique-for-lab units."""
    _refresh_from_remote(config.USAGI_UNITS_URL, config.USAGI_UNITS_FILE, verbose=verbose)
    df = pd.read_csv(
        config.USAGI_UNITS_FILE, usecols=["sourceCode", "ADD_INFO:UniqueForLab"]
    ).drop_duplicates()
    assert df["ADD_INFO:UniqueForLab"].dtype == bool
    result = set(df.loc[df["ADD_INFO:UniqueForLab"], "sourceCode"])
    if verbose:
        sample = sorted(result)[:5]
        print(f"[reference_data] usagi_units: {len(result)}/{len(df)} unique-for-lab units loaded, sample {sample}")
    return result


@lru_cache(maxsize=1)
def get_injection_results(verbose: bool = False) -> pd.DataFrame:
    """Unit-injection targets from scripts/injection/ (both PASS and FAIL rows kept).

    FAIL rows are kept here (not dropped) because a bimodal split's low/high pair can have
    one side FAIL and the other PASS (e.g. neutrofiilit) — the FAIL side's CUTOFF is still
    needed to know where the split boundary is, even though its own UNIT must not be used.
    Callers are responsible for checking OUTCOME before using a row's UNIT.
    """
    df = pd.read_csv(
        config.INJECTION_RESULTS_FILE,
        sep="\t",
        usecols=["TEST_NAME", "SUB_DIST", "CUTOFF", "UNIT", "OUTCOME", "BIMODAL_BC", "BIMODAL_OVERLAP"],
        dtype={"TEST_NAME": str, "SUB_DIST": str, "UNIT": str, "OUTCOME": str},
    )
    if verbose:
        counts = df["OUTCOME"].value_counts().to_dict()
        print(f"[reference_data] injection_results: {len(df)} rows loaded, OUTCOME counts {counts}")
        print(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_injection_table(verbose: bool = False) -> pd.DataFrame:
    """Unit-injection candidates indexed by TEST_NAME: CUTOFF, LOW_UNIT, HIGH_UNIT, BIMODAL_BC,
    BIMODAL_OVERLAP.

    Simple (non-split) tests get CUTOFF=-inf and only HIGH_UNIT set, so comparing
    MEASUREMENT_VALUE against CUTOFF in the filter always resolves to HIGH_UNIT for them —
    the same low/high logic handles both simple and bimodal-split tests, no separate branch
    needed. Split tests are only included when both their low and high side are PASS: one
    FAILed side means the true boundary can't be trusted, so the whole test is excluded.
    """
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

    table = pd.concat([simple_table, split_table])
    if verbose:
        print(
            f"[reference_data] injection_table: {len(simple_table)} simple + "
            f"{len(split_table)} split = {len(table)} tests"
        )
        print(table.head(3).to_string())
    return table


@lru_cache(maxsize=1)
def get_usagi_mapping(verbose: bool = False) -> pd.DataFrame:
    """Usagi lab-test mapping table: mappingStatus/OMOP_ID/omopQuantity per
    (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT) pair.

    "NA"-fills all columns since ADD_INFO:measurementUnit (etc.) can be genuinely blank in the
    source CSV, and the engine's own MEASUREMENT_UNIT/TEST_NAME_ABBREVIATION columns use the
    string "NA" (not NaN) for missing, so join keys must match on that same convention.
    """
    _refresh_from_remote(config.USAGI_MAPPING_URL, config.USAGI_MAPPING_FILE, verbose=verbose)
    df = pd.read_csv(
        config.USAGI_MAPPING_FILE,
        usecols=[
            "mappingStatus",
            "conceptId",
            "ADD_INFO:omopQuantity",
            "ADD_INFO:testNameAbbreviation",
            "ADD_INFO:measurementUnit",
        ],
        dtype=str,
    ).drop_duplicates()
    df = df.rename(
        columns={
            "mappingStatus": "harmonization_omop::mappingStatus",
            "conceptId": "harmonization_omop::OMOP_ID",
            "ADD_INFO:omopQuantity": "harmonization_omop::omopQuantity",
            "ADD_INFO:testNameAbbreviation": "TEST_NAME_ABBREVIATION",
            "ADD_INFO:measurementUnit": "MEASUREMENT_UNIT",
        }
    ).fillna("NA")
    if verbose:
        print(f"[reference_data] usagi_mapping: {len(df)} rows loaded")
        print(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_harmonization_counts(verbose: bool = False) -> pd.DataFrame:
    """Target MEASUREMENT_UNIT per (OMOP_ID, omopQuantity) — the chosen harmonization destination.

    "NA"-fills harmonization_omop::MEASUREMENT_UNIT: a blank target unit is a legitimate choice
    for some OMOP concepts (e.g. Presence/Threshold quantities), not a missing value to drop.
    """
    _refresh_from_remote(config.HARMONIZATION_COUNTS_URL, config.HARMONIZATION_COUNTS_FILE, verbose=verbose)
    df = pd.read_csv(
        config.HARMONIZATION_COUNTS_FILE,
        sep="\t",
        usecols=[
            "harmonization_omop::OMOP_ID",
            "harmonization_omop::omopQuantity",
            "harmonization_omop::MEASUREMENT_UNIT",
        ],
        dtype=str,
    )
    df["harmonization_omop::MEASUREMENT_UNIT"] = df["harmonization_omop::MEASUREMENT_UNIT"].fillna("NA")
    if verbose:
        print(f"[reference_data] harmonization_counts: {len(df)} rows loaded")
        print(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_unit_conversion(verbose: bool = False) -> pd.DataFrame:
    """Per-omopQuantity unit conversion factors: MEASUREMENT_UNIT (source) -> harmonization_omop::
    MEASUREMENT_UNIT (target), with harmonization_omop::CONVERSION_FACTOR (numeric or a formula
    string containing "X", e.g. "10.93*X-23.50").

    "NA"-fills the two unit columns for the same reason as get_harmonization_counts() (a blank
    unit can be a legitimate source/target for Presence/Threshold quantities). only_to_omop_concepts
    is deliberately left with real NaN (not "NA"-filled): a NaN there means "applies to any OMOP_ID
    with this quantity", while a real value means "only applies to this specific OMOP_ID" — future
    harmonize logic needs to distinguish the two with .isna(), not a string comparison.
    """
    _refresh_from_remote(config.UNIT_CONVERSION_URL, config.UNIT_CONVERSION_FILE, verbose=verbose)
    df = pd.read_csv(
        config.UNIT_CONVERSION_FILE,
        sep="\t",
        usecols=[
            "omop_quantity",
            "source_unit_valid",
            "to_source_unit_valid",
            "conversion",
            "only_to_omop_concepts",
        ],
        dtype=str,
    ).rename(
        columns={
            "omop_quantity": "harmonization_omop::omopQuantity",
            "source_unit_valid": "MEASUREMENT_UNIT",
            "to_source_unit_valid": "harmonization_omop::MEASUREMENT_UNIT",
            "conversion": "harmonization_omop::CONVERSION_FACTOR",
        }
    )
    df[["MEASUREMENT_UNIT", "harmonization_omop::MEASUREMENT_UNIT"]] = df[
        ["MEASUREMENT_UNIT", "harmonization_omop::MEASUREMENT_UNIT"]
    ].fillna("NA")
    if verbose:
        print(f"[reference_data] unit_conversion: {len(df)} rows loaded")
        print(df.head(3).to_string(index=False))
    return df


@lru_cache(maxsize=1)
def get_conversion_table(verbose: bool = False) -> pd.DataFrame:
    """Per-(OMOP_ID, omopQuantity, MEASUREMENT_UNIT) conversion factors, indexed for a direct
    row lookup of harmonization_omop::MEASUREMENT_UNIT (target) / CONVERSION_FACTOR.

    Merges get_unit_conversion() (conversion factors, keyed by omopQuantity + target unit, each
    either general or restricted to one specific OMOP_ID via only_to_omop_concepts) with
    get_harmonization_counts() (the target unit actually *chosen* for each OMOP_ID) on
    (omopQuantity, target unit) — this narrows the general conversion table down to only the
    rows that convert to the unit chosen for a given OMOP_ID, tagging each surviving row with
    that OMOP_ID. A row is then kept only if it's general (only_to_omop_concepts is NaN) or
    restricted to this exact OMOP_ID; a restriction naming a *different* OMOP_ID is dropped.

    When both a general and an OMOP-specific row survive for the same (OMOP_ID, omopQuantity,
    source MEASUREMENT_UNIT) key, the OMOP-specific one wins (mirrors old finngen_qc's
    _priority tie-break) — resolved once here rather than on every chunk.
    """
    unit_conversion = get_unit_conversion(verbose=verbose)
    harmonization_counts = get_harmonization_counts(verbose=verbose)

    merged = pd.merge(
        unit_conversion,
        harmonization_counts,
        on=["harmonization_omop::omopQuantity", "harmonization_omop::MEASUREMENT_UNIT"],
        how="inner",
    )

    applies = merged["only_to_omop_concepts"].isna() | (
        merged["only_to_omop_concepts"] == merged["harmonization_omop::OMOP_ID"]
    )
    merged = merged[applies].copy()

    join_cols = ["harmonization_omop::OMOP_ID", "harmonization_omop::omopQuantity", "MEASUREMENT_UNIT"]
    merged["_is_specific"] = merged["only_to_omop_concepts"].notna()
    table = (
        merged.sort_values("_is_specific", ascending=False)
        .drop_duplicates(subset=join_cols, keep="first")
        .drop(columns=["_is_specific", "only_to_omop_concepts"])
        .set_index(join_cols)
    )

    if verbose:
        print(f"[reference_data] conversion_table: {len(table)} (OMOP_ID, quantity, unit) conversions ready")
        print(table.head(3).to_string())
    return table


@lru_cache(maxsize=1)
def get_posneg_table(verbose: bool = False) -> dict[str, str]:
    """MEASUREMENT_FREE_TEXT -> extracted::IS_POS ("0"/"1") for text-based positive/negative
    results (e.g. "NEGAT" -> "0").

    Rows with a missing extracted::IS_POS are dropped (not a valid pos/neg signal). Built as a
    plain dict for .map(), not a merge table, so a duplicate MEASUREMENT_FREE_TEXT key can never
    fan out a row in df (none found in the current snapshot, but this guarantees it either way).
    """
    df = pd.read_csv(
        config.POSNEG_MAP_FILE,
        sep="\t",
        usecols=["MEASUREMENT_FREE_TEXT", "extracted::IS_POS"],
        dtype=str,
    ).dropna(subset=["extracted::IS_POS"])
    result = dict(zip(df["MEASUREMENT_FREE_TEXT"].drop_duplicates(keep="first"), df["extracted::IS_POS"]))
    if verbose:
        print(f"[reference_data] posneg_table: {len(result)} entries loaded, sample {_sample_dict(result)}")
    return result


@lru_cache(maxsize=1)
def get_ab_limits(verbose: bool = False) -> pd.DataFrame:
    """Per-OMOP_ID abnormality reference range: LOW_LIMIT/HIGH_LIMIT plus LOW_PROBLEM/HIGH_PROBLEM
    flags, indexed by harmonization_omop::OMOP_ID for a direct row lookup.

    Kept as dtype=str throughout (engine convention); LOW_LIMIT/HIGH_LIMIT are cast to numeric
    transiently by the caller for comparison — pd.to_numeric parses the literal "-inf"/"inf"
    tokens in the source file fine.
    """
    df = pd.read_csv(
        config.AB_LIMITS_FILE,
        sep="\t",
        dtype=str,
    ).rename(columns={"ID": "harmonization_omop::OMOP_ID"})
    df = df.drop_duplicates(subset="harmonization_omop::OMOP_ID", keep="first").set_index(
        "harmonization_omop::OMOP_ID"
    )
    if verbose:
        print(f"[reference_data] ab_limits: {len(df)} OMOP_ID reference ranges loaded")
        print(df.head(3).to_string())
    return df


@lru_cache(maxsize=1)
def get_omop_qc(verbose: bool = False) -> pd.DataFrame:
    """Per-OMOP_ID QC threshold rules: SIDE/THRESHOLD/QC_NOTES, usecols=["harmonization_omop::
    OMOP_ID", "THRESHOLD", "SIDE", "QC_NOTES"].

    Not deduplicated or indexed: a single OMOP_ID can carry several rules (e.g. both a "too high"
    and a "too low" implausible-value check), and some rows are placeholder "register this
    OMOP_ID as checked" entries with SIDE/THRESHOLD left blank — callers iterate rows directly.
    """
    df = pd.read_csv(
        config.OMOP_QC_FILE,
        sep="\t",
        usecols=["harmonization_omop::OMOP_ID", "THRESHOLD", "SIDE", "QC_NOTES"],
        dtype=str,
    )
    if verbose:
        print(f"[reference_data] omop_qc: {len(df)} rules loaded, {df['SIDE'].isna().sum()} placeholder-only")
        print(df.head(3).to_string())
    return df
