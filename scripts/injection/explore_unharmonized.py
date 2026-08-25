#!/usr/bin/env python3
"""
explore_unharmonized.py

Identifies lab measurements that hold a real source value but failed unit
harmonization (harmonization_omop::MEASUREMENT_VALUE == "NA"). Ground truth and
inference are kept in separate columns, never conflated:

  STATUS          Straight from the row's own harmonization_omop::OMOP_ID —
                   MAPPED if it's already a real concept, UNMAPPED if it's
                   "0"/"NA". This is a fact about the data, not a guess.
  OMOP_ID         The row's real OMOP_ID when STATUS=MAPPED; "NA" otherwise.
                   (When MAPPED, conversion itself is what's failing — no
                   factor defined for this unit, including MEASUREMENT_UNIT
                   == "NA" i.e. the unit is missing entirely.)
  CANDIDATE_OMOP  An inference, computed the same way regardless of STATUS:
                   what TEST_NAME_ABBREVIATION resolves to via its OTHER,
                   Usagi-APPROVED units (unit-agnostic) — e.g. "osuus" for
                   b-hkr isn't itself Usagi-approved, but the abbreviation's
                   other units ("ratio"/"%"/blank) all agree on one concept,
                   so that's the candidate. "AMBIGUOUS" if 2+ distinct
                   concepts, "NA" if the abbreviation isn't in Usagi at all.
  USAGI_STATUS    What Usagi itself says about this exact (TEST_NAME_ABBREVIATION,
                   MEASUREMENT_UNIT) pair: "APPROVED", "UNCHECKED" (a candidate
                   OMOP_ID is sitting right there, just not yet reviewed —
                   different from having no entry at all), or "MISSING" (no
                   Usagi row for this exact pair whatsoever).

Injection (--inject --test TEST_NAME): reverse-engineers the unit for a single
test's unharmonized rows. The discovery table above (built automatically if
missing) resolves the test's OMOP_ID; from there:

  UNAMBIGUOUS (the OMOP_ID's target unit has exactly one numeric candidate
  conversion factor in quantity_source_unit_conversion.tsv — including the
  identity row, source_unit == target_unit, CF=1, when one is defined — which
  covers rows whose value is already on the right scale but the unit itself
  is missing/unrecognized): try that one candidate directly.

  AMBIGUOUS (2+ candidates): reuses primary injection's bimodal-split
  machinery, but applied to the candidate VALUES themselves rather than to
  unit prevalence (which doesn't exist here — these rows have no unit to
  observe prevalence of). At most bimodal — one low/high separator, never 3+
  modes, matching injection_engine.bimodal_check's own ceiling. Every
  candidate is tried against the full population first; a low/high split is
  only preferred if bimodality is statistically confirmed or splitting
  demonstrably improves the fit (same split_improvement/same_best_unit guard
  primary injection uses, so a genuine physiological low/high split within
  one correct unit isn't mistaken for a unit-ambiguity signal).

--min-target-n gates on the OMOP_ID's own harmonized ground-truth population
size (every candidate reuses the same pooled ground truth, just rescaled by
1/CF, so there's only one population size to gate on, not one per candidate).
Formula-based conversion factors (e.g. "10.93*X-23.50") aren't invertible
here yet, so only numeric factors are considered. If a test's unharmonized
rows split across more than one raw OMOP_ID, the dominant one is used (a
known, documented limitation for now). Only single-test mode is implemented
— looping over every test in the corpus is future work.

Usage
-----
  python3 explore_unharmonized.py PARQUET [--min-count 50] [--out unharmonized_counts.tsv]
  python3 explore_unharmonized.py PARQUET --inject --test TEST_NAME
"""

import argparse
import subprocess
import sys
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_USAGI_FILE = _REPO_ROOT / "src/kanta/engine/data/LABfi.tsv"

sys.path.insert(0, str(_REPO_ROOT / "src"))

import explore_test_name as etn
import injection_engine
from kanta.engine import reference_data as rd


# ---------------------------------------------------------------------------
# ClickHouse
# ---------------------------------------------------------------------------

def clickhouse(query, **params):
    cmd = ["clickhouse", "-q", query]
    for k, v in params.items():
        cmd.append(f"--param_{k}={v}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise RuntimeError("ClickHouse query failed")
    return proc.stdout


# ---------------------------------------------------------------------------
# Discovery table
# ---------------------------------------------------------------------------

def query_raw_counts(parquet, min_count, cache="unharmonized_raw_counts.tsv"):
    """(TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID_RAW, COUNT) for every row with a real
    source value but a failed harmonized value. OMOP_ID_RAW is whatever omop_mapping already
    assigned the row (a real concept, or "0"/"NA" if that (abbreviation, unit) pair has no Usagi
    match at all) — resolve_omop_ids() turns this into a usable OMOP_ID."""
    if Path(cache).exists():
        print(f"{cache} already exists, skipping query.")
        return pd.read_csv(cache, sep="\t", keep_default_na=False, na_values=[""])

    result = clickhouse(f"""
        SELECT
            TEST_NAME_ABBREVIATION,
            MEASUREMENT_UNIT,
            `harmonization_omop::OMOP_ID` AS OMOP_ID_RAW,
            count() AS COUNT
        FROM file('{parquet}')
        WHERE MEASUREMENT_VALUE != 'NA'
          AND `harmonization_omop::MEASUREMENT_VALUE` = 'NA'
        GROUP BY TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID_RAW
        HAVING COUNT >= {min_count}
        ORDER BY COUNT DESC
        FORMAT TSVWithNames
    """)
    Path(cache).write_text(result)
    df = pd.read_csv(cache, sep="\t", keep_default_na=False, na_values=[""])
    print(f"Wrote {cache}  ({len(df)} rows)")
    return df


def query_raw_counts_single(parquet, test_name):
    """Same shape as query_raw_counts, but for exactly one TEST_NAME_ABBREVIATION and no
    --min-count floor — used to resolve a --test target that fell under the discovery table's
    threshold (or wasn't in it because the table was built before this test's data existed).

    Selects/groups by the real TEST_NAME_ABBREVIATION column rather than projecting the
    parameter as a same-named alias — aliasing a literal to a name that collides with a real
    column lets ClickHouse resolve the WHERE clause against the (always-true) alias instead of
    the actual column, silently turning the filter into a no-op across the whole file."""
    result = clickhouse(f"""
        SELECT
            TEST_NAME_ABBREVIATION,
            MEASUREMENT_UNIT,
            `harmonization_omop::OMOP_ID` AS OMOP_ID_RAW,
            count() AS COUNT
        FROM file('{parquet}')
        WHERE TEST_NAME_ABBREVIATION = {{name:String}}
          AND MEASUREMENT_VALUE != 'NA'
          AND `harmonization_omop::MEASUREMENT_VALUE` = 'NA'
        GROUP BY TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID_RAW
        FORMAT TSVWithNames
    """, name=test_name)
    if not result.strip():
        return pd.DataFrame(columns=["TEST_NAME_ABBREVIATION", "MEASUREMENT_UNIT", "OMOP_ID_RAW", "COUNT"])
    return pd.read_csv(StringIO(result), sep="\t", keep_default_na=False, na_values=[""])


def load_abbrev_omop_map(usagi_path):
    """TEST_NAME_ABBREVIATION -> sorted list of distinct OMOP_IDs it maps to via ANY of its
    APPROVED units in the Usagi table (unit-agnostic, unlike omop_mapping's own per-row
    (abbreviation, unit) join)."""
    df = pd.read_csv(usagi_path, sep="\t", dtype=str)
    df = df[df["harmonization_omop::MAPPING_STATUS"] == "APPROVED"]
    grouped = df.groupby("TEST_NAME_ABBREVIATION")["harmonization_omop::OMOP_ID"].agg(lambda s: sorted(set(s)))
    return grouped.to_dict()


def load_usagi_status_map(usagi_path):
    """(TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT) -> MAPPING_STATUS, straight from Usagi. Blank
    units are normalized to "NA" to match the engine's own missing-unit convention. Pairs with no
    Usagi row at all aren't in this dict — callers should default to "MISSING"."""
    df = pd.read_csv(usagi_path, sep="\t", dtype=str, keep_default_na=False, na_values=[""])
    df["MEASUREMENT_UNIT"] = df["MEASUREMENT_UNIT"].replace("", "NA")
    return dict(zip(
        zip(df["TEST_NAME_ABBREVIATION"], df["MEASUREMENT_UNIT"]),
        df["harmonization_omop::MAPPING_STATUS"],
    ))


def _candidate_omop(abbrev, abbrev_map):
    ids = abbrev_map.get(abbrev)
    if not ids:
        return "NA"
    if len(ids) == 1:
        return ids[0]
    return "AMBIGUOUS"


def resolve_omop_ids(df, abbrev_map, usagi_status_map):
    """Adds STATUS (ground truth, straight from OMOP_ID_RAW), OMOP_ID (real ID when MAPPED, "NA"
    otherwise), CANDIDATE_OMOP (abbreviation-fallback inference, computed the same way regardless
    of STATUS), and USAGI_STATUS (this exact (abbreviation, unit) pair's status in Usagi itself —
    APPROVED/UNCHECKED/MISSING). Then re-aggregates COUNT since rows that only differed by a
    now-collapsed OMOP_ID_RAW (e.g. several "0"/"NA" variants) can merge into the same output row."""
    df = df.copy()
    df["STATUS"] = np.where(df["OMOP_ID_RAW"].isin(["0", "NA", ""]), "UNMAPPED", "MAPPED")
    df["OMOP_ID"] = np.where(df["STATUS"] == "MAPPED", df["OMOP_ID_RAW"], "NA")
    df["CANDIDATE_OMOP"] = df["TEST_NAME_ABBREVIATION"].apply(lambda a: _candidate_omop(a, abbrev_map))
    df["USAGI_STATUS"] = [
        usagi_status_map.get((t, u), "MISSING")
        for t, u in zip(df["TEST_NAME_ABBREVIATION"], df["MEASUREMENT_UNIT"])
    ]

    out = (
        df.groupby(["TEST_NAME_ABBREVIATION", "MEASUREMENT_UNIT", "STATUS", "OMOP_ID",
                    "CANDIDATE_OMOP", "USAGI_STATUS"])["COUNT"]
        .sum()
        .reset_index()
        .sort_values("COUNT", ascending=False)
        .reset_index(drop=True)
    )
    return out


def print_summary(df):
    total_rows = int(df["COUNT"].sum())
    print(f"\n{'=' * 60}")
    print("UNHARMONIZED SUMMARY")
    print(f"{'=' * 60}")
    print(f"  (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT) rows: {len(df):,}")
    print(f"  Distinct TEST_NAME_ABBREVIATION: {df['TEST_NAME_ABBREVIATION'].nunique():,}")
    print(f"  Distinct OMOP_ID (ground truth, MAPPED): "
          f"{df.loc[df['STATUS'] == 'MAPPED', 'OMOP_ID'].nunique():,}")
    print(f"  Total measurements affected:     {total_rows:,}")
    print()
    print(f"  {'STATUS':<10}  {'ROWS':>8}  {'MEASUREMENTS':>14}")
    for status, sub in df.groupby("STATUS"):
        n = int(sub["COUNT"].sum())
        print(f"  {status:<10}  {len(sub):>8,}  {n:>14,}  ({100 * n / total_rows:.1f}%)")

    unmapped = df[df["STATUS"] == "UNMAPPED"]
    if len(unmapped):
        n_unmapped = int(unmapped["COUNT"].sum())
        print(f"\n  Of {n_unmapped:,} UNMAPPED measurements, by CANDIDATE_OMOP resolvability:")
        cand_kind = np.select(
            [unmapped["CANDIDATE_OMOP"] == "AMBIGUOUS", unmapped["CANDIDATE_OMOP"] == "NA"],
            ["AMBIGUOUS (2+ concepts)", "NA (abbreviation not in Usagi)"],
            default="single candidate",
        )
        for kind, sub in unmapped.groupby(cand_kind):
            n = int(sub["COUNT"].sum())
            print(f"    {kind:<32}  {len(sub):>8,}  {n:>14,}  ({100 * n / n_unmapped:.1f}%)")

        print(f"\n  Of {n_unmapped:,} UNMAPPED measurements, by USAGI_STATUS for this exact (abbrev, unit) pair:")
        for status, sub in unmapped.groupby("USAGI_STATUS"):
            n = int(sub["COUNT"].sum())
            print(f"    {status:<32}  {len(sub):>8,}  {n:>14,}  ({100 * n / n_unmapped:.1f}%)")
    print()
    no_unit = df[df["MEASUREMENT_UNIT"] == "NA"]["COUNT"].sum()
    print(f"  Of which MEASUREMENT_UNIT == NA: {int(no_unit):,} "
          f"({100 * no_unit / total_rows:.1f}%)")
    print()


# ---------------------------------------------------------------------------
# Injection — OMOP_ID / quantity / target-unit / candidate-CF resolution
# ---------------------------------------------------------------------------

def resolve_test_omop_id(df, test_name, parquet, abbrev_map, usagi_status_map, min_count):
    """Picks a single working OMOP_ID for test_name out of the discovery table `df` (built by
    resolve_omop_ids). Falls back to a fresh zero-threshold query for just this one test if it
    isn't present — e.g. because its rows fell under --min-count — reusing the exact same
    resolution logic (resolve_omop_ids) rather than duplicating it.

    Prefers real ground-truth MAPPED rows (by volume, if it splits across more than one); only
    falls back to the CANDIDATE_OMOP inference if nothing is actually MAPPED yet. Returns
    (omop_id, source) where source is "MAPPED"/"ABBREV_FALLBACK" (how the ID was obtained) or
    (None, "AMBIGUOUS"/"UNMAPPED"/"NOT_FOUND") if nothing usable was found."""
    rows = df[df["TEST_NAME_ABBREVIATION"] == test_name]
    if rows.empty:
        print(f"  '{test_name}' not in the {len(df)}-row discovery table "
              f"(below --min-count={min_count}?) — querying it directly")
        raw = query_raw_counts_single(parquet, test_name)
        if raw.empty:
            return None, "NOT_FOUND"
        rows = resolve_omop_ids(raw, abbrev_map, usagi_status_map)

    mapped = rows[rows["STATUS"] == "MAPPED"]
    if not mapped.empty:
        agg = mapped.groupby("OMOP_ID")["COUNT"].sum().sort_values(ascending=False)
        if len(agg) > 1:
            print(f"  NOTE: '{test_name}' splits across {len(agg)} OMOP_IDs {dict(agg)} — using the dominant one")
        return agg.index[0], "MAPPED"

    candidates = rows[~rows["CANDIDATE_OMOP"].isin(["AMBIGUOUS", "NA"])]
    if candidates.empty:
        fallback = "AMBIGUOUS" if (rows["CANDIDATE_OMOP"] == "AMBIGUOUS").any() else "UNMAPPED"
        return None, fallback

    agg = candidates.groupby("CANDIDATE_OMOP")["COUNT"].sum().sort_values(ascending=False)
    if len(agg) > 1:
        print(f"  NOTE: '{test_name}' candidate splits across {len(agg)} OMOP_IDs {dict(agg)} — using the dominant one")
    return agg.index[0], "ABBREV_FALLBACK"


def _query_candidate_values(parquet, test_name):
    """Raw MEASUREMENT_VALUE for this test's currently-unharmonized rows (real value, failed
    harmonization) — untouched, no unit filter."""
    q = f"""
        SELECT MEASUREMENT_VALUE AS value
        FROM file('{parquet}')
        WHERE TEST_NAME_ABBREVIATION = {{name:String}}
          AND MEASUREMENT_VALUE != 'NA'
          AND `harmonization_omop::MEASUREMENT_VALUE` = 'NA'
        FORMAT TSV
    """
    out = clickhouse(q, name=test_name)
    vals = []
    if out.strip():
        for line in out.strip().split("\n"):
            try:
                vals.append(float(line))
            except ValueError:
                pass
    return np.array(vals, dtype=float)


def _query_harmonized_ground_truth(parquet, omop_id):
    """All already-harmonized (value, unit) pairs for this OMOP_ID, pooled across every test that
    maps to it — this is the ground-truth population, assumed correct."""
    q = f"""
        SELECT
            `harmonization_omop::MEASUREMENT_VALUE` AS value,
            `harmonization_omop::MEASUREMENT_UNIT`  AS unit
        FROM file('{parquet}')
        WHERE `harmonization_omop::OMOP_ID` = {{omop_id:String}}
          AND `harmonization_omop::MEASUREMENT_VALUE` != 'NA'
        FORMAT TSV
    """
    out = clickhouse(q, omop_id=str(omop_id))
    vals, units = [], []
    if out.strip():
        for line in out.strip().split("\n"):
            try:
                v, u = line.split("\t")
                vals.append(float(v))
                units.append(u)
            except ValueError:
                pass
    if not vals:
        return np.array([]), None

    vals = np.array(vals, dtype=float)
    units = np.array(units)
    unit_counts = pd.Series(units).value_counts()
    target_unit = unit_counts.index[0]
    if len(unit_counts) > 1:
        print(f"  NOTE: harmonized ground truth for OMOP_ID {omop_id} spans "
              f"{len(unit_counts)} units {dict(unit_counts)} — using the dominant one")
    return vals[units == target_unit], target_unit


def get_omop_quantity(omop_id):
    mapping = rd.get_usagi_mapping()
    rows = mapping[mapping["harmonization_omop::OMOP_ID"] == str(omop_id)]
    quantities = [q for q in rows["harmonization_omop::OMOP_QUANTITY"].unique() if q not in ("NA", "")]
    if not quantities:
        return None
    if len(quantities) > 1:
        print(f"  NOTE: OMOP_ID {omop_id} has {len(quantities)} distinct quantities "
              f"{quantities} — using the first")
    return quantities[0]


def find_candidate_conversions(quantity, target_unit, omop_id):
    """Every (source_unit, CF) pair whose TO_MEASUREMENT_UNIT is target_unit for this quantity,
    honoring ONLY_TO_OMOP_CONCEPTS. Numeric CF only. Deliberately includes the identity row
    (source_unit == target_unit, CF=1) when the conversion table has one."""
    conv = rd.get_unit_conversion()
    cand = conv[
        (conv["harmonization_omop::OMOP_QUANTITY"] == quantity)
        & (conv["harmonization_omop::MEASUREMENT_UNIT"] == target_unit)
        & (conv["ONLY_TO_OMOP_CONCEPTS"].isna() | (conv["ONLY_TO_OMOP_CONCEPTS"] == str(omop_id)))
    ].copy()

    def _numeric(cf):
        try:
            float(cf)
            return True
        except (TypeError, ValueError):
            return False

    cand = cand[cand["harmonization_omop::CONVERSION_FACTOR"].apply(_numeric)]
    return [
        (row["MEASUREMENT_UNIT"], float(row["harmonization_omop::CONVERSION_FACTOR"]))
        for _, row in cand.iterrows()
    ]


def _pass_rank(row):
    """Lower is better: KS-decided PASS beats T-decided beats MAD-decided beats FAIL beats SKIP —
    mirrors explore_test_name.py's run_ambiguous ranking (no UNIT_PREVALENCE tiebreak available
    here, so ties break on KS_STAT alone)."""
    if row["KS_PASS"] == "PASS": return 0
    if row["T_PASS"]  == "PASS": return 1
    if row["OUTCOME"] == "PASS": return 2   # MAD decided
    if row["OUTCOME"] == "FAIL": return 3
    return 4


# ---------------------------------------------------------------------------
# Injection — single-test run
# ---------------------------------------------------------------------------

def run_test_injection(parquet, test_name, df, abbrev_map, usagi_status_map, dump_dir,
                       min_target_n=100, dip_threshold=0.05, split_threshold=0.15,
                       min_count=50):
    print(f"Resolving OMOP_ID for {test_name}...")
    omop_id, source = resolve_test_omop_id(df, test_name, parquet, abbrev_map, usagi_status_map, min_count)
    if omop_id is None:
        print(f"  SKIP: OMOP_ID resolution = {source}")
        return None
    print(f"  OMOP_ID={omop_id}  ({source})")

    c_vals = _query_candidate_values(parquet, test_name)
    print(f"  N_candidate (unharmonized, real value): {len(c_vals):,}")
    if len(c_vals) < 2:
        print("  SKIP: fewer than 2 candidate values")
        return None

    quantity = get_omop_quantity(omop_id)
    if quantity is None:
        print(f"  SKIP: no omop_quantity found for OMOP_ID {omop_id}")
        return None
    print(f"  omop_quantity={quantity}")

    ground_truth, target_unit = _query_harmonized_ground_truth(parquet, omop_id)
    if target_unit is None or len(ground_truth) < min_target_n:
        print(f"  SKIP: OMOP_ID {omop_id} harmonized population ({len(ground_truth)}) "
              f"< --min-target-n={min_target_n}")
        return None
    print(f"  target_unit={target_unit}  N_target(raw)={len(ground_truth):,}")

    candidates = find_candidate_conversions(quantity, target_unit, omop_id)
    if not candidates:
        print(f"  SKIP: no numeric conversion factor into {target_unit} for {quantity}")
        return None

    unit_data = {cand_unit: ground_truth / cf for cand_unit, cf in candidates}
    mode = "UNAMBIGUOUS" if len(candidates) == 1 else "AMBIGUOUS"
    print(f"  {len(candidates)} candidate conversion(s) into {target_unit}: "
          f"{[f'{u}(CF={cf:.4g})' for u, cf in candidates]}  -> {mode}")

    def _try(c_arr, cand_unit, tag):
        return etn._run_engine(
            c_arr, unit_data[cand_unit], f"{test_name} [unharm{tag}] [{cand_unit}]", dump_dir,
            prevalence=None,
        )

    def _row(c_arr, cand_unit, cf, sub_dist, updates):
        return dict(
            TEST_NAME=test_name, OMOP_ID=omop_id, SOURCE=source, OMOP_QUANTITY=quantity,
            SUB_DIST=sub_dist, CANDIDATE_UNIT=cand_unit, TARGET_UNIT=target_unit,
            CONVERSION_FACTOR=cf, N_CANDIDATE=len(c_arr), N_TARGET=len(unit_data[cand_unit]),
            **updates,
        )

    def _log(tag, cand_unit, cf, c_arr, ks, t, mad, outcome):
        print(f"    [{tag}][{cand_unit:<10} CF={cf:.4g}]  N={len(c_arr):,}/{len(unit_data[cand_unit]):,}"
              f"  KS={'P' if ks.passed else 'F'}(stat={ks.details['stat']:.3g})"
              f"  T={'P' if t.passed else 'F'}"
              f"  MAD={'P' if mad.passed else 'F'}"
              f"  → {outcome}")

    # ---- UNAMBIGUOUS: exactly one candidate, no bimodal machinery needed ----
    # (splitting the candidate population couldn't change the outcome anyway —
    # both halves would just be compared against the same single hypothesis.)
    if len(candidates) == 1:
        cand_unit, cf = candidates[0]
        updates, ks, t, mad, _ = _try(c_vals, cand_unit, "")
        _log("all", cand_unit, cf, c_vals, ks, t, mad, updates["OUTCOME"])
        best = _row(c_vals, cand_unit, cf, "all", updates)
        print(f"\n  BEST: {best['CANDIDATE_UNIT']} (CF={best['CONVERSION_FACTOR']:.6g})"
              f"  → {best['OUTCOME']}  ({best['NOTES']})  [unambiguous, 1 candidate]")
        return best

    # ---- AMBIGUOUS: 2+ candidates — pre-check, then at-most-bimodal split ----
    print(f"  pre-check (full candidate)  units={list(unit_data)}")
    precheck = []
    for cand_unit, cf in candidates:
        updates, ks, t, mad, _ = _try(c_vals, cand_unit, " all")
        _log("all", cand_unit, cf, c_vals, ks, t, mad, updates["OUTCOME"])
        precheck.append((cand_unit, cf, updates))

    any_precheck_pass = any(u["OUTCOME"] == "PASS" for _, _, u in precheck)

    bim = injection_engine.bimodal_check(c_vals, dip_threshold=dip_threshold)
    injection_engine.plot_bimodal_check(bim, test_name, dump_dir)
    print(f"  bimodal={bim.status}  sep={bim.separator:.4g}  BC={bim.bc:.3f}  dip_p={bim.dip_p:.3g}")

    prefer_split = False
    si = {}
    sep = bim.separator
    c_low  = c_vals[c_vals <= sep] if not np.isnan(sep) else np.array([])
    c_high = c_vals[c_vals >  sep] if not np.isnan(sep) else np.array([])
    if len(c_low) >= 2 and len(c_high) >= 2:
        si = injection_engine.split_improvement(c_vals, c_low, c_high, unit_data)
        print(f"  split_improvement={si['improvement']:+.1%}  same_best_unit={si['same_best_unit']}"
              f"  (global_KS={si['global_score']:.4f}  split_KS={si['split_score']:.4f})")
        if si["improvement"] > split_threshold and not si["same_best_unit"]:
            prefer_split = True
            print(f"  → SPLIT preferred (threshold={split_threshold:.0%})")

    trials = []
    if any_precheck_pass and not prefer_split:
        print("  pre-check passed → global result kept")
        trials = [_row(c_vals, u, cf, "all", upd) for u, cf, upd in precheck]
    else:
        do_split = prefer_split or bim.status in ("bimodal", "bimodal_cautious")
        if not do_split:
            print("  unimodal, pre-check failed, split not preferred → reusing pre-check results")
            trials = [_row(c_vals, u, cf, "all", upd) for u, cf, upd in precheck]
        else:
            sub_dists = []
            if len(c_low)  >= 2: sub_dists.append(("low",  c_low))
            if len(c_high) >= 2: sub_dists.append(("high", c_high))
            if not sub_dists:
                sub_dists = [("all", c_vals)]
            print(f"  sub_dists={[s for s, _ in sub_dists]}")
            for sub_name, c_sub in sub_dists:
                for cand_unit, cf in candidates:
                    updates, ks, t, mad, _ = _try(c_sub, cand_unit, f" {sub_name}")
                    _log(sub_name, cand_unit, cf, c_sub, ks, t, mad, updates["OUTCOME"])
                    trials.append(_row(c_sub, cand_unit, cf, sub_name, updates))

    by_sub = {}
    for row in trials:
        by_sub.setdefault(row["SUB_DIST"], []).append(row)
    best_rows = [min(rows, key=lambda r: (_pass_rank(r), r["KS_STAT"])) for rows in by_sub.values()]

    for row in best_rows:
        print(f"\n  BEST[{row['SUB_DIST']}]: {row['CANDIDATE_UNIT']} (CF={row['CONVERSION_FACTOR']:.6g})"
              f"  → {row['OUTCOME']}  ({row['NOTES']})")

    return best_rows if len(best_rows) > 1 else best_rows[0]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="Extract unit-harmonization-failed lab measurements, resolving an OMOP_ID "
                    "either from the row's own mapping or (as a fallback) its abbreviation, and "
                    "optionally reverse-engineer the missing unit for one test via --inject --test.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("parquet", help="Input parquet file (engine output, e.g. kanta.parquet)")
    p.add_argument("--min-count", type=int, default=50, metavar="INT",
                   help="Minimum row count per (TEST_NAME_ABBREVIATION, MEASUREMENT_UNIT, OMOP_ID_RAW) to include")
    p.add_argument("--usagi", default=str(_DEFAULT_USAGI_FILE), metavar="PATH",
                   help="Usagi lab mapping TSV, for the abbreviation-level OMOP_ID fallback")
    p.add_argument("--out", default="unharmonized_counts.tsv", metavar="PATH",
                   help="Output TSV path (final, resolved discovery table)")
    p.add_argument("--raw-cache", default="unharmonized_raw_counts.tsv", metavar="PATH",
                   help="Cache path for the (expensive) raw ClickHouse query; skips re-querying if present")
    p.add_argument("--inject", action="store_true",
                   help="Reverse-engineer the unit for --test TEST_NAME's unharmonized rows")
    p.add_argument("--test", metavar="TEST_NAME", default=None,
                   help="TEST_NAME_ABBREVIATION to run --inject against (required with --inject; "
                        "single-test mode only for now)")
    p.add_argument("--min-target-n", type=int, default=100, metavar="INT",
                   help="Minimum harmonized ground-truth population an OMOP_ID needs to attempt injection")
    p.add_argument("--dip-threshold", type=float, default=0.05, metavar="FLOAT",
                   help="Hartigan dip test p-value threshold for bimodality detection")
    p.add_argument("--split-threshold", type=float, default=0.15, metavar="FLOAT",
                   help="Minimum relative KS improvement to prefer a split over the global fit")
    p.add_argument("--dump-dir", default="dump_unharm", metavar="PATH",
                   help="Cache/plot directory for the injection engine")
    p.add_argument("--out-injection", default="unharmonized_injection_results.tsv", metavar="PATH",
                   help="Output TSV for --inject results")
    return p


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.inject and not args.test:
        parser.error("--inject requires --test TEST_NAME")

    raw = query_raw_counts(args.parquet, args.min_count, cache=args.raw_cache)
    abbrev_map = load_abbrev_omop_map(args.usagi)
    usagi_status_map = load_usagi_status_map(args.usagi)
    df = resolve_omop_ids(raw, abbrev_map, usagi_status_map)
    df.to_csv(args.out, sep="\t", index=False)
    print(f"Wrote {args.out}  ({len(df)} rows)")
    print_summary(df)

    if args.inject:
        Path(args.dump_dir).mkdir(parents=True, exist_ok=True)
        result = run_test_injection(
            args.parquet, args.test, df, abbrev_map, usagi_status_map, args.dump_dir,
            min_target_n=args.min_target_n,
            dip_threshold=args.dip_threshold,
            split_threshold=args.split_threshold,
            min_count=args.min_count,
        )
        if result is None:
            return
        rows = result if isinstance(result, list) else [result]
        pd.DataFrame(rows).to_csv(args.out_injection, sep="\t", index=False,
                                  float_format="%.6g", na_rep="NA")
        print(f"\nWrote {args.out_injection}  ({len(rows)} row(s))")


if __name__ == "__main__":
    main()
