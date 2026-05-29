#!/usr/bin/env python3
"""
explore_test_name.py

Identifies lab measurements that have a numeric value but no unit, characterises
the unit distribution of matching records that do have a unit, and optionally
runs the injection engine to validate each candidate TEST_NAME.

Usage
-----
  python3 explore_test_name.py PARQUET [options]
"""

import argparse
import json
import os
import re
import subprocess
import sys
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd

import omop_unit_table as _omop

import injection_engine
import split_eval
from plot_exploration import make_scatter_plot


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
# Queries
# ---------------------------------------------------------------------------

_COUNTS_MIN = 50  # fixed baseline for the cached query; --min-count filters post-load


def query_counts(parquet, out="test_name_counts.tsv"):
    if Path(out).exists():
        df = pd.read_csv(out, sep="\t", na_values=["\\N"])
        if "OMOP_CONCEPT_ID" in df.columns:
            df["OMOP_CONCEPT_ID"] = pd.to_numeric(df["OMOP_CONCEPT_ID"], errors="coerce").astype("Int64")
            print(f"{out} already exists, skipping.")
            return df
        print(f"{out} exists but missing OMOP_CONCEPT_ID — re-querying.")

    result = clickhouse(f"""
        SELECT TEST_NAME, count() AS COUNT, any(OMOP_CONCEPT_ID) AS OMOP_CONCEPT_ID
        FROM file('{parquet}')
        WHERE (MEASUREMENT_VALUE_EXTRACTED IS NOT NULL OR MEASUREMENT_VALUE_SOURCE IS NOT NULL)
          AND MEASUREMENT_UNIT_PRE_FIX IS NULL
        GROUP BY TEST_NAME
        HAVING COUNT > {_COUNTS_MIN}
        ORDER BY COUNT DESC
        FORMAT TSVWithNames
    """)
    Path(out).write_text(result)
    df = pd.read_csv(out, sep="\t", na_values=["\\N"])
    df["OMOP_CONCEPT_ID"] = pd.to_numeric(df["OMOP_CONCEPT_ID"], errors="coerce").astype("Int64")
    print(f"Wrote {out}  ({len(df)} rows)")
    return df


def query_details(parquet, counts_file="test_name_counts.tsv", out="test_name_details.tsv"):
    if Path(out).exists():
        print(f"{out} already exists, skipping.")
        return pd.read_csv(out, sep="\t")

    counts_abs = str(Path(counts_file).resolve())
    result = clickhouse(f"""
        WITH
        global_names AS (
            SELECT DISTINCT TEST_NAME
            FROM file('{counts_abs}', TSVWithNames)
        ),
        top3_units AS (
            SELECT
                TEST_NAME,
                argMax(MEASUREMENT_UNIT_PRE_FIX, unit_cnt) AS UNIT,
                concat('{{', arrayStringConcat(groupArray(unit_json), ','), '}}') AS PREVALENCE_DICT
            FROM (
                SELECT TEST_NAME, MEASUREMENT_UNIT_PRE_FIX, unit_cnt, unit_json
                FROM (
                    SELECT
                        TEST_NAME, MEASUREMENT_UNIT_PRE_FIX, unit_cnt,
                        ROW_NUMBER() OVER (PARTITION BY TEST_NAME ORDER BY unit_cnt DESC) AS rn,
                        concat(
                            MEASUREMENT_UNIT_PRE_FIX, ':',
                            toString(round(100.0 * unit_cnt / SUM(unit_cnt) OVER (PARTITION BY TEST_NAME), 2))
                        ) AS unit_json
                    FROM (
                        SELECT TEST_NAME, MEASUREMENT_UNIT_PRE_FIX, count() AS unit_cnt
                        FROM file('{parquet}')
                        WHERE MEASUREMENT_VALUE_SOURCE IS NOT NULL
                          AND MEASUREMENT_UNIT_PRE_FIX IS NOT NULL
                        GROUP BY TEST_NAME, MEASUREMENT_UNIT_PRE_FIX
                    ) AS sub
                ) AS ranked
                WHERE rn <= 3
            ) AS top3
            GROUP BY TEST_NAME
        ),
        total_counts AS (
            SELECT TEST_NAME, count() AS COUNT
            FROM file('{parquet}')
            WHERE MEASUREMENT_VALUE_SOURCE IS NOT NULL
              AND MEASUREMENT_UNIT_PRE_FIX IS NOT NULL
            GROUP BY TEST_NAME
        )
        SELECT
            t.TEST_NAME       AS TEST_NAME,
            t.COUNT           AS COUNT,
            u.UNIT            AS UNIT,
            u.PREVALENCE_DICT AS PREVALENCE_DICT
        FROM total_counts t
        LEFT JOIN top3_units u USING (TEST_NAME)
        INNER JOIN global_names g USING (TEST_NAME)
        ORDER BY t.COUNT DESC
        FORMAT TSVWithNames
    """)
    Path(out).write_text(result)
    df = pd.read_csv(out, sep="\t")
    print(f"Wrote {out}  ({len(df)} rows)")
    return df


# ---------------------------------------------------------------------------
# Plot table
# ---------------------------------------------------------------------------

def load_omop_unit_table(parquet, out="omop_unit_table.tsv"):
    if Path(out).exists():
        df = pd.read_csv(out, sep="\t")
        if "OMOP_TOTAL_N" in df.columns:
            print(f"{out} already exists, skipping.")
            df["OMOP_CONCEPT_ID"] = pd.to_numeric(df["OMOP_CONCEPT_ID"], errors="coerce").astype("Int64")
            df["OMOP_TOTAL_N"]    = df["OMOP_TOTAL_N"].astype("Int64")
            return df
        print(f"{out} exists but missing OMOP_TOTAL_N — adding counts.")
    else:
        print("Building OMOP unit table...")
        lab_df  = _omop.load_lab(_omop.DATA_DIR / "LABfi_ALL.usagi.csv")
        conv_df = _omop.load_conversions(_omop.DATA_DIR / "quantity_source_unit_conversion.tsv")
        df = _omop.build_table(lab_df, conv_df)

    df["OMOP_CONCEPT_ID"] = pd.to_numeric(df["OMOP_CONCEPT_ID"], errors="coerce").astype("Int64")
    raw = clickhouse(f"""
        SELECT OMOP_CONCEPT_ID, count() AS OMOP_TOTAL_N
        FROM file('{parquet}')
        WHERE OMOP_CONCEPT_ID IS NOT NULL
        GROUP BY OMOP_CONCEPT_ID
        FORMAT TSVWithNames
    """)
    counts = pd.read_csv(StringIO(raw), sep="\t")
    counts["OMOP_CONCEPT_ID"] = pd.to_numeric(counts["OMOP_CONCEPT_ID"], errors="coerce").astype("Int64")
    counts["OMOP_TOTAL_N"]    = counts["OMOP_TOTAL_N"].astype("Int64")
    df = df.merge(counts, on="OMOP_CONCEPT_ID", how="outer")
    df.to_csv(out, sep="\t", index=False, na_rep="NA")
    print(f"  Wrote {out}  ({len(df)} rows)")
    return df


def build_plot_table(counts, details):
    def parse_top_prevalence(d):
        if pd.isna(d):
            return None
        pcts = re.findall(r':(\d+\.?\d*)[,}]', str(d))
        return max(float(p) for p in pcts) if pcts else None

    details = details.copy()
    details["top_prevalence"] = details["PREVALENCE_DICT"].apply(parse_top_prevalence)

    plot_name = counts.merge(
        details[["TEST_NAME", "top_prevalence", "COUNT"]].rename(columns={"COUNT": "N_WITH_UNIT"}),
        on="TEST_NAME", how="left",
    )
    plot_name["top_prevalence"] = plot_name["top_prevalence"].fillna(0)
    plot_name["N_WITH_UNIT"]    = plot_name["N_WITH_UNIT"].fillna(0).astype(int)
    return plot_name


# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

_SUMMARY_THRESHOLDS = [95, 98, 99, 100]


def _cat_at(df, t, min_target_n):
    return np.select(
        [
            df["N_WITH_UNIT"] < min_target_n,
            df["top_prevalence"] >= t,
            df["top_prevalence"] > 0,
        ],
        ["NO_DATA", "UNAMBIGUOUS", "AMBIGUOUS"],
        default="NO_DATA",
    )


def _add_category(plot_name, threshold, min_target_n):
    """Add CATEGORY (operational) and CATEGORY_{t} (exploratory) columns."""
    df = plot_name.copy()
    df["CATEGORY"] = _cat_at(df, threshold, min_target_n)
    for t in _SUMMARY_THRESHOLDS:
        df[f"CATEGORY_{t}"] = _cat_at(df, t, min_target_n)
    return df


def print_summary(plot_name, threshold):
    total   = len(plot_name)
    total_n = int(plot_name["COUNT"].sum())

    tw = 13  # test_names cell width
    mw = 18  # measurements cell width

    cats = ["UNAMBIGUOUS", "AMBIGUOUS", "NO_DATA"]

    # Header
    print()
    h1 = f"{'':>12}"
    h2 = f"{'Threshold':>10}  "
    sep = f"{'':>12}"
    for cat in cats:
        h1  += f"  {cat:^{tw+mw+2}}"
        h2  += f"  {'TEST_NAMES':>{tw}}  {'MEASUREMENTS':>{mw}}"
        sep += f"  {'-'*tw}  {'-'*mw}"
    print(h1)
    print(h2)
    print(sep)

    for t in _SUMMARY_THRESHOLDS:
        marker = " *" if t == threshold else "  "
        row = f"{t:>9}%{marker}"
        for cat in cats:
            sub = plot_name[plot_name[f"CATEGORY_{t}"] == cat]
            n   = len(sub)
            m   = int(sub["COUNT"].sum())
            row += f"  {f'{n:,} ({100*n/total:.1f}%)':>{tw}}  {f'{m:,} ({100*m/total_n:.1f}%)':>{mw}}"
        print(row)

    print(sep)
    print(f"{'TOTAL':>12}  {total:>{tw},}{'':>{mw+4}}"
          f"  {'':>{tw}}  {total_n:>{mw},}")
    print(f"\n  * = active threshold ({threshold}%)\n")


def print_assignment_summary(udf, adf, no_data, plot_name, out_doc=None, out_tsv=None):
    """Fraction of TEST_NAMEs/measurements successfully assigned a unit."""
    count_map  = plot_name.set_index("TEST_NAME")["COUNT"].to_dict()
    omop_names = set(plot_name[plot_name["OMOP_CONCEPT_ID"].notna()]["TEST_NAME"])

    def _safe(df):
        return df if df is not None and len(df) else None

    udf_     = _safe(udf)
    adf_     = _safe(adf)
    no_data_ = _safe(no_data)

    assigned = {
        "UNAMBIGUOUS": set(udf_[udf_["OUTCOME"] == "PASS"]["TEST_NAME"])         if udf_     is not None else set(),
        "AMBIGUOUS":   set(adf_[adf_["OUTCOME"] == "PASS"]["TEST_NAME"])         if adf_     is not None else set(),
        "NO_DATA":     set(no_data_[no_data_["UNIT"].notna()]["TEST_NAME"])       if no_data_ is not None else set(),
    }
    processed = {
        "UNAMBIGUOUS": set(udf_["TEST_NAME"])     if udf_     is not None else set(),
        "AMBIGUOUS":   set(adf_["TEST_NAME"])     if adf_     is not None else set(),
        "NO_DATA":     set(no_data_["TEST_NAME"]) if no_data_ is not None else set(),
    }

    def _meas(names):
        return sum(count_map.get(n, 0) for n in names)

    def _build_row(name_filter=None):
        cells    = []
        all_asgn = set()
        all_proc = set()
        for cat in ["UNAMBIGUOUS", "AMBIGUOUS", "NO_DATA"]:
            asgn = assigned[cat]  if name_filter is None else assigned[cat]  & name_filter
            proc = processed[cat] if name_filter is None else processed[cat] & name_filter
            all_asgn |= asgn
            all_proc |= proc
            cells.append((f"{len(asgn):,}/{len(proc):,}", f"{_meas(asgn):,}/{_meas(proc):,}"))
        cells.append((f"{len(all_asgn):,}/{len(all_proc):,}", f"{_meas(all_asgn):,}/{_meas(all_proc):,}"))
        return cells

    rows = [
        ("All",         _build_row()),
        ("OMOP-mapped", _build_row(omop_names)),
    ]

    nw  = max(len(ns) for _, cells in rows for ns, _  in cells)
    mw  = max(len(ms) for _, cells in rows for _,  ms in cells)
    lw  = 12
    cw  = nw + 2 + mw
    cols = ["UNAMBIGUOUS", "AMBIGUOUS", "NO_DATA", "TOTAL"]

    text_lines = [
        "\nAssignment summary (successfully assigned)\n",
        f"{'':>{lw}}" + "".join(f"  {c:^{cw}}" for c in cols),
        f"{'':>{lw}}" + "".join(f"  {'names':>{nw}}  {'meas':>{mw}}" for _ in cols),
    ]
    div = f"{'':>{lw}}" + "".join(f"  {'-'*nw}  {'-'*mw}" for _ in cols)
    text_lines.append(div)
    for label, cells in rows:
        text_lines.append(f"{label:<{lw}}" + "".join(f"  {ns:>{nw}}  {ms:>{mw}}" for ns, ms in cells))
    text_lines.append(div)

    for line in text_lines:
        print(line)

    if out_doc:
        Path(out_doc).write_text("```\n" + "\n".join(text_lines) + "\n```\n")
        print(f"Wrote {out_doc}")

    if out_tsv:
        tsv_rows = []
        for label, cells in rows:
            row = {"subset": label}
            for col, (ns, ms) in zip(cols, cells):
                row[f"{col}_names"] = ns
                row[f"{col}_meas"]  = ms
            tsv_rows.append(row)
        pd.DataFrame(tsv_rows).to_csv(out_tsv, sep="\t", index=False)
        print(f"Wrote {out_tsv}")


def dump_summary_md(plot_name, threshold, min_count, out="summary_table.md", out_tsv=None):
    total   = len(plot_name)
    total_n = int(plot_name["COUNT"].sum())
    cats    = ["UNAMBIGUOUS", "AMBIGUOUS", "NO_DATA"]

    lines = []
    lines.append(f"*Active threshold: {threshold}% — min count: {min_count:,} — "
                 f"{total:,} TEST_NAMEs, {total_n:,} measurements*\n")

    header = "| Threshold | " + " | ".join(
        f"{c} test names | {c} measurements" for c in cats
    ) + " |"
    sep = "| --- | " + " | ".join(["---: | ---:" for _ in cats]) + " |"
    lines.append(header)
    lines.append(sep)

    tsv_rows = []
    for t in _SUMMARY_THRESHOLDS:
        marker = " \\*" if t == threshold else ""
        row = f"| {t}%{marker} |"
        tsv_row = {"threshold": f"{t}%"}
        for cat in cats:
            sub = plot_name[plot_name[f"CATEGORY_{t}"] == cat]
            n   = len(sub)
            m   = int(sub["COUNT"].sum())
            row += f" {n:,} ({100*n/total:.1f}%) | {m:,} ({100*m/total_n:.1f}%) |"
            tsv_row[f"{cat}_names"] = n
            tsv_row[f"{cat}_meas"]  = m
        lines.append(row)
        tsv_rows.append(tsv_row)

    lines.append(f"| **TOTAL** | **{total:,}** | | " + "| |" * (len(cats) - 1))

    Path(out).write_text("\n".join(lines) + "\n")
    print(f"Wrote {out}")

    if out_tsv:
        pd.DataFrame(tsv_rows).to_csv(out_tsv, sep="\t", index=False)
        print(f"Wrote {out_tsv}")


# ---------------------------------------------------------------------------
# Injection engine
# ---------------------------------------------------------------------------

def _query_test_values(parquet, name, mode, unit=None):
    if mode == "candidate":
        q = f"""
            SELECT coalesce(MEASUREMENT_VALUE_EXTRACTED, MEASUREMENT_VALUE_SOURCE) AS value
            FROM file('{parquet}')
            WHERE TEST_NAME = {{name:String}}
              AND MEASUREMENT_UNIT_PRE_FIX IS NULL
              AND isNotNull(coalesce(MEASUREMENT_VALUE_EXTRACTED, MEASUREMENT_VALUE_SOURCE))
            FORMAT TSV
        """
        out = clickhouse(q, name=name)
    else:
        q = f"""
            SELECT MEASUREMENT_VALUE_SOURCE AS value
            FROM file('{parquet}')
            WHERE TEST_NAME = {{name:String}}
              AND MEASUREMENT_UNIT_PRE_FIX = {{unit:String}}
              AND MEASUREMENT_VALUE_SOURCE IS NOT NULL
            FORMAT TSV
        """
        out = clickhouse(q, name=name, unit=unit)
    if not out.strip():
        return np.array([])
    vals = []
    for line in out.strip().split("\n"):
        try:
            vals.append(float(line.strip()))
        except ValueError:
            pass
    return np.sort(np.array(vals, dtype=float))


def _sample_deciles(df):
    """Return one row per COUNT decile (by rank), up to 10 rows."""
    df = df.reset_index(drop=True)
    n  = len(df)
    indices = sorted({int(round((i / 9) * (n - 1))) for i in range(10)})
    return df.iloc[indices]


_PCTS = [10, 20, 30, 40, 50, 60, 70, 80, 90]


def _fmt_deciles(arr):
    vals = np.percentile(arr, _PCTS)
    return "[" + ",".join(f"{v:.4g}" for v in vals) + "]"


def _run_engine(c_vals, t_vals, name, dump_dir, plots_dir=None, prevalence=None, tag_override=None):
    """Run KS / T / MAD pipeline, save diagnostic plot, return (updates, ks, t, mad)."""
    ks_step  = injection_engine._ks(c_vals, t_vals, ks_threshold=0.3, sig_level=0.05)
    t_step   = injection_engine._t(c_vals, t_vals, sig_level=0.05)
    mad_step = injection_engine._mad(c_vals, t_vals, n_mad=3.0)

    ks_mlogp = -np.log10(np.clip(ks_step.details["pval"], 1e-300, 1.0))
    t_mlogp  = -np.log10(np.clip(t_step.details["pval"],  1e-300, 1.0))

    if ks_step.passed:
        decided_by, outcome = "KS", "PASS"
    elif t_step.passed:
        decided_by, outcome = "T", "PASS"
    else:
        decided_by = "MAD"
        outcome    = "PASS" if mad_step.passed else "FAIL"

    result = injection_engine.PipelineResult(outcome, decided_by,
                                             [ks_step, t_step, mad_step])
    injection_engine.plot_result(c_vals, t_vals, result, name, plots_dir or dump_dir,
                                 prevalence=prevalence, tag_override=tag_override)
    plot_data = injection_engine.compute_plot_data(c_vals, t_vals, result,
                                                   prevalence=prevalence)

    n_passed = sum(s.passed for s in [ks_step, t_step, mad_step])
    updates = dict(
        KS_STAT=ks_step.details["stat"],  KS_MLOGP=ks_mlogp,
        KS_PASS="PASS" if ks_step.passed else "FAIL",
        T_STAT=t_step.details["stat"],    T_MLOGP=t_mlogp,
        T_PASS="PASS" if t_step.passed else "FAIL",
        MAD_DIST=mad_step.details["distance"],
        MAD_THRESHOLD=mad_step.details["threshold"],
        MAD_PASS="PASS" if mad_step.passed else "FAIL",
        OUTCOME=outcome,
        NOTES=f"{outcome}_at_{decided_by}_({n_passed}/3)",
        CAND_DECILES=_fmt_deciles(c_vals),
        TARG_DECILES=_fmt_deciles(t_vals),
    )
    return updates, ks_step, t_step, mad_step, plot_data


def run_unambiguous(parquet, plot_name, details, dump_dir, plots_dir=None, test_mode=False,
                    out="unambiguous_results.tsv"):
    plot_data = {}
    os.makedirs(dump_dir, exist_ok=True)

    unambig = plot_name[plot_name["CATEGORY"] == "UNAMBIGUOUS"].sort_values("COUNT", ascending=False)
    if test_mode:
        unambig = _sample_deciles(unambig)
    test_names = unambig["TEST_NAME"].tolist()
    print(f"Unambiguous TEST_NAMEs: {len(test_names)}")

    dom_unit  = dict(zip(details["TEST_NAME"], details["UNIT"]))
    prev_dict = dict(zip(details["TEST_NAME"], details["PREVALENCE_DICT"]))

    rows = []
    n_pass = n_fail = n_skip = 0

    for i, name in enumerate(test_names, 1):
        print(f"  [{i:>4}/{len(test_names)}] {name:<40}", end="  ", flush=True)

        tag      = name.replace("/", "_").replace(" ", "_")
        cand_npy = os.path.join(dump_dir, f"cand_{tag}.npy")
        targ_npy = os.path.join(dump_dir, f"targ_{tag}.npy")

        unit = dom_unit.get(name, "NA")
        prev = prev_dict.get(name, "NA")

        if os.path.exists(cand_npy):
            c_vals = np.load(cand_npy)
            print("cand=cache", end="  ", flush=True)
        else:
            print("cand=query", end="  ", flush=True)
            c_vals = _query_test_values(parquet, name, "candidate")
            np.save(cand_npy, c_vals)

        if os.path.exists(targ_npy):
            t_vals = np.load(targ_npy)
            print(f"targ=cache({unit})", end="  ", flush=True)
        else:
            print(f"targ=query({unit})", end="  ", flush=True)
            t_vals = _query_test_values(parquet, name, "target", unit=unit)
            np.save(targ_npy, t_vals)

        print(f"N={len(c_vals):,}/{len(t_vals):,}", end="  ", flush=True)

        row = dict(TEST_NAME=name, UNIT=unit, PREVALENCE_DICT=prev,
                   N_CANDIDATE=len(c_vals), N_TARGET=len(t_vals),
                   KS_STAT=np.nan, KS_MLOGP=np.nan, KS_PASS="NA",
                   T_STAT=np.nan,  T_MLOGP=np.nan,  T_PASS="NA",
                   MAD_DIST=np.nan, MAD_THRESHOLD=np.nan, MAD_PASS="NA",
                   OUTCOME="SKIP", NOTES="SKIP(NO_DATA)")

        if len(c_vals) >= 2 and len(t_vals) >= 2:
            print("engine...", end="  ", flush=True)
            updates, ks, t, mad, pd_ = _run_engine(c_vals, t_vals, name, dump_dir,
                                                    plots_dir=plots_dir, prevalence=prev)
            row.update(updates)
            plot_data[name] = pd_
            print(
                f"KS={'P' if ks.passed else 'F'}"
                f"(stat={ks.details['stat']:.3g},mlogp={updates['KS_MLOGP']:.1f},{ks.details['method']})"
                f"  T={'P' if t.passed else 'F'}(mlogp={updates['T_MLOGP']:.1f})"
                f"  MAD={'P' if mad.passed else 'F'}"
                f"(dist={mad.details['distance']:.3f},thr={mad.details['threshold']:.3f})",
                end="  ", flush=True,
            )

        if row["OUTCOME"] == "PASS":   n_pass += 1
        elif row["OUTCOME"] == "FAIL": n_fail += 1
        else:                          n_skip += 1

        rows.append(row)
        print(f"{row['OUTCOME']}  ({row['NOTES']})  "
              f"[running: {n_pass}P/{n_fail}F/{n_skip}S]  {prev}")

    cols = ["TEST_NAME", "UNIT", "PREVALENCE_DICT", "N_CANDIDATE", "N_TARGET",
            "CAND_DECILES", "TARG_DECILES",
            "KS_STAT", "KS_MLOGP", "KS_PASS",
            "T_STAT",  "T_MLOGP",  "T_PASS",
            "MAD_DIST", "MAD_THRESHOLD", "MAD_PASS",
            "OUTCOME", "NOTES"]
    results = pd.DataFrame(rows, columns=cols)
    for col in ("TEST_NAME", "UNIT", "OUTCOME", "NOTES"):
        results[col] = results[col].str.replace(" ", "_", regex=False)
    results.to_csv(out, sep="\t", index=False, float_format="%.4g", na_rep="NA")
    print(f"\nWrote {out}  ({len(results)} rows)")
    return results, plot_data


# ---------------------------------------------------------------------------
# Ambiguous injection
# ---------------------------------------------------------------------------

def parse_top_units(prevalence_dict, n=3):
    """Parse '{unit1:pct1,unit2:pct2,...}' → list of (unit, pct) sorted by pct desc."""
    if pd.isna(prevalence_dict) or str(prevalence_dict) in ("", "NA", "nan"):
        return []
    matches = re.findall(r'([^:{},\s][^:{},]*):(\d+\.?\d*)', str(prevalence_dict))
    pairs = [(u.strip(), float(p)) for u, p in matches if u.strip()]
    pairs.sort(key=lambda x: x[1], reverse=True)
    return pairs[:n]


def _unit_tag(unit):
    return unit.replace("/", "_").replace(" ", "_").replace("%", "pct")


def run_ambiguous(parquet, plot_name, details, dump_dir, plots_dir=None,
                  dip_threshold=0.05, min_target_n=30, test_mode=False,
                  split_threshold=0.15, out="ambiguous_results.tsv"):
    os.makedirs(dump_dir, exist_ok=True)
    plot_data = {}

    ambig = plot_name[plot_name["CATEGORY"] == "AMBIGUOUS"].sort_values("COUNT", ascending=False)
    if test_mode:
        ambig = ambig.head(10)
    n_names = len(ambig)
    print(f"\nAmbiguous TEST_NAMEs: {n_names}")

    prev_dict = dict(zip(details["TEST_NAME"], details["PREVALENCE_DICT"]))

    rows = []

    for i, name in enumerate(ambig["TEST_NAME"].tolist(), 1):
        tag      = name.replace("/", "_").replace(" ", "_")
        cand_npy = os.path.join(dump_dir, f"cand_{tag}.npy")

        print(f"\n[{i:>4}/{n_names}] {name}")

        # Candidate values (reuse cache from unambiguous run if present)
        if os.path.exists(cand_npy):
            c_vals = np.load(cand_npy)
            print(f"  cand=cache  N={len(c_vals):,}")
        else:
            c_vals = _query_test_values(parquet, name, "candidate")
            np.save(cand_npy, c_vals)
            print(f"  cand=query  N={len(c_vals):,}")

        if len(c_vals) < 2:
            print("  SKIP: too few candidate values")
            continue

        # Filter to units with prevalence > 1% (top 3)
        top_units = [(u, p) for u, p in parse_top_units(prev_dict.get(name, ""), n=3) if p > 1.0]
        if not top_units:
            print("  SKIP: no units with prevalence > 1%")
            continue

        # Load & cache target arrays once (applies min_target_n guard)
        unit_data = {}   # unit -> t_vals
        for unit, upct in top_units:
            utag     = _unit_tag(unit)
            targ_npy = os.path.join(dump_dir, f"targ_{tag}_{utag}.npy")
            if os.path.exists(targ_npy):
                t_vals_u = np.load(targ_npy)
            else:
                t_vals_u = _query_test_values(parquet, name, "target", unit=unit)
                np.save(targ_npy, t_vals_u)
            if len(t_vals_u) < min_target_n:
                print(f"  SKIP unit {unit}({upct:.1f}%): N_TARGET={len(t_vals_u)}<{min_target_n}")
            else:
                unit_data[unit] = t_vals_u

        if not unit_data:
            print("  SKIP: no units with sufficient reference data")
            continue

        # Pre-check: run engine on full candidate vs each qualifying unit.
        # If any unit already passes, record those results and skip bimodality.
        print(f"  pre-check (full candidate)  units={list(unit_data)}")
        precheck = []   # (unit, upct, t_vals, updates, ks_step, t_step, mad_step)
        for unit, upct in [(u, p) for u, p in top_units if u in unit_data]:
            t_vals_u = unit_data[unit]
            upd, ks, t, mad, pd_ = _run_engine(
                c_vals, t_vals_u,
                f"{name} [all] [{unit}]", dump_dir, plots_dir=plots_dir,
                prevalence=f"{upct:.1f}%",
                tag_override=f"{tag}_all_{_unit_tag(unit)}",
            )
            plot_data.setdefault(name, {}).setdefault("engine", {})[f"all_{unit}"] = pd_
            precheck.append((unit, upct, t_vals_u, upd, ks, t, mad))
            print(f"    [all][{unit}({upct:.1f}%)]"
                  f"  N={len(c_vals):,}/{len(t_vals_u):,}"
                  f"  KS={'P' if ks.passed else 'F'}(stat={ks.details['stat']:.3g})"
                  f"  T={'P' if t.passed else 'F'}"
                  f"  MAD={'P' if mad.passed else 'F'}"
                  f"  → {upd['OUTCOME']}")

        any_precheck_pass = any(upd["OUTCOME"] == "PASS" for _, _, _, upd, *_ in precheck)

        # Bimodal check always runs — needed for split score even when pre-check passed
        bim = injection_engine.bimodal_check(c_vals, dip_threshold=dip_threshold)
        injection_engine.plot_bimodal_check(bim, name, plots_dir or dump_dir)
        plot_data.setdefault(name, {})["bimodal"] = injection_engine.compute_bimodal_plot_data(bim)
        print(f"  bimodal={bim.status}  sep={bim.separator:.4g}"
              f"  BC={bim.bc:.3f}  dip_p={bim.dip_p:.3g}")

        # Evaluate whether splitting improves the fit
        prefer_split = False
        si    = {}
        sep   = bim.separator
        c_low  = c_vals[c_vals <= sep] if not np.isnan(sep) else np.array([])
        c_high = c_vals[c_vals >  sep] if not np.isnan(sep) else np.array([])
        if len(c_low) >= 2 and len(c_high) >= 2:
            si = injection_engine.split_improvement(c_vals, c_low, c_high, unit_data)
            print(f"  split_improvement={si['improvement']:+.1%}"
                  f"  same_unit={si['same_best_unit']}"
                  f"  (global_KS={si['global_score']:.4f}"
                  f"  split_KS={si['split_score']:.4f})")
            if si["improvement"] > split_threshold and not si["same_best_unit"]:
                prefer_split = True
                print(f"  → SPLIT preferred (threshold={split_threshold:.0%})")

        # Decision-tree figure
        unit_data_fig = {unit: (unit_data[unit], upct)
                         for unit, upct in top_units if unit in unit_data}
        global_ranks  = split_eval.rank_units(c_vals, unit_data_fig)
        low_ranks     = split_eval.rank_units(c_low,  unit_data_fig) if len(c_low)  >= 2 else []
        high_ranks    = split_eval.rank_units(c_high, unit_data_fig) if len(c_high) >= 2 else []
        split_eval.make_figure(
            name=name, c_vals=c_vals, unit_data=unit_data_fig,
            global_ranks=global_ranks, low_ranks=low_ranks, high_ranks=high_ranks,
            c_low=c_low, c_high=c_high, sep=sep, bim=bim,
            g_score=global_ranks[0][2] if global_ranks else np.nan,
            s_score=si.get("split_score", np.nan),
            improvement=si.get("improvement", 0.0),
            out_path=os.path.join(plots_dir or dump_dir, f"split_eval_{tag}.png"),
        )

        if any_precheck_pass and not prefer_split:
            print(f"  pre-check passed → global result kept")
            for unit, upct, t_vals_u, upd, ks, t, mad in precheck:
                rows.append(dict(
                    TEST_NAME=name,
                    BIMODAL_STATUS="skipped",
                    BIMODAL_SEP=np.nan, BIMODAL_BC=np.nan, BIMODAL_DIP_P=np.nan,
                    SCORE_GLOBAL=si.get("global_score", np.nan),
                    SCORE_SPLIT=si.get("split_score", np.nan),
                    SCORE_IMPROVEMENT=si.get("improvement", np.nan),
                    SUB_DIST="all",
                    UNIT=unit, UNIT_PREVALENCE=upct,
                    PREVALENCE_DICT=prev_dict.get(name, "NA"),
                    N_CANDIDATE=len(c_vals), N_TARGET=len(t_vals_u),
                    **upd,
                ))
            continue

        # Decide whether to split: bimodal detected, or split preferred by score
        do_split      = prefer_split or bim.status in ("bimodal", "bimodal_cautious")
        bimodal_label = "split_by_score" if prefer_split else bim.status

        if not do_split:
            # Unimodal, pre-check failed, split not preferred — reuse pre-check results
            for unit, upct, t_vals_u, upd, ks, t, mad in precheck:
                rows.append(dict(
                    TEST_NAME=name,
                    BIMODAL_STATUS=bim.status,
                    BIMODAL_SEP=bim.separator, BIMODAL_BC=bim.bc, BIMODAL_DIP_P=bim.dip_p,
                    SCORE_GLOBAL=si.get("global_score", np.nan),
                    SCORE_SPLIT=si.get("split_score", np.nan),
                    SCORE_IMPROVEMENT=si.get("improvement", np.nan),
                    SUB_DIST="all",
                    UNIT=unit, UNIT_PREVALENCE=upct,
                    PREVALENCE_DICT=prev_dict.get(name, "NA"),
                    N_CANDIDATE=len(c_vals), N_TARGET=len(t_vals_u),
                    **upd,
                ))
        else:
            sep  = bim.separator
            low  = c_vals[c_vals <= sep]
            high = c_vals[c_vals >  sep]
            sub_dists = []
            if len(low)  >= 2: sub_dists.append(("low",  low))
            if len(high) >= 2: sub_dists.append(("high", high))
            if not sub_dists:
                sub_dists = [("all", c_vals)]

            print(f"  sub_dists={[s for s, _ in sub_dists]}")
            for sub_name, c_sub in sub_dists:
                for unit, upct in [(u, p) for u, p in top_units if u in unit_data]:
                    t_vals_u = unit_data[unit]
                    upd, ks, t, mad, pd_ = _run_engine(
                        c_sub, t_vals_u,
                        f"{name} [{sub_name}] [{unit}]", dump_dir, plots_dir=plots_dir,
                        prevalence=f"{upct:.1f}%",
                        tag_override=f"{tag}_{sub_name}_{_unit_tag(unit)}",
                    )
                    plot_data.setdefault(name, {}).setdefault("engine", {})[f"{sub_name}_{unit}"] = pd_
                    rows.append(dict(
                        TEST_NAME=name,
                        BIMODAL_STATUS=bimodal_label,
                        BIMODAL_SEP=bim.separator, BIMODAL_BC=bim.bc, BIMODAL_DIP_P=bim.dip_p,
                        SCORE_GLOBAL=si.get("global_score", np.nan),
                        SCORE_SPLIT=si.get("split_score", np.nan),
                        SCORE_IMPROVEMENT=si.get("improvement", np.nan),
                        SUB_DIST=sub_name,
                        UNIT=unit, UNIT_PREVALENCE=upct,
                        PREVALENCE_DICT=prev_dict.get(name, "NA"),
                        N_CANDIDATE=len(c_sub), N_TARGET=len(t_vals_u),
                        **upd,
                    ))
                    print(f"    [{sub_name}][{unit}({upct:.1f}%)]"
                          f"  N={len(c_sub):,}/{len(t_vals_u):,}"
                          f"  KS={'P' if ks.passed else 'F'}(stat={ks.details['stat']:.3g})"
                          f"  T={'P' if t.passed else 'F'}"
                          f"  MAD={'P' if mad.passed else 'F'}"
                          f"  → {upd['OUTCOME']}")

    if not rows:
        print("No ambiguous results to write.")
        return None

    results = pd.DataFrame(rows)

    # Best unit per (TEST_NAME, SUB_DIST): PASS > FAIL > SKIP, then highest prevalence
    def _outcome_rank(o):
        return {"PASS": 0, "FAIL": 1, "SKIP": 2}.get(o, 2)

    results["_rank"] = results["OUTCOME"].map(_outcome_rank)
    best = (results
            .sort_values(["_rank", "UNIT_PREVALENCE"], ascending=[True, False])
            .groupby(["TEST_NAME", "SUB_DIST"])["UNIT"]
            .first()
            .reset_index())
    results = results.merge(best.rename(columns={"UNIT": "_best_unit"}),
                            on=["TEST_NAME", "SUB_DIST"], how="left")
    results = results[results["UNIT"] == results["_best_unit"]].drop(columns=["_rank", "_best_unit"])

    col_order = [
        "TEST_NAME", "BIMODAL_STATUS", "BIMODAL_SEP", "BIMODAL_BC", "BIMODAL_DIP_P",
        "SCORE_GLOBAL", "SCORE_SPLIT", "SCORE_IMPROVEMENT",
        "SUB_DIST",
        "UNIT", "UNIT_PREVALENCE", "PREVALENCE_DICT",
        "N_CANDIDATE", "N_TARGET",
        "CAND_DECILES", "TARG_DECILES",
        "KS_STAT", "KS_MLOGP", "KS_PASS",
        "T_STAT",  "T_MLOGP",  "T_PASS",
        "MAD_DIST", "MAD_THRESHOLD", "MAD_PASS",
        "OUTCOME", "NOTES",
    ]
    results = results[col_order]
    results.to_csv(out, sep="\t", index=False, float_format="%.4g", na_rep="NA")

    _print_ambig_summary(results, out)
    return results, plot_data


def _print_ambig_summary(results, out="ambiguous_results.tsv"):
    n_tot  = results["TEST_NAME"].nunique()
    tested = results[results["OUTCOME"] != "SKIP"]

    best_rows = tested.copy()

    n_pass = (best_rows["OUTCOME"] == "PASS").sum()
    n_fail = (best_rows["OUTCOME"] == "FAIL").sum()
    n_skip = n_tot - best_rows["TEST_NAME"].nunique()

    print(f"\n{'=' * 72}")
    print("AMBIGUOUS INJECTION SUMMARY")
    print(f"{'=' * 72}")
    print(f"  TEST_NAMEs: {n_tot}  "
          f"PASS={n_pass}  FAIL={n_fail}  SKIP={n_skip}")
    print()

    w = 40
    print(f"  {'TEST_NAME':<{w}}  {'BIMODAL':<18}  {'SUB':<5}  {'UNIT':<14}  OUTCOME")
    print(f"  {'-'*w}  {'-'*18}  {'-'*5}  {'-'*14}  -------")
    for _, r in best_rows.iterrows():
        print(f"  {r['TEST_NAME']:<{w}}  {r['BIMODAL_STATUS']:<18}  {r['SUB_DIST']:<5}"
              f"  {r['UNIT']:<14}  {r['OUTCOME']}")

    print(f"\nWrote {out}  ({len(results)} rows, {n_tot} TEST_NAMEs)")


# ---------------------------------------------------------------------------
# Unified output
# ---------------------------------------------------------------------------

_UNIFIED_COLS = [
    "TYPE",
    "TEST_NAME", "BIMODAL_STATUS", "BIMODAL_SEP", "BIMODAL_BC", "BIMODAL_DIP_P",
    "SCORE_GLOBAL", "SCORE_SPLIT", "SCORE_IMPROVEMENT",
    "SUB_DIST", "CUTOFF", "UNIT", "UNIT_PREVALENCE", "PREVALENCE_DICT",
    "N_CANDIDATE", "N_TARGET",
    "CAND_DECILES", "TARG_DECILES",
    "KS_STAT", "KS_MLOGP", "KS_PASS",
    "T_STAT",  "T_MLOGP",  "T_PASS",
    "MAD_DIST", "MAD_THRESHOLD", "MAD_PASS",
    "OUTCOME", "NOTES",
]


def _write_unified(udf, adf, out="injection_results.tsv"):
    parts = []

    if udf is not None and len(udf):
        u = udf.copy()
        u["TYPE"]           = "unambiguous"
        u["SUB_DIST"]       = "all"
        # Derive UNIT_PREVALENCE from the dominant unit in PREVALENCE_DICT
        def _top_pct(row):
            pairs = parse_top_units(row["PREVALENCE_DICT"], n=10)
            for unit, pct in pairs:
                if unit == row["UNIT"]:
                    return pct
            return np.nan
        u["UNIT_PREVALENCE"] = u.apply(_top_pct, axis=1)
        parts.append(u)

    if adf is not None and len(adf):
        a = adf.copy()
        a["TYPE"] = "ambiguous"
        parts.append(a)

    if not parts:
        return

    combined = pd.concat(parts, ignore_index=True)
    combined["CUTOFF"] = np.where(
        combined["SUB_DIST"] == "low",
        combined["BIMODAL_SEP"],
        np.inf,
    )
    combined = combined.reindex(columns=_UNIFIED_COLS)
    combined.to_csv(out, sep="\t", index=False, float_format="%.4g", na_rep="NA")
    print(f"\nWrote {out}  ({len(combined)} rows)")


# ---------------------------------------------------------------------------
# Coverage check
# ---------------------------------------------------------------------------

def _check_coverage(plot_name, udf, adf, no_data):
    expected = set(plot_name["TEST_NAME"])

    covered = set()
    for label, df in [("unambiguous", udf), ("ambiguous", adf), ("no_data", no_data)]:
        if df is not None and len(df):
            names = set(df["TEST_NAME"])
            overlap = covered & names
            if overlap:
                print(f"  COVERAGE ERROR: {len(overlap)} TEST_NAME(s) appear in multiple files: "
                      f"{sorted(overlap)[:5]}{'...' if len(overlap) > 5 else ''}")
            covered |= names

    missing  = expected - covered
    extra    = covered - expected

    print(f"\nCoverage check: {len(expected)} expected  "
          f"{len(covered)} covered  "
          f"{len(missing)} missing  "
          f"{len(extra)} extra")

    if missing:
        print(f"  MISSING: {sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}")
    if extra:
        print(f"  EXTRA:   {sorted(extra)[:10]}{'...' if len(extra) > 10 else ''}")
    if not missing and not extra:
        print("  OK: all TEST_NAMEs accounted for across 3 output files")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="Explore unit injection targets in Kanta lab data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("parquet",   help="Input parquet file (INJECT.parquet)")
    p.add_argument("--min-count", type=int, default=1000, metavar="INT",
                   help="Minimum no-unit records per TEST_NAME to include")
    p.add_argument("--prevalence-threshold", type=float, default=98, metavar="FLOAT",
                   help="Min top-unit prevalence (%%) for a TEST_NAME to be unambiguous")
    p.add_argument("--dump-dir", default="/mnt/disks/data/kanta/inject/tmp/", metavar="PATH",
                   help="Cache directory for per-test .npy arrays")
    p.add_argument("--inject", action="store_true",
                   help="Run the injection engine on all tests (unambiguous + ambiguous)")
    p.add_argument("--dip-threshold", type=float, default=0.05, metavar="FLOAT",
                   help="Hartigan dip test p-value threshold for bimodality detection")
    p.add_argument("--split-threshold", type=float, default=0.15, metavar="FLOAT",
                   help="Minimum relative KS improvement to prefer a split over the global fit")
    p.add_argument("--omop-unit-table", default=None, metavar="PATH",
                   help="Override path for the OMOP unit table cache (default: data/omop_unit_table.tsv under --out-dir)")
    p.add_argument("--min-target-n", type=int, default=30, metavar="INT",
                   help="Minimum reference records a unit must have to be tested (ambiguous pass)")
    p.add_argument("--test", action="store_true",
                   help="Limit to a small sample: one per COUNT decile (unambiguous) "
                        "and top 10 by volume (ambiguous)")
    p.add_argument("--out-dir", default=".", metavar="PATH",
                   help="Root output directory; doc/, plots/, data/ subdirs are created here")
    return p


def main():
    args = build_parser().parse_args()

    data_dir  = Path(args.out_dir)
    doc_dir   = data_dir / "doc"
    plots_dir = data_dir / "plots"
    for d in (data_dir, doc_dir, plots_dir):
        d.mkdir(parents=True, exist_ok=True)

    counts  = query_counts(args.parquet, out=str(data_dir / "test_name_counts.tsv"))
    counts  = counts[counts["COUNT"] > args.min_count].reset_index(drop=True)
    details = query_details(args.parquet,
                            counts_file=str(data_dir / "test_name_counts.tsv"),
                            out=str(data_dir / "test_name_details.tsv"))

    plot_name = build_plot_table(counts, details)
    plot_name = _add_category(plot_name, args.prevalence_threshold, args.min_target_n)
    plot_name.to_csv(data_dir / "plot_name_level.tsv", sep="\t", index=False)
    print(f"Built {data_dir}/plot_name_level.tsv  ({len(plot_name)} rows, "
          f"min_count={args.min_count}, threshold={args.prevalence_threshold}%)")

    omop_tbl = load_omop_unit_table(args.parquet,
                                    out=args.omop_unit_table or str(data_dir / "omop_unit_table.tsv"))
    omop_cat = omop_tbl.set_index("OMOP_CONCEPT_ID")["CATEGORY"]

    make_scatter_plot(plot_name, output=str(plots_dir / "test_names_exploration_scatter.png"))
    print_summary(plot_name, args.prevalence_threshold)
    dump_summary_md(plot_name, args.prevalence_threshold, args.min_count,
                    out=str(doc_dir / "summary_table.md"),
                    out_tsv=str(data_dir / "summary_table.tsv"))

    omop_sub = plot_name[plot_name["OMOP_CONCEPT_ID"].notna()].copy()
    print(f"\n── OMOP-mapped subset ({len(omop_sub):,} TEST_NAMEs) ──")
    print_summary(omop_sub, args.prevalence_threshold)
    dump_summary_md(omop_sub, args.prevalence_threshold, args.min_count,
                    out=str(doc_dir / "omop_summary_table.md"),
                    out_tsv=str(data_dir / "omop_summary_table.tsv"))

    if args.inject:
        udf, udf_plots = run_unambiguous(args.parquet, plot_name, details, args.dump_dir,
                                         plots_dir=str(plots_dir),
                                         test_mode=args.test,
                                         out=str(data_dir / "unambiguous_results.tsv"))
        adf, adf_plots = run_ambiguous(args.parquet, plot_name, details, args.dump_dir,
                                       plots_dir=str(plots_dir),
                                       dip_threshold=args.dip_threshold,
                                       min_target_n=args.min_target_n, test_mode=args.test,
                                       split_threshold=args.split_threshold,
                                       out=str(data_dir / "ambiguous_results.tsv"))
        _write_unified(udf, adf, out=str(data_dir / "injection_results.tsv"))

        all_plot_data = {**udf_plots, **adf_plots}
        plot_data_path = data_dir / "plot_data.json"
        class _Enc(json.JSONEncoder):
            def default(self, o):
                if isinstance(o, (np.bool_,)):   return bool(o)
                if isinstance(o, np.integer):    return int(o)
                if isinstance(o, np.floating):   return float(o)
                return super().default(o)
        plot_data_path.write_text(json.dumps(all_plot_data, cls=_Enc), encoding="utf-8")
        print(f"Wrote {plot_data_path}  ({len(all_plot_data)} TEST_NAMEs)")

        no_data = plot_name[plot_name["CATEGORY"] == "NO_DATA"][["TEST_NAME", "COUNT", "OMOP_CONCEPT_ID"]].copy()

        no_data = no_data.merge(
            omop_tbl[["OMOP_CONCEPT_ID", "OMOP_QUANTITY", "CATEGORY", "N_UNITS", "UNITS",
                       "CONVERSIONS", "CANONICAL_UNIT", "OMOP_TOTAL_N"]],
            on="OMOP_CONCEPT_ID", how="left",
        )
        no_data["UNIT"]        = no_data["CANONICAL_UNIT"].where(
                                     no_data["CATEGORY"].isin(["SINGLE", "EQUIVALENT"])
                                 )
        no_data["COUNT"]       = no_data["COUNT"].astype("Int64")
        no_data["OMOP_TOTAL_N"] = no_data["OMOP_TOTAL_N"].astype("Int64")
        no_data["PREVALENCE"]  = (no_data["COUNT"] / no_data["OMOP_TOTAL_N"]).round(4)
        no_data = no_data.drop(columns=["CANONICAL_UNIT"])

        no_data.to_csv(data_dir / "no_data_results.tsv", sep="\t", index=False, na_rep="NA")
        n_injected = no_data["UNIT"].notna().sum()
        print(f"Wrote {data_dir}/no_data_results.tsv  ({len(no_data)} rows, {n_injected} units injected via OMOP)")

        has_omop   = no_data["OMOP_CONCEPT_ID"].notna()
        has_unit   = no_data["UNIT"].notna()
        n_total    = len(no_data)
        summary_rows = [
            ("no OMOP",             (~has_omop),             ),
            ("OMOP, unit injected", ( has_omop &  has_unit), ),
            ("OMOP, no unit",       ( has_omop & ~has_unit), ),
        ]
        w = max(len(r[0]) for r in summary_rows)
        print(f"\n  {'':>{w}}   n      %   prev (median)")
        print(f"  {'-'*w}  ----  -----  -------------")
        for label, mask in summary_rows:
            n       = mask.sum()
            pct     = 100 * n / n_total
            prev_s  = no_data.loc[mask, "PREVALENCE"].dropna()
            prev_str = f"{prev_s.median():.1%}" if len(prev_s) else "NA"
            print(f"  {label:>{w}}  {n:>4}  {pct:>5.1f}%  {prev_str:>13}")
        print(f"  {'-'*w}  ----")
        print(f"  {'total':>{w}}  {n_total:>4}")

        if not args.test:
            _check_coverage(plot_name, udf, adf, no_data)

        print_assignment_summary(udf, adf, no_data, plot_name,
                                 out_doc=str(doc_dir / "assignment_summary.md"),
                                 out_tsv=str(data_dir / "assignment_summary.tsv"))


if __name__ == "__main__":
    main()
