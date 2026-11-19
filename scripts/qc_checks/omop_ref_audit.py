#!/usr/bin/env python3
"""
omop_ref_audit.py

Audit OMOP concepts with the reference ranges: which rows went wrong in injection/harmonization, how, where in the
pipeline, and what their unit should have been.

Idea: every row carries three independent witnesses of its scale -- its value v, its reference range (lo-hi), and its
declared unit -- plus, once harmonized, the factor harmonization actually applied, f = HARM_VALUE / MEASUREMENT_VALUE.
The range is on the scale of the source value even when the declared unit is wrong (the same assumption as
ref_ranges/ref_units.py). Harmonizing the range with the row's own f puts every test of the concept on one axis, so one
canonical range per concept can be learned and every row's range compared to it -- across tests, not per test x OMOP.

Per row, three log10 offsets (unit-free):
  1 range vs canonical   log10(f x range / canonical range)          ~0 when the unit used for harmonization is right
  2 value vs own range   log10(v / range) - the concept's typical     ~0 when value and range are on the same scale
  3 value vs concept     log10(HARM_VALUE / concept median)           ~0 when the harmonized value is on scale
"range" is the upper limit when the row has one (> 0), else the lower one: upper limits are the most stable part of a
range across labs (<10 and 0-10 agree, a geometric mid would not). Checks 2 and 3 pass within
max(log10(--shift), --far robust SDs); check 1 within log10(--range-margin) (ranges are tight).

Verdict (rows with a value > 0; 'A/B/C' = checks 2/1/3 pass):
  ok                       A B C
  value extreme            range agrees, value far off but not by a unit factor (e.g. severe anaemia): not an error
  wrong unit               A, not B, not C: value and range agree with each other, not with the unit -> suggested
                           unit, from the factor that would put the range on the canonical one, matched against the
                           conversion table (e.g. b-hkr declared % with 0.41 and 0.35-0.46)
  range off scale          not A, not B, C: the value is fine, the range is on another scale (41 % with 0.35-0.46)
  value mis-scaled         not A, B, not C, off by a unit factor (x10^k or a conversion ratio): the unit and range agree,
                           the value doesn't
  inconsistent             any other pattern
  ok (no range)            no range, value on scale
  value off (no range)     no range, value off scale; when off by a unit factor, suggested unit from the value alone:
                           the unit whose factor brings it to the concept median (u-krea 7.24 umol/l -> mmol/l)
  unharmonized: fixable    no harmonized value; value and range agree, and their range points to exactly one unit of
                           the conversion table whose factor also brings the value on scale -> suggested unit
  unharmonized: conflict   no harmonized value; value and range disagree, or the suggested factor doesn't bring the
                           value on scale;  unharmonized: no unit for factor / no range: as named (no range: unit
                           suggested from the value alone, as for value off)
  no value > 0             a value of 0 or below (raw or harmonized), or a harmonized value without a raw one: not
                           checked, listed so that the groups add up to the rows
Canonical range: weighted median of the harmonized limits over rows passing A and C, per sex x age group (F/M x minor
<18 / adult) with >= --min-stratum-rows rows, else pooled over every row.

Stage, from cleaned-pre-inj::MEASUREMENT_UNIT (source) vs MEASUREMENT_UNIT (post-injection):
  lab        source unit kept         -> the error, if any, came from the lab
  injected   source NA, unit injected -> injection chose it
  rewritten  source X -> Y            -> injection changed it (also tells when the suggested unit is the source one:
                                         injection broke a right unit)
  no unit    no unit after injection;  unknown: the input has no pre-injection unit column (a release)

Usage
-----
  python3 omop_ref_audit.py --omop ID[,ID ...] | --all [--min-rows N]  [--input PARQUET] [--out-dir DIR] [--cache DIR]
  python3 omop_ref_audit.py ID                                          (one OMOP, as before)

Three steps per OMOP, each cached under --cache/audit/omop=<id>/ and redone only when its input changes:
  rows     the OMOP's rows, from sp_qc.py's row cache (--cache/rows_<input name>/omop=<id>/: extracted from --input
           in batches of --extract-batch OMOPs per pass, shared with sp_qc.py; --input defaults to the most recent run in
           /mnt/disks/data/kanta/engine)
  groups   groups.parquet: test x source unit x unit x harmonized unit x sex x age x range x 0.01-log10 value bin x
           factor, with rows and a people sketch (the 5 smallest person hashes); rebuilt when the rows change
  tables   the analysis and every table the outputs show, already gated (*.parquet + tables.json); rebuilt when the
           groups or the parameters change. The figure is drawn from these tables only (--plots-only redraws it).
Writes into --out-dir:
  omop_<id>_ref_audit.png / .pdf     the report (the PDF in vectors): the three offsets per test, a decision map (value
                                     vs range offset), verdict shares per test, flagged rows per stage, the flagged groups
  omop_<id>_ref_audit_groups.tsv     test x source unit -> unit -> harmonized unit x stage x verdict x suggested unit: rows,
                                     median range (source scale), median value before (value) and after harmonization
                                     (value_h), median offsets as factors
  omop_<id>_ref_audit_verdicts.tsv   rows and % per verdict (the shown groups)
  omop_<id>_ref_audit_canonical.tsv  the canonical range per stratum
  omop_<id>_ref_audit_tests.tsv      per test name: KS D of its harmonized values vs the rest of the concept (differs
                                     at D >= --ks-d), % of its rows flagged, harmonized Q1 / median / Q3: a high D with
                                     few flagged rows is another analyte or specimen on a similar scale (the scale
                                     checks miss it)
and prints the flagged groups as Markdown.

Privacy (as sp_qc.py; a hard condition): every published number covers >= --min-n (5) people, not just rows.
  - A group is shown when its rows cover >= 5 people (merged sketches), else BELOW_THRESHOLD with no statistics. Within
    each test x unit x harmonized unit (the composition the QC portal shows), the hidden groups together are none or
    >= 5 people (secondary suppression), so no shown total minus the shown groups gives a small one away.
  - Every other count (verdicts, stages, tests, canonical strata) sums shown groups only; % rows are shares of the
    shown rows. A test's KS D and quantiles need >= 5 people in the test and in the rest, and Q1 / Q3 >= 5 people
    beyond them.
  - Figures: offset histogram bins are pooled with their neighbours until each covers >= 5 people (no row left out);
    decision-map points under 5 people are left out; the bars sum shown groups.
  - Each TSV is checked once more before it is written (sp_qc.publish_tsv). The cache holds hashed person ids.
"""
import argparse
import json
import math
import os
import sys
import time
import traceback
from pathlib import Path

import duckdb
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sp_qc as S  # noqa: E402  (row cache, people sketches, publish_tsv)

try:
    from kanta import config
    DATA_DIR = Path(config.DATA_DIR)
except ImportError:  # outside the kanta env: the repo's data folder
    DATA_DIR = Path(__file__).resolve().parents[2] / "src" / "kanta" / "engine" / "data"

BIN = 0.01  # log10 width of the value bins in the cached groups
HBIN = 0.05  # log10 width of the offset histograms
XLIM = 3.5  # offsets drawn within +- this (log10); beyond, piled at the edge
CACHE_VERSION = "g2"  # groups: from the shared row cache, with people sketches
TABLES_VERSION = "t2"
BELOW = S.BELOW
NO_VALUE = "no value > 0"

VERDICTS = [  # (name, color, flagged) -- order = legend / stacking order
    ("ok", "#0ca30c", False),
    ("ok (no range)", "#7fc97f", False),
    ("value extreme", "#c3c2b7", False),
    ("wrong unit", "#d03b3b", True),
    ("value mis-scaled", "#ec835a", True),
    ("range off scale", "#fab219", True),
    ("inconsistent", "#6250d6", True),
    ("value off (no range)", "#e87ba4", True),
    ("unharmonized: fixable", "#2a78d6", True),
    ("unharmonized: conflict", "#184f95", True),
    ("unharmonized: no unit for factor", "#86b6ef", True),
    ("unharmonized: no range", "#8f8e86", True),
    (NO_VALUE, "#e0ded6", False),
]
VCOLOR = {v: c for v, c, _ in VERDICTS}
FLAGGED = [v for v, _, f in VERDICTS if f]
STAGES = [("lab", "#52514e"), ("injected", "#2a78d6"), ("rewritten", "#eb6834"), ("no unit", "#c3c2b7"),
          ("unknown", "#e0ded6")]
TEST_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
OTHER_COLOR = "#a8a69c"
INK, MUTED, LINE, SURFACE = "#0b0b0b", "#52514e", "#d6d4cc", "#fcfcfb"
GKEYS = ["test", "src_unit", "unit", "h_unit", "sex", "age", "ref_source", "lo", "hi", "lv", "lf", "excluded"]
GROUP_KEYS = ["test", "src_unit", "unit", "h_unit", "stage", "verdict", "suggested", "src_was_right"]
CELL = ["test", "unit", "h_unit"]  # the composition cell (qc_portal.py's rows): secondary suppression within it
TABLE_COLS = ["test", "source -> unit -> harmonized", "stage", "verdict", "rows", "% rows", "range (source scale)",
              "median pre-harm.", "median harmonized", "range x", "value x", "suggested unit"]
KS_COLS = ["test", "rows", "D (KS)", "differs", "% rows flagged", "Q1", "median", "Q3"]

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:6.1f}s] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Reference tables (read straight from the files: no network refresh)
# ---------------------------------------------------------------------------

def concept_info(omop_id, names_file, counts_file):
    name, quantity, target = None, None, None
    if Path(names_file).exists():
        df = pd.read_csv(names_file, sep="\t", dtype=str,
                         usecols=["harmonization_omop::OMOP_ID", "harmonization_omop::OMOP_NAME"])
        hit = df[df["harmonization_omop::OMOP_ID"] == str(omop_id)]
        name = hit["harmonization_omop::OMOP_NAME"].iloc[0] if len(hit) else None
    if Path(counts_file).exists():
        df = pd.read_csv(counts_file, sep="\t", dtype=str)
        hit = df[df["harmonization_omop::OMOP_ID"] == str(omop_id)]
        if len(hit):
            quantity = hit["harmonization_omop::OMOP_QUANTITY"].iloc[0]
            target = hit["harmonization_omop::MEASUREMENT_UNIT"].iloc[0]
    return name, quantity, target


def candidate_units(conversion_file, omop_id, quantity, target):
    """unit -> log10 conversion factor into the concept's harmonized unit, from the conversion table."""
    if not (quantity and target and Path(conversion_file).exists()):
        return {}
    df = pd.read_csv(conversion_file, sep="\t", dtype=str).fillna("")
    df = df[(df["harmonization_omop::OMOP_QUANTITY"] == quantity) & (df["TO_MEASUREMENT_UNIT"] == target)]
    # as the engine (reference_data.get_conversion_table): a restricted rule applies only to that exact OMOP id, and
    # wins over a general one for the same unit (sorted last, so it overwrites it in the dict below)
    only = df["ONLY_TO_OMOP_CONCEPTS"].str.strip()
    df = df[(only == "") | (only == str(omop_id))]
    df = df.assign(_specific=only.loc[df.index] != "").sort_values("_specific")
    f = pd.to_numeric(df["CONVERSION"], errors="coerce")
    df = df[f > 0].assign(lf=np.log10(f[f > 0]))
    return dict(zip(df["MEASUREMENT_UNIT"], df["lf"]))


# ---------------------------------------------------------------------------
# Groups (duckdb over the cached rows, with people sketches)
# ---------------------------------------------------------------------------

def build_groups(omop, part, cdir, con, force):
    """The OMOP's groups (GKEYS) with rows n and a people sketch p, from its cached rows; cached in cdir."""
    path, meta_p = cdir / "groups.parquet", cdir / "groups.json"
    want = {"version": CACHE_VERSION, "rows": str(part), "rows_mtime": S.part_mtime(part)}
    if path.exists() and meta_p.exists() and not force and json.loads(meta_p.read_text()) == want:
        log(f"OMOP {omop}: groups cached ({path})")
        return pd.read_parquet(path)
    src = f"read_parquet({S.sql_str(str(part / '*.parquet'))})"
    has_src = con.sql(f"select count(src_unit) from {src}").fetchone()[0] > 0
    rows = f"""(select coalesce(test, 'NA') test, {"coalesce(src_unit, 'NA')" if has_src else "'?'"} src_unit,
                       coalesce(unit, 'NA') unit, coalesce(h_unit, 'NA') h_unit,
                       case sex when 0 then 'F' when 1 then 'M' end sex,
                       case when age < 18 then 'minor' when age >= 18 then 'adult' end age,
                       coalesce(ref_src, case when ref_low is not null or ref_high is not null then 'num' end,
                                'none') ref_source,
                       round(case when ref_low > 0 then ref_low end, 6) lo,
                       round(case when ref_high > 0 then ref_high end, 6) hi,
                       case when v_raw > 0 and (v is null or v > 0) then round(log10(v_raw) / {BIN}) * {BIN} end lv,
                       case when v_raw > 0 and v > 0 then round(log10(v / v_raw), 3) end lf,
                       coalesce(v_raw is null or v_raw <= 0 or v <= 0, false) excluded, f
                from {src})"""
    df = con.sql(S.sketch_sql(GKEYS, rows)).df()
    cdir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    meta_p.write_text(json.dumps(want, indent=1) + "\n")
    log(f"OMOP {omop}: {len(df)} groups over {df['n'].sum()} rows -> {path}")
    return df


# ---------------------------------------------------------------------------
# Analysis (unchanged: on the groups, weighted by rows)
# ---------------------------------------------------------------------------

def wq(x, w, q=0.5):
    x, w = np.asarray(x, float), np.asarray(w, float)
    ok = np.isfinite(x) & (w > 0)
    if not ok.any():
        return np.nan
    x, w = x[ok], w[ok]
    o = np.argsort(x)
    cw = np.cumsum(w[o])
    return x[o][min(np.searchsorted(cw, q * cw[-1]), len(x) - 1)]


def robust(x, w):
    m = wq(x, w)
    return m, 1.4826 * wq(np.abs(np.asarray(x, float) - m), w)


def snap(lf_needed, cands, tol, exclude=None):
    """Units of the conversion table whose factor is within tol of lf_needed (log10)."""
    if not np.isfinite(lf_needed):
        return ""
    return "|".join(sorted(u for u, c in cands.items() if abs(c - lf_needed) <= tol and u != exclude))


def is_unit_shift(d, cands, tol):
    """True when the offset d (log10) is a unit factor: a non-zero power of 10, or the ratio of two candidate factors."""
    if not np.isfinite(d):
        return False
    ratios = {round(a - b, 3) for a in cands.values() for b in cands.values()} | {k for k in range(-6, 7)}
    return any(abs(d - r) <= tol for r in ratios if abs(r) > tol)


def add_stage(g):
    g["stage"] = np.select(
        [g["src_unit"] == "?", g["unit"] == "NA", g["src_unit"] == "NA", g["src_unit"] == g["unit"]],
        ["unknown", "no unit", "injected", "lab"], default="rewritten")
    return g


def analyse(g, cands, args):
    g = g[~g["excluded"]].copy()
    n_all = g["n"].sum()
    g["harm"] = g["lf"].notna()
    g["lh"] = g["lv"] + g["lf"]
    g["anchor"] = np.where(g["hi"].notna(), "upper", np.where(g["lo"].notna(), "lower", ""))
    g["la"] = np.log10(np.where(g["hi"].notna(), g["hi"], g["lo"]).astype(float))
    has_range = g["anchor"] != ""
    shift = math.log10(args.shift)
    rtol = math.log10(args.range_margin)

    # 3: harmonized value vs the concept's median
    h = g[g["harm"]]
    m_h, sd_h = robust(h["lh"], h["n"])
    tol_v = max(shift, args.far * sd_h)
    g["d_vu"] = g["lh"] - m_h
    C = g["d_vu"].abs() <= tol_v

    # 2: value vs its own range, minus the typical offset per anchor (upper / lower)
    g["raw_vr"] = g["lv"] - g["la"]
    g["d_vr"] = np.nan
    tol_vr = {}
    for a in ("upper", "lower"):
        sel = g["anchor"] == a
        if sel.any():
            typ, sd = robust(g.loc[sel, "raw_vr"], g.loc[sel, "n"])
            g.loc[sel, "d_vr"] = g.loc[sel, "raw_vr"] - typ
            tol_vr[a] = max(shift, args.far * sd)
    g["tol_vr"] = g["anchor"].map(tol_vr).astype(float)
    A = g["d_vr"].abs() <= g["tol_vr"]

    # canonical range: harmonized limits over the rows passing 2 and 3
    g["base"] = g["harm"] & has_range & A & C
    base = g[g["base"]]
    strata = [("all", "all")] + [(s, a) for s in ("F", "M") for a in ("minor", "adult")]
    canon = []
    for s, a in strata:
        b = base if s == "all" else base[(base["sex"] == s) & (base["age"] == a)]
        lo = b[b["lo"].notna()]
        hi = b[b["hi"].notna()]
        canon.append({"sex": s, "age": a, "rows": int(b["n"].sum()),
                      "lower": 10 ** wq(np.log10(lo["lo"]) + lo["lf"], lo["n"]) if len(lo) else np.nan,
                      "upper": 10 ** wq(np.log10(hi["hi"]) + hi["lf"], hi["n"]) if len(hi) else np.nan})
    canon = pd.DataFrame(canon)
    canon["use"] = (canon["rows"] >= args.min_stratum_rows) | ((canon["sex"] == "all") & (canon["rows"] > 0))
    use = canon[canon["use"]].set_index(["sex", "age"])
    if ("all", "all") not in use.index:
        raise SystemExit("No rows pass the value checks with a range: no canonical range to compare with")

    def canon_la(r):
        key = (r.sex, r.age) if (r.sex, r.age) in use.index else ("all", "all")
        x = use.loc[key, "upper" if r.anchor == "upper" else "lower"]
        if not np.isfinite(x):  # the stratum has no such limit: the pooled one
            x = use.loc[("all", "all"), "upper" if r.anchor == "upper" else "lower"]
        return np.log10(x)

    g["canon_la"] = [canon_la(r) if r.anchor else np.nan for r in g[["sex", "age", "anchor"]].itertuples()]
    # 1: harmonized range vs canonical; the factor that would put the range on the canonical one
    g["lf_needed"] = g["canon_la"] - g["la"]
    g["d_ru"] = g["la"] + g["lf"] - g["canon_la"]
    B = g["d_ru"].abs() <= rtol

    # verdicts
    harm, nr = g["harm"], ~has_range
    g["suggested"] = ""
    fix = ~harm & has_range & A
    g.loc[fix, "suggested"] = [snap(x, cands, rtol) for x in g.loc[fix, "lf_needed"]]
    fix_ok = fix & (g["suggested"] != "") & ((g["lv"] + g["lf_needed"] - m_h).abs() <= tol_v)
    shifted = g["d_vu"].apply(lambda d: is_unit_shift(d, cands, rtol))
    conds = [
        harm & nr & C,
        harm & nr & ~C,
        harm & has_range & A & B & C,
        harm & has_range & A & ~B & ~C,
        harm & has_range & ~A & ~B & C,
        harm & has_range & ~A & B & ~C & shifted,
        harm & has_range & B & ~C,
        harm & has_range,
        ~harm & nr,
        fix_ok,
        fix & (g["suggested"] == ""),
        ~harm & has_range,
    ]
    names = ["ok (no range)", "value off (no range)", "ok", "wrong unit", "range off scale", "value mis-scaled",
             "value extreme", "inconsistent", "unharmonized: no range", "unharmonized: fixable",
             "unharmonized: no unit for factor", "unharmonized: conflict"]
    g["verdict"] = np.select(conds, names, default="inconsistent")
    wrong = g["verdict"] == "wrong unit"
    g.loc[wrong, "suggested"] = [snap(x, cands, rtol, exclude=u) or f"x{10 ** x:.3g} (no unit)"
                                 for x, u in zip(g.loc[wrong, "lf_needed"] + 0, g.loc[wrong, "unit"])]
    g.loc[(g["verdict"] == "unharmonized: no unit for factor"), "suggested"] = [
        f"x{10 ** x:.3g} (no unit)" for x in g.loc[g["verdict"] == "unharmonized: no unit for factor", "lf_needed"]]
    g.loc[g["verdict"] == "unharmonized: conflict", "suggested"] = ""
    # no range: suggest from the value alone, when its offset is a unit factor (value off) or it isn't harmonized;
    # the unit whose factor brings the value to the concept median
    by_value = ((g["verdict"] == "value off (no range)") & shifted) | (g["verdict"] == "unharmonized: no range")
    g.loc[by_value, "suggested"] = [snap(m_h - lv, cands, rtol, exclude=u if h else None)
                                    for lv, u, h in zip(g.loc[by_value, "lv"], g.loc[by_value, "unit"],
                                                        g.loc[by_value, "harm"])]

    add_stage(g)
    g["src_was_right"] = (g["stage"] == "rewritten") & (g["suggested"] != "") & (g["suggested"] == g["src_unit"])

    stats = {"rows": int(n_all), "median": 10 ** m_h, "tol_value": 10 ** tol_v, "tol_value_range": tol_vr,
             "tol_range": args.range_margin}
    return g, canon, stats


# ---------------------------------------------------------------------------
# Tables (every count gated on people; the figure is drawn from these only)
# ---------------------------------------------------------------------------

def npeople(sketches):
    return len(S.pset(sketches))


def gate_groups(g):
    """g with `gid` (its GROUP_KEYS group) and `shown`: the group covers >= MIN_N people, and within each composition
    cell (test x unit x harmonized unit) the hidden groups together are none or >= MIN_N people."""
    g = g.copy()
    g["gid"] = g.groupby(GROUP_KEYS, dropna=False, sort=False).ngroup()
    grp = g.groupby("gid").agg(n=("n", "sum"), **{k: (k, "first") for k in CELL})
    grp["p"] = S.merge_sketch(g, ["gid"])
    grp["people"] = [npeople([p]) for p in grp["p"]]
    ok = np.array(grp["people"] >= S.MIN_N, dtype=bool)  # a writable copy (pandas 3 arrays are read-only)
    for _, idx in grp.groupby(CELL, dropna=False).indices.items():
        cells = grp.iloc[idx][["n", "p"]].reset_index(drop=True)
        ok[idx] = S.suppress_group(cells, ok[idx])
    g["shown"] = g["gid"].map(dict(zip(grp.index, ok)))
    return g


def group_table(g):
    """One row per GROUP_KEYS group: rows, median range / values / offsets (shown groups), % of the shown rows."""
    rows = []
    for k, d in g.groupby(GROUP_KEYS, sort=False, dropna=False):
        w = d["n"]
        shown = bool(d["shown"].iloc[0])
        st = dict(range_lower=wq(d["lo"], w), range_upper=wq(d["hi"], w), value=10 ** wq(d["lv"], w),
                  value_h=10 ** wq(d["lh"], w), x_range=10 ** wq(d["d_ru"], w),
                  x_value_vs_range=10 ** wq(d["d_vr"], w), x_value=10 ** wq(d["d_vu"], w)) if shown else {}
        rows.append(dict(zip(GROUP_KEYS, k), n=int(w.sum()), shown=shown, **st))
    t = pd.DataFrame(rows).sort_values(["test", "n"], ascending=[True, False], ignore_index=True)
    t["flagged"] = t["verdict"].isin(FLAGGED)
    # % of the rows in the shown groups: a share of the full total would give the hidden ones away
    t["pct"] = np.where(t["shown"], 100 * t["n"] / t.loc[t["shown"], "n"].sum(), np.nan)
    return t


def ks_tests(g, args):
    """Each test name's harmonized values vs the rest of the concept: the KS D statistic (largest gap between the two
    cumulative distributions, on the cached 0.01-log10 bins), from the retired explore_omop.py. D, not the p-value, which is ~0
    at these row counts even for comparable tests (serum vs plasma). It catches what the scale checks miss: another
    analyte or specimen on a similar scale (ap-na under sodium). With the share of the test's rows already flagged:
    a high D with few flagged rows points to a mapping problem rather than a unit one. A test is compared when it and
    the rest each cover >= MIN_N people; Q1 / Q3 are shown when >= MIN_N people lie beyond them."""
    h = g[g["harm"]].assign(b=(g["lh"] / 0.01).round())
    h = h[h["b"].notna()]
    tot = h.groupby("b")["n"].sum().sort_index()
    rows = []
    for test, d in h.groupby("test"):
        n = d["n"].sum()
        rest_rows = h[h["test"] != test]
        rest = tot - d.groupby("b")["n"].sum().reindex(tot.index, fill_value=0)
        if n < args.ks_min_n or rest.sum() < args.ks_min_n:
            continue
        if npeople(d["p"]) < S.MIN_N or npeople(rest_rows["p"]) < S.MIN_N:
            continue
        own = d.groupby("b")["n"].sum().reindex(tot.index, fill_value=0)
        dist = (own.cumsum() / n - rest.cumsum() / rest.sum()).abs().max()
        sh = g[(g["test"] == test) & g["shown"]]  # counts only over the groups the TSV shows
        bins = d.groupby("b").agg(n=("n", "sum"))
        bins["p"] = S.merge_sketch(d, ["b"])
        safe = S.safe_bins(bins.reset_index())
        qs = {}
        for name, q in (("q25", 0.25), ("q50", 0.5), ("q75", 0.75)):
            x = wq(d["lh"], d["n"], q)
            ok = safe is not None and safe[0] <= round(x / 0.01) <= safe[1]
            qs[name] = 10 ** x if ok else np.nan
        rows.append({"test": test, "rows": int(sh["n"].sum()), "D": float(dist), "differs": bool(dist >= args.ks_d),
                     "flagged_pct": 100 * sh.loc[sh["verdict"].isin(FLAGGED), "n"].sum() / sh["n"].sum()
                     if sh["n"].sum() else np.nan, **qs})
    cols = ["test", "rows", "D", "differs", "flagged_pct", "q25", "q50", "q75"]
    return pd.DataFrame(rows, columns=cols).sort_values("rows", ascending=False, ignore_index=True)


def canonical_table(g, canon):
    """The canonical range per stratum, shown when the stratum's base rows cover >= MIN_N people; `rows` counts the
    base rows of shown groups only (like every other count)."""
    out = []
    for r in canon.itertuples():
        b = g[g["base"]] if r.sex == "all" else g[g["base"] & (g["sex"] == r.sex) & (g["age"] == r.age)]
        ok = npeople(b["p"]) >= S.MIN_N
        out.append({"sex": r.sex, "age": r.age, "rows": str(int(b.loc[b["shown"], "n"].sum())) if ok else BELOW,
                    "lower": r.lower if ok else np.nan, "upper": r.upper if ok else np.nan, "use": bool(r.use and ok)})
    return pd.DataFrame(out)


def offsets_table(g, keep):
    """Per check (1 d_ru, 2 d_vr, 3 d_vu) x test (keep + 'other'): HBIN bins of the shown rows, pooled with their
    neighbours until each covers >= MIN_N people; `share` of the test's rows."""
    out = []
    edges0 = -XLIM
    sub0 = g[g["shown"]]
    for col in ("d_ru", "d_vr", "d_vu"):
        sub = sub0[sub0[col].notna()]
        for test in keep + ["other"]:
            d = sub[sub["test"] == test] if test != "other" else sub[~sub["test"].isin(keep)]
            if d.empty:
                continue
            d = d.assign(b=np.floor((d[col].clip(-XLIM, XLIM - 1e-9) - edges0) / HBIN).astype(int))
            cells = d.groupby("b").agg(n=("n", "sum")).reset_index()
            cells["p"] = S.merge_sketch(d, ["b"]).reindex(cells["b"]).to_numpy()
            pools = S.pool_adjacent(cells, "b", clip=True)
            if pools.empty:
                continue
            tot = pools["n"].sum()
            for p in pools.itertuples():
                out.append({"check": col, "test": test, "lo": edges0 + p.lo * HBIN, "hi": edges0 + (p.hi + 1) * HBIN,
                            "rows": int(p.n), "share": p.n / tot})
    return pd.DataFrame(out, columns=["check", "test", "lo", "hi", "rows", "share"])


def map_table(g):
    """Decision-map points: harmonized shown rows with a range, in 0.1-log10 cells per verdict; cells under MIN_N
    people are left out (the map shows sizes, no totals)."""
    d = g[g["shown"] & g["harm"] & g["d_ru"].notna()].copy()
    if d.empty:
        return pd.DataFrame(columns=["bx", "by", "verdict", "rows"])
    d["bx"] = (d["d_vu"].clip(-XLIM, XLIM) / 0.1).round() * 0.1
    d["by"] = (d["d_ru"].clip(-XLIM, XLIM) / 0.1).round() * 0.1
    pts = d.groupby(["bx", "by", "verdict"], as_index=False)["n"].sum()
    pts["p"] = S.merge_sketch(d, ["bx", "by", "verdict"]).reindex(
        pd.MultiIndex.from_frame(pts[["bx", "by", "verdict"]])).to_numpy()
    pts = pts[[npeople([p]) >= S.MIN_N for p in pts["p"]]]
    return pts.rename(columns={"n": "rows"})[["bx", "by", "verdict", "rows"]]


def fmt_pct(x):
    """Share of the concept's rows: 2 significant digits under 10 %, whole percent above."""
    if x is None or not np.isfinite(x):
        return ""
    return f"{x:.0f}" if x >= 10 else f"{x:.2g}"


def fmt(x):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return ""
    return f"{x:.0f}" if abs(x) >= 1000 else f"{x:.3g}"


def fmt_x(x):
    """A factor as text: 1 -> 'x1', 0.01 -> '/100'."""
    if x is None or not np.isfinite(x):
        return ""
    y = x if x >= 1 else 1 / x
    return ("x" if x >= 1 else "/") + (f"{y:.0f}" if y >= 10 else f"{y:.2g}")


def flagged_rows(t, top):
    f = t[t["flagged"] & t["shown"]].sort_values("n", ascending=False).head(top)
    out = []
    for r in f.itertuples():
        rng = (f"{fmt(r.range_lower)}-{fmt(r.range_upper)}" if np.isfinite(r.range_lower) and np.isfinite(r.range_upper)
               else f"<{fmt(r.range_upper)}" if np.isfinite(r.range_upper)
               else f">{fmt(r.range_lower)}" if np.isfinite(r.range_lower) else "")
        stage = r.stage + (" (source unit was right)" if r.src_was_right else "")
        out.append([r.test, f"{r.src_unit} -> {r.unit} -> {r.h_unit}", stage, r.verdict, f"{r.n}", fmt_pct(r.pct), rng,
                    fmt(r.value), fmt(r.value_h), fmt_x(r.x_range), fmt_x(r.x_value), r.suggested])
    return out


def build_tables(omop, groups, cands, target, args, cdir, groups_mtime):
    """All tables of the outputs, gated, cached in cdir (tables.json + *.parquet); rebuilt when the groups or the
    parameters change. Returns (tables dict, meta dict)."""
    params = {"version": TABLES_VERSION, "groups_mtime": groups_mtime, "min_n": S.MIN_N, "target": target,
              "cands": cands, **{k: getattr(args, k) for k in (
                  "shift", "far", "range_margin", "min_stratum_rows", "top_n_tests", "table_rows", "ks_d", "ks_min_n")}}
    meta_p = cdir / "tables.json"
    names = ["groups", "verdicts", "canonical", "tests", "offsets", "map", "vbars", "stages", "flagged"]
    if meta_p.exists() and not args.force:
        meta = json.loads(meta_p.read_text())
        if meta.get("params") == json.loads(json.dumps(params)) and all((cdir / f"t_{n}.parquet").exists() for n in names):
            log(f"OMOP {omop}: tables cached ({cdir})")
            return {n: pd.read_parquet(cdir / f"t_{n}.parquet") for n in names}, meta
    ex = groups[groups["excluded"]].copy()
    g, canon, stats = analyse(groups, cands, args)
    ex = add_stage(ex.assign(verdict=NO_VALUE, suggested="", src_was_right=False, harm=False, base=False))
    g = gate_groups(pd.concat([g, ex], ignore_index=True))
    t = group_table(g)
    shown = g[g["shown"]]
    a = g[g["verdict"] != NO_VALUE]  # the checked rows (the figure and the tests)
    # top tests by shown rows: drawn by name, the rest pooled as "other"
    order = shown[shown["verdict"] != NO_VALUE].groupby("test")["n"].sum().sort_values(ascending=False)
    keep = list(order.index[:args.top_n_tests])
    ks = ks_tests(a, args)
    v = shown.groupby("verdict")["n"].sum()
    verdicts = pd.DataFrame({"verdict": [x for x, _, _ in VERDICTS if x in v.index]})
    verdicts["rows"] = [int(v[x]) for x in verdicts["verdict"]]
    verdicts["pct"] = 100 * verdicts["rows"] / verdicts["rows"].sum() if len(verdicts) else []
    sa = a[a["shown"]]
    vb = sa.assign(test=np.where(sa["test"].isin(keep), sa["test"], "other")).groupby(
        ["test", "verdict"], as_index=False)["n"].sum().rename(columns={"n": "rows"})
    vb["share"] = vb["rows"] / vb.groupby("test")["rows"].transform("sum")
    st = sa[sa["verdict"].isin(FLAGGED)].groupby(["verdict", "stage"], as_index=False)["n"].sum().rename(
        columns={"n": "rows"})
    flagged = pd.DataFrame(flagged_rows(t, args.table_rows), columns=TABLE_COLS)
    tables = {"groups": t, "verdicts": verdicts, "canonical": canonical_table(a, canon), "tests": ks,
              "offsets": offsets_table(a, keep), "map": map_table(a), "vbars": vb, "stages": st, "flagged": flagged}
    fl = shown[shown["verdict"] != NO_VALUE]
    meta = {"params": params, "keep": keep, "stats": stats,
            "flagged_pct": 100 * fl.loc[fl["verdict"].isin(FLAGGED), "n"].sum() / fl["n"].sum() if len(fl) else None,
            "rtol": math.log10(args.range_margin), "tol_v": math.log10(stats["tol_value"]),
            "tol_vr": max(stats["tol_value_range"].values()) if stats["tol_value_range"] else math.log10(args.shift)}
    cdir.mkdir(parents=True, exist_ok=True)
    for n, df in tables.items():
        df.to_parquet(cdir / f"t_{n}.parquet", index=False)
    meta_p.write_text(json.dumps(meta, indent=1, default=float) + "\n")
    log(f"OMOP {omop}: tables -> {cdir}")
    return tables, json.loads(meta_p.read_text())


def masked_groups(t):
    """The groups table for export: hidden groups as BELOW_THRESHOLD, without statistics."""
    t = t.copy()
    stat_cols = ["range_lower", "range_upper", "value", "value_h", "x_range", "x_value_vs_range", "x_value"]
    for c in stat_cols:
        if c not in t.columns:
            t[c] = np.nan
    t[stat_cols] = t[stat_cols].map(lambda x: f"{x:.4g}" if pd.notna(x) and np.isfinite(x) else "")
    t["pct"] = [fmt_pct(x) for x in t["pct"].astype(float)]
    t.loc[~t["shown"], stat_cols + ["pct"]] = ""
    t["n"] = t["n"].astype(object)
    t.loc[~t["shown"], "n"] = BELOW
    return t[GROUP_KEYS + ["n", "pct"] + stat_cols + ["flagged"]]


def ks_rows(ks):
    return [[r.test, f"{r.rows}" if r.rows > 0 else BELOW, f"{r.D:.2f}", "yes" if r.differs else "",
             fmt_pct(r.flagged_pct) if r.rows > 0 else "", fmt(r.q25), fmt(r.q50), fmt(r.q75)]
            for r in ks.itertuples()]


def write_tsvs(stem, tables):
    S.publish_tsv(masked_groups(tables["groups"]), f"{stem}_groups.tsv", count_cols=("n",))
    S.publish_tsv(tables["verdicts"].assign(pct=tables["verdicts"]["pct"].map(fmt_pct)), f"{stem}_verdicts.tsv")
    S.publish_tsv(tables["canonical"], f"{stem}_canonical.tsv")
    S.publish_tsv(pd.DataFrame(ks_rows(tables["tests"]), columns=KS_COLS), f"{stem}_tests.tsv")
    log(f"Wrote {stem}_groups.tsv, _verdicts.tsv, _canonical.tsv, _tests.tsv")


# ---------------------------------------------------------------------------
# Figure (from the tables only)
# ---------------------------------------------------------------------------

def style(ax):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(LINE)
    ax.tick_params(colors=MUTED, labelsize=8)


def titled(ax, title, question):
    """Title on top, and under it, muted, the question the plot answers."""
    ax.set_title(title, fontsize=10, color=INK, loc="left", pad=19)
    ax.text(0, 1.015, question, transform=ax.transAxes, fontsize=8.5, color=MUTED, style="italic",
            ha="left", va="bottom")


def factor_axis(ax, which="x"):
    ticks = list(range(-3, 4))
    labels = ["/1000", "/100", "/10", "x1", "x10", "x100", "x1000"]
    (ax.set_xticks if which == "x" else ax.set_yticks)(ticks)
    (ax.set_xticklabels if which == "x" else ax.set_yticklabels)(labels)
    (ax.set_xlim if which == "x" else ax.set_ylim)(-XLIM - 0.1, XLIM + 0.1)


def offset_panel(ax, off, col, keep, colors, tol, title, question, xlabel):
    """One check's histograms: pooled bins (each >= MIN_N people), drawn as share per HBIN of the test's rows."""
    sub = off[off["check"] == col]
    for test in keep + ["other"]:
        d = sub[sub["test"] == test].sort_values("lo")
        if d.empty:
            continue
        xs, ys = [], []  # pools are contiguous ranges; a gap between two pools is drawn as nothing (NaN)
        for lo, hi, sh in zip(d["lo"], d["hi"], d["share"]):
            if xs and xs[-1] < lo - 1e-9:
                ys.append(np.nan)
                xs.append(lo)
            if not xs:
                xs.append(lo)
            ys.append(sh * HBIN / (hi - lo))
            xs.append(hi)
        ax.stairs(ys, xs, lw=1.4, alpha=0.8, color=colors.get(test, OTHER_COLOR), label=test, baseline=None)
    for lo, hi in ([(-tol, tol)] if np.isscalar(tol) else tol):
        ax.axvspan(lo, hi, color=MUTED, alpha=0.08, lw=0)
    factor_axis(ax)
    titled(ax, title, question)
    ax.set_xlabel(xlabel, fontsize=8, color=MUTED)
    ax.set_ylabel(f"share of the test's rows per {HBIN:g} log10 (log scale)", fontsize=8, color=MUTED)
    ax.set_yscale("log")
    ax.set_ylim(bottom=1e-4)
    ax.grid(axis="x", color=LINE, lw=0.5)
    style(ax)


def decision_map(ax, pts, tol_v, rtol):
    smax = pts["rows"].max() if len(pts) else 1
    ax.axhspan(-rtol, rtol, color=MUTED, alpha=0.06, lw=0)
    ax.axvspan(-tol_v, tol_v, color=MUTED, alpha=0.06, lw=0)
    ax.plot([-XLIM, XLIM], [-XLIM, XLIM], color=LINE, lw=1, ls="--", zorder=0)
    for v, c, _ in VERDICTS:
        p = pts[pts["verdict"] == v]
        if len(p):
            ax.scatter(p["bx"], p["by"], s=12 + 500 * np.sqrt(p["rows"] / smax), color=c, alpha=0.85,
                       edgecolor=SURFACE, lw=1, label=v, zorder=3)
    for x, y, txt in [(0.5, 0.35, "ok"), (-3.4, -1.3, "wrong unit: value and\nrange shift together"),
                      (0.55, 2.6, "range off scale"), (0.55, -2.9, "range off scale"),
                      (-3.4, 0.35, "value mis-scaled"), (1.5, 0.35, "value mis-scaled")]:
        ax.text(x, y, txt, fontsize=7.5, color=MUTED, va="bottom", style="italic")
    factor_axis(ax, "x")
    factor_axis(ax, "y")
    titled(ax, "Decision map: harmonized rows with a range", "What is off: the unit, the value or the range?")
    ax.set_xlabel("3 · harmonized value / concept median", fontsize=8, color=MUTED)
    ax.set_ylabel("1 · harmonized range / canonical range", fontsize=8, color=MUTED)
    if len(pts):
        leg = ax.legend(fontsize=7, loc="lower right", frameon=True, facecolor=SURFACE, edgecolor=LINE)
        for h in leg.legend_handles:
            h.set_sizes([40])
    ax.grid(color=LINE, lw=0.5)
    style(ax)


def verdict_bars(ax, vb, keep, ks):
    if vb.empty:
        ax.text(0.5, 0.5, "no rows shown", ha="center", va="center", transform=ax.transAxes, color=MUTED)
        ax.axis("off")
        return
    order = [t for t in keep + ["other"] if t in set(vb["test"])][::-1]
    share = vb.pivot_table(index="test", columns="verdict", values="share", aggfunc="sum", fill_value=0).reindex(order)
    left = np.zeros(len(share))
    for v, c, _ in VERDICTS:
        if v in share.columns and share[v].sum() > 0:
            ax.barh(range(len(share)), share[v], left=left, color=c, label=v, height=0.7, edgecolor=SURFACE, lw=1)
            left += share[v].to_numpy()
    ax.set_yticks(range(len(share)))
    dmap = dict(zip(ks["test"], ks["D"]))
    ax.set_yticklabels([f"{x}\nD {dmap[x]:.2f}" if x in dmap else x for x in share.index], fontsize=8)
    differs = set(ks.loc[ks["differs"], "test"])
    for lab, x in zip(ax.get_yticklabels(), share.index):  # tests whose distribution differs: the critical color
        if x in differs:
            lab.set_color(VCOLOR["wrong unit"])
            lab.set_fontweight("bold")
    ax.set_xlim(0, 1)
    ax.set_xlabel("share of the test's rows", fontsize=8, color=MUTED)
    titled(ax, "Verdict per test (shares of the rows shown; D = KS vs the other tests, bold: differs)",
           "Which tests have problems, and which don't belong here?")
    ax.legend(fontsize=7, loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=3, frameon=False)
    style(ax)


def stage_bars(ax, st):
    if st.empty:
        ax.text(0.5, 0.5, "no flagged rows", ha="center", va="center", transform=ax.transAxes, color=MUTED)
        ax.axis("off")
        return
    ct = st.pivot_table(index="verdict", columns="stage", values="rows", aggfunc="sum", fill_value=0)
    ct = ct.loc[[v for v in FLAGGED if v in ct.index and ct.loc[v].sum() > 0]][::-1]
    if ct.empty:
        ax.text(0.5, 0.5, "no flagged rows", ha="center", va="center", transform=ax.transAxes, color=MUTED)
        ax.axis("off")
        return
    left = np.zeros(len(ct))
    for s, c in STAGES:
        if s in ct.columns:
            ax.barh(range(len(ct)), ct[s], left=left, color=c, label=s, height=0.7, edgecolor=SURFACE, lw=1)
            left += ct[s].to_numpy()
    for i, total in enumerate(left):
        ax.text(total, i, f"  {total:.0f}", va="center", fontsize=7.5, color=INK)
    ax.set_yticks(range(len(ct)))
    ax.set_yticklabels(ct.index, fontsize=8)
    ax.set_xlabel("rows", fontsize=8, color=MUTED)
    ax.set_xlim(0, left.max() * 1.2)
    titled(ax, "Flagged rows: where the unit came from", "Where did the problem come from: the lab or injection?")
    ax.legend(fontsize=7, loc="lower right", frameon=False)
    style(ax)


def report(tables, meta, args, title, path):
    keep = meta["keep"]
    colors = {t: TEST_COLORS[i] for i, t in enumerate(keep)}
    rtol, tol_v, tol_vr = meta["rtol"], meta["tol_v"], meta["tol_vr"]
    rows = tables["flagged"].values.tolist()
    stats, prm = meta["stats"], meta["params"]
    plt.rcParams["font.family"] = "DejaVu Sans"
    fig = plt.figure(figsize=(22, 15 + 0.32 * max(len(rows), 1)), facecolor=SURFACE)
    gs = fig.add_gridspec(3, 3, height_ratios=[1, 1.15, 0.25 + 0.075 * max(len(rows), 1)],
                          hspace=0.38, wspace=0.32, left=0.06, right=0.98, top=0.88, bottom=0.02)
    off = tables["offsets"]
    offset_panel(fig.add_subplot(gs[0, 0]), off, "d_ru", keep, colors, rtol, "1 · Range vs canonical",
                 "Does the row's unit match its reference range? (range harmonized with the unit, vs the other rows')",
                 "harmonized range / canonical range (shaded: same scale)")
    offset_panel(fig.add_subplot(gs[0, 1]), off, "d_vr", keep, colors, tol_vr, "2 · Value vs its own range",
                 "Does the row agree with itself? (the value vs its own range)",
                 "value / own range, over the concept's typical")
    offset_panel(fig.add_subplot(gs[0, 2]), off, "d_vu", keep, colors, tol_v, "3 · Harmonized value vs concept",
                 "Is the harmonized value on scale? (vs the other rows')", "harmonized value / concept median")
    shown = keep + (["other"] if (off["test"] == "other").any() else [])
    handles = [plt.Line2D([], [], color=colors.get(t, OTHER_COLOR), lw=2) for t in shown]
    fig.legend(handles, shown, loc="upper right", bbox_to_anchor=(0.98, 0.925), ncol=max(len(handles), 1),
               fontsize=8, frameon=False, title="test (top row)", title_fontsize=8)
    decision_map(fig.add_subplot(gs[1, 0]), tables["map"], tol_v, rtol)
    verdict_bars(fig.add_subplot(gs[1, 1]), tables["vbars"], keep, tables["tests"])
    stage_bars(fig.add_subplot(gs[1, 2]), tables["stages"])

    tx = fig.add_subplot(gs[2, :])
    tx.axis("off")
    if rows:
        tb = tx.table(cellText=rows, colLabels=TABLE_COLS, loc="upper center", cellLoc="left", colLoc="left")
        tb.auto_set_font_size(False)
        tb.set_fontsize(8)
        tb.auto_set_column_width(list(range(len(TABLE_COLS))))
        tb.scale(1, 1.45)
        for (r, c), cell in tb.get_celld().items():
            cell.set_edgecolor(LINE)
            cell.set_facecolor(SURFACE)
            cell.get_text().set_color(MUTED if r == 0 else INK)
            if r > 0 and c == 3:
                cell.set_facecolor(VCOLOR[rows[r - 1][3]] + "40")
        titled(tx, f"Flagged groups (largest {len(rows)}; all flagged: {fmt_pct(meta['flagged_pct'])} % of the rows)"
                   f": range x / value x = offset of the harmonized range / "
                   f"value from the canonical range / concept median", "What should be fixed, and to which unit?")
    else:
        tx.text(0.5, 0.5, "no flagged groups", ha="center", color=MUTED, transform=tx.transAxes)

    c = tables["canonical"]
    c = c[c["use"]]
    ctext = "   ".join(f"{r.sex}/{r.age}: {fmt(r.lower)}-{fmt(r.upper)}" for r in c.itertuples())
    cands = prm["cands"]
    cand = ", ".join(f"{u} {fmt_x(10 ** f)}" for u, f in sorted(cands.items(), key=lambda kv: kv[1])) or "none found"
    fig.suptitle(title, fontsize=14, fontweight="bold", color=INK, x=0.06, y=0.985, ha="left")
    fig.text(0.06, 0.95,
             f"canonical range ({prm['target'] or 'harmonized unit'}): {ctext}      concept median "
             f"{fmt(stats['median'])}      conversion into {prm['target']}: {cand}\n"
             f"same scale when within: range x{prm['range_margin']:g}, value x{stats['tol_value']:.2g}, "
             f"value vs range x{10 ** tol_vr:.2g}   ·   every group, bin and point covers >= {S.MIN_N} people "
             f"(histogram bins pooled with their neighbours)",
             fontsize=9, color=MUTED, ha="left", va="top")
    fig.savefig(path, dpi=200, facecolor=SURFACE)
    pdf = str(path)[:-4] + ".pdf"  # the same figure as vectors: sharp at any zoom
    fig.savefig(pdf, facecolor=SURFACE)
    plt.close(fig)
    log(f"Wrote {path} and {pdf}")


def markdown(rows, cols=None):
    cols = cols or TABLE_COLS
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def print_summary(tables, meta):
    target = meta["params"]["target"]
    print("\nRows per verdict (shown groups):")
    for r in tables["verdicts"].itertuples():
        print(f"  {r.verdict:<34} {r.rows:>12}")
    c = tables["canonical"]
    print("\nCanonical range:\n" + c[c["rows"] != BELOW].to_string(index=False))
    rows = tables["flagged"].values.tolist()
    print(f"\nFlagged rows: {fmt_pct(meta['flagged_pct'])} % of the rows (in the shown groups)")
    print(f"Flagged groups (largest first; % rows = share of the shown rows; value x = median harmonized / concept "
          f"median {fmt(meta['stats']['median'])}{' ' + target if target else ''}):\n" + (markdown(rows) if rows else "none"))
    ks = tables["tests"]
    print(f"\nTest names vs the rest of the concept (KS on harmonized values, differs: D >= {meta['params']['ks_d']:g}; "
          f"Q1 / median / Q3 in {target or 'the harmonized unit'}; a high D with few rows flagged: another analyte or "
          f"specimen?):\n" + (markdown(ks_rows(ks), KS_COLS) if len(ks) else "no tests to compare"))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(description=" ".join(__doc__.strip().splitlines()[2:4]),
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("omop_id", nargs="?", help="One OMOP_ID to audit (same as --omop ID)")
    p.add_argument("--omop", nargs="+", default=[], help="OMOP_IDs to audit (space- or comma-separated)")
    p.add_argument("--all", action="store_true", help="Every OMOP in the input with >= --min-rows rows")
    p.add_argument("--min-rows", type=int, default=100_000, help="With --all: OMOPs with at least this many rows")
    p.add_argument("--input", type=Path, default=S.latest_engine_output(),
                   help=f"Engine output (or release) parquet (default: the most recent run in {S.ENGINE_DIR}, "
                        f"now %(default)s)")
    p.add_argument("--out-dir", type=Path, default=Path("/mnt/disks/data/kanta/qc/omop"),
                   help="Report folder (sp_qc.py writes next to it, in qc/sp)")
    p.add_argument("--cache", type=Path, default=S.CACHE,
                   help="sp_qc.py's cache: the rows (rows_<input>/, shared) and this script's groups and tables (audit/)")
    p.add_argument("--extract-batch", type=int, default=40, help="OMOPs extracted per pass over the input")
    p.add_argument("--force", action="store_true", help="Rebuild the groups and tables (not the shared rows)")
    p.add_argument("--plots-only", action="store_true", help="Redraw the figures from the cached tables")
    p.add_argument("--names", type=Path, default=DATA_DIR / "LABfi.tsv", help="Usagi mapping (concept names)")
    p.add_argument("--harmonization-counts", type=Path, default=DATA_DIR / "harmonization_counts.tsv",
                   help="OMOP_ID -> quantity and harmonized unit")
    p.add_argument("--conversion", type=Path, default=DATA_DIR / "quantity_source_unit_conversion.tsv",
                   help="Unit conversion table (candidate units for the suggestions)")
    p.add_argument("--shift", type=float, default=3.0,
                   help="Checks 2 and 3: at least this factor off counts as off scale (floor of the tolerance)")
    p.add_argument("--far", type=float, default=3.0,
                   help="Checks 2 and 3: tolerance in robust SDs (1.4826 x MAD, log10) when wider than --shift")
    p.add_argument("--range-margin", type=float, default=2.0,
                   help="Check 1: a harmonized range within this factor of the canonical one is on the same scale; "
                        "also the tolerance when matching a factor to a unit of the conversion table")
    p.add_argument("--min-stratum-rows", type=int, default=100,
                   help="Rows a sex x age stratum needs for its own canonical range")
    p.add_argument("--min-n", type=int, default=5, choices=range(1, S.SKETCH + 1), metavar=f"1..{S.SKETCH}",
                   help="People every published number must cover")
    p.add_argument("--top-n-tests", type=int, default=6, help="Tests drawn by name (the rest pooled as 'other')")
    p.add_argument("--table-rows", type=int, default=15, help="Flagged groups listed in the report")
    p.add_argument("--ks-d", type=float, default=0.3,
                   help="A test name's distribution differs from the rest of the concept at this KS D")
    p.add_argument("--ks-min-n", type=int, default=20, help="Harmonized rows a test name (and the rest) needs for the KS test")
    p.add_argument("--skip-done", action="store_true",
                   help="Skip the OMOPs whose figure (PDF) is already in --out-dir and newer than their cached rows, "
                        "e.g. after lowering --min-rows; parameters and code changes are not checked")
    p.add_argument("--memory-limit", default=f"{int(S.RAM_GB * 0.45)}GB", help="duckdb memory limit (spills beyond it)")
    p.add_argument("--threads", type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads")
    return p


def audit_one(omop, rows, args, con):
    name, quantity, target = concept_info(omop, args.names, args.harmonization_counts)
    title = f"OMOP {omop}" + (f" — {name}" if name else "") + ": injection / harmonization audit with reference ranges"
    stem = args.out_dir / f"omop_{omop}_ref_audit"
    cdir = args.cache / "audit" / f"omop={omop}"
    if args.plots_only:
        meta_p = cdir / "tables.json"
        if not meta_p.exists():
            log(f"OMOP {omop}: no cached tables in {cdir}: run without --plots-only first")
            return
        meta = json.loads(meta_p.read_text())
        tables = {p.stem[2:]: pd.read_parquet(p) for p in cdir.glob("t_*.parquet")}
        report(tables, meta, args, title, f"{stem}.png")
        return
    part = rows / f"omop={omop}"
    if not part.is_dir():
        log(f"OMOP {omop}: no rows in {Path(args.input).name}, skipped")
        return
    g = build_groups(omop, part, cdir, con, args.force)
    if g.empty or npeople(g["p"]) < S.MIN_N:
        log(f"OMOP {omop}: rows of fewer people than the analysis minimum: nothing to publish, skipped")
        return
    if not target:  # not in the harmonization table: the input's most common harmonized unit
        hu = g[g["h_unit"] != "NA"].groupby("h_unit")["n"].sum()
        target = hu.idxmax() if len(hu) else None
    cands = candidate_units(args.conversion, omop, quantity, target)
    log(f"OMOP {omop} ({name or 'name not found'}): {quantity} in {target}; candidate units {cands}")
    tables, meta = build_tables(omop, g, cands, target, args, cdir, (cdir / "groups.parquet").stat().st_mtime)
    write_tsvs(stem, tables)
    report(tables, meta, args, title, f"{stem}.png")
    print_summary(tables, meta)


def main():
    args = build_parser().parse_args()
    S.MIN_N = args.min_n
    omops = ([args.omop_id] if args.omop_id else []) + [x for o in args.omop for x in o.split(",") if x.strip()]
    omops = list(dict.fromkeys(o.strip() for o in omops))
    if not omops and not args.all:
        raise SystemExit("Give OMOP ids (an id, or --omop ID[,ID ...]) or --all")
    bad = [o for o in omops if not o.isdigit()]
    if bad:
        raise SystemExit(f"Not an OMOP id: {', '.join(bad)}")
    if args.input is None:
        raise SystemExit(f"No engine output found in {S.ENGINE_DIR}: pass --input <engine_output>.parquet")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.cache / "duckdb").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"set memory_limit = '{args.memory_limit}'")
    con.sql(f"set threads = {args.threads}")
    con.sql(f"set temp_directory = '{args.cache / 'duckdb'}'")
    con.sql("set preserve_insertion_order = false")
    log(f"Input: {args.input}")
    # the shared row cache (sp_qc.py's): --force here rebuilds this script's groups and tables, not the rows
    rargs = argparse.Namespace(input=args.input, cache=args.cache, force=False, extract_batch=args.extract_batch)
    if args.all:
        c = S.omop_counts(rargs, con)
        omops = list(dict.fromkeys(omops + [str(o) for o in c.loc[c["n"] >= args.min_rows].sort_values(
            "n", ascending=False)["omop"]]))
    rows = S.ensure_rows(omops, rargs, con) if not args.plots_only else None
    log(f"{len(omops)} OMOP(s): {', '.join(omops)}; out {args.out_dir}")
    failed = []
    for i, omop in enumerate(omops, 1):
        log(f"PROGRESS {i}/{len(omops)} OMOP {omop}")  # for run_qc_all.sh's progress bar
        pdf = args.out_dir / f"omop_{omop}_ref_audit.pdf"  # the figure is written last: there, the OMOP is done
        if args.skip_done and rows is not None and S.up_to_date(pdf, rows / f"omop={omop}"):
            log(f"OMOP {omop}: done already ({pdf.name}), skipped (--skip-done)")
            continue
        try:
            audit_one(omop, rows, args, con)
        except SystemExit as e:  # one OMOP's problem (e.g. no canonical range) does not stop the others
            if len(omops) == 1:
                raise
            log(f"OMOP {omop}: skipped: {e}")
        except Exception:  # an unexpected error: logged with its traceback, the run goes on
            if len(omops) == 1:
                raise
            failed.append(omop)
            plt.close("all")
            log(f"OMOP {omop}: FAILED, skipped:\n{traceback.format_exc()}")
    if failed:
        log(f"{len(failed)} OMOP(s) failed: {', '.join(failed)} (tracebacks above)")


if __name__ == "__main__":
    main()
