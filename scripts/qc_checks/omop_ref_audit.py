#!/usr/bin/env python3
"""
omop_ref_audit.py

Audit one OMOP concept with the reference ranges: which rows went wrong in injection/harmonization, how, where in the
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
Canonical range: weighted median of the harmonized limits over rows passing A and C, per sex x age group (F/M x minor
<18 / adult) with >= --min-rows rows, else pooled over every row.

Stage, from cleaned-pre-inj::MEASUREMENT_UNIT (source) vs MEASUREMENT_UNIT (post-injection):
  lab        source unit kept         -> the error, if any, came from the lab
  injected   source NA, unit injected -> injection chose it
  rewritten  source X -> Y            -> injection changed it (also tells when the suggested unit is the source one:
                                         injection broke a right unit)
  no unit    no unit after injection;  unknown: the input has no pre-injection unit column

Usage
-----
  python3 omop_ref_audit.py OMOP_ID [--input PARQUET] [--out-dir DIR] [--cache-dir DIR] [--force]

--input defaults to the most recent engine run in /mnt/disks/data/kanta/engine
(<prefix>_<YYYY_MM_DD>/<prefix>_<YYYY_MM_DD>.parquet, the latest date in the folder name).

Reads one OMOP's rows from an engine output (or release) parquet, aggregated in duckdb to groups (test x source unit x
unit x harmonized unit x sex x age x range x 0.01-log10 value bin x factor), cached under --cache-dir and rebuilt when
older than the input. Writes into --out-dir:
  omop_<id>_ref_audit.png            the report: the three offsets per test, a decision map (value vs range offset),
                                     verdict shares per test, flagged rows per stage, and the flagged groups
  omop_<id>_ref_audit_groups.tsv     test x source unit -> unit -> harmonized unit x stage x verdict x suggested unit: rows,
                                     median range (source scale), median value before (value) and after
                                     harmonization (value_h), median offsets as factors
  omop_<id>_ref_audit_canonical.tsv  the canonical range per stratum
  omop_<id>_ref_audit_tests.tsv      per test name: KS D of its harmonized values vs the rest of the concept (differs
                                     at D >= --ks-d), % of its rows flagged, harmonized Q1 / median / Q3: a high D with
                                     few flagged rows is another analyte or specimen on a similar scale (the scale
                                     checks miss it)
and prints the flagged groups as Markdown. Aggregates only: groups and bins under --min-n rows are left out of the plot
and written as BELOW_THRESHOLD in the TSV; "% rows" is the share of the
rows in the groups shown (not of the full total, which would give the masked ones away); verdict and stage counts sum
the shown groups only; no per-test totals are written.
"""
import argparse
import math
import time
from pathlib import Path

import duckdb
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

try:
    from kanta import config
    DATA_DIR = Path(config.DATA_DIR)
except ImportError:  # outside the kanta env: the repo's data folder
    DATA_DIR = Path(__file__).resolve().parents[2] / "src" / "kanta" / "engine" / "data"

# short name -> (engine output column, release column)
COLUMNS = {
    "test": ("TEST_NAME_ABBREVIATION", "TEST_NAME"),
    "omop": ("harmonization_omop::OMOP_ID", "OMOP_CONCEPT_ID"),
    "quantity": ("harmonization_omop::OMOP_QUANTITY", "OMOP_QUANTITY"),
    "v": ("MEASUREMENT_VALUE", "MEASUREMENT_VALUE_CLEANED"),
    "unit": ("MEASUREMENT_UNIT", "MEASUREMENT_UNIT_CLEANED"),
    "src_unit": ("cleaned-pre-inj::MEASUREMENT_UNIT", "MEASUREMENT_UNIT_PRE_INJ"),
    "h": ("harmonization_omop::MEASUREMENT_VALUE", "MEASUREMENT_VALUE_HARMONIZED"),
    "h_unit": ("harmonization_omop::MEASUREMENT_UNIT", "MEASUREMENT_UNIT_HARMONIZED"),
    "sex": ("SEX", "SEX"),
    "age": ("EVENT_AGE", "EVENT_AGE"),
    "lo": ("REFERENCE_RANGE_LOWER_VALUE", "REFERENCE_RANGE_LOW_VALUE"),
    "hi": ("REFERENCE_RANGE_UPPER_VALUE", "REFERENCE_RANGE_HIGH_VALUE"),
    "ref_source": ("REFERENCE_RANGE_SOURCE", "REFERENCE_RANGE_SOURCE"),
}
OPTIONAL = {"quantity", "src_unit", "h_unit", "sex", "age", "ref_source"}
BIN = 0.01  # log10 width of the value bins in the cached groups
HBIN = 0.05  # log10 width of the offset histograms
XLIM = 3.5  # offsets drawn within +- this (log10); beyond, piled at the edge
CACHE_VERSION = "v1"
ENGINE_DIR = Path("/mnt/disks/data/kanta/engine")
BELOW = "BELOW_THRESHOLD"

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
]
VCOLOR = {v: c for v, c, _ in VERDICTS}
FLAGGED = [v for v, _, f in VERDICTS if f]
STAGES = [("lab", "#52514e"), ("injected", "#2a78d6"), ("rewritten", "#eb6834"), ("no unit", "#c3c2b7"),
          ("unknown", "#e0ded6")]
TEST_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
OTHER_COLOR = "#a8a69c"
INK, MUTED, LINE, SURFACE = "#0b0b0b", "#52514e", "#d6d4cc", "#fcfcfb"

t0 = time.time()


def latest_engine_output(engine_dir=ENGINE_DIR):
    """Most recent engine output: runs sit in <engine_dir>/<prefix>_<YYYY_MM_DD>/<prefix>_<YYYY_MM_DD>.parquet; the one
    with the latest date in its folder name (ties: the newest file), or None if there is none."""
    runs = [(d.name[-10:], (d / f"{d.name}.parquet").stat().st_mtime, d / f"{d.name}.parquet")
            for d in engine_dir.glob("*_[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]") if (d / f"{d.name}.parquet").is_file()]
    return max(runs)[2] if runs else None


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
# Groups (duckdb, cached)
# ---------------------------------------------------------------------------

def build_groups(parquet, omop_id, cache_dir, force, memory_limit, threads):
    cache = Path(cache_dir) / f"ref_audit_groups_{omop_id}_{Path(parquet).stem}_{CACHE_VERSION}.parquet"
    if cache.exists() and not force and cache.stat().st_mtime >= Path(parquet).stat().st_mtime:
        log(f"Cached groups: {cache}")
        return pd.read_parquet(cache)
    con = duckdb.connect()
    con.execute(f"set memory_limit = '{memory_limit}'")
    con.execute(f"set threads = {threads}")
    con.execute("set preserve_insertion_order = false")
    cols = set(con.sql(f"select * from read_parquet('{parquet}') limit 0").columns)

    def col(name):
        c = next((c for c in COLUMNS[name] if c in cols), None)
        if c is None:
            if name in OPTIONAL:
                return "null"
            raise SystemExit(f"{parquet}: missing column {' / '.join(COLUMNS[name])}")
        return f"nullif(nullif(trim(cast(\"{c}\" as varchar)), ''), 'NA')"

    def num(name):
        return f"try_cast(replace({col(name)}, ',', '.') as double)"

    has_src = col("src_unit") != "null"
    log(f"Querying OMOP {omop_id} from {parquet} (full scan)...")
    df = con.execute(f"""
        with r as (
            select coalesce({col('test')}, 'NA') test,
                   {"coalesce(" + col('src_unit') + ", 'NA')" if has_src else "'?'"} src_unit,
                   coalesce({col('unit')}, 'NA') unit, coalesce({col('h_unit')}, 'NA') h_unit,
                   case when lower({col('sex')}) in ('female', 'f') then 'F'
                        when lower({col('sex')}) in ('male', 'm') then 'M' end sex,
                   case when {num('age')} < 18 then 'minor' when {num('age')} >= 18 then 'adult' end age,
                   {num('v')} v, {num('h')} h,
                   case when {num('lo')} > 0 then {num('lo')} end lo, case when {num('hi')} > 0 then {num('hi')} end hi,
                   coalesce({col('ref_source')},
                            case when {num('lo')} is not null or {num('hi')} is not null then 'num' end, 'none') ref_source
            from read_parquet('{parquet}') where {col('omop')} = ?)
        select test, src_unit, unit, h_unit, sex, age, ref_source, round(lo, 6) lo, round(hi, 6) hi,
               case when v > 0 and (h is null or h > 0) then round(log10(v) / {BIN}) * {BIN} end lv,
               case when v > 0 and h > 0 then round(log10(h / v), 3) end lf,
               coalesce(v is null or v <= 0 or h <= 0, false) excluded, count(*) n
        from r group by all""", [str(omop_id)]).df()
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache, index=False)
    log(f"{len(df)} groups over {df['n'].sum()} rows -> {cache}")
    return df


# ---------------------------------------------------------------------------
# Analysis
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
    base = g[g["harm"] & has_range & A & C]
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
    canon["use"] = (canon["rows"] >= args.min_rows) | ((canon["sex"] == "all") & (canon["rows"] > 0))
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

    # stage
    g["stage"] = np.select(
        [g["src_unit"] == "?", g["unit"] == "NA", g["src_unit"] == "NA", g["src_unit"] == g["unit"]],
        ["unknown", "no unit", "injected", "lab"], default="rewritten")
    g["src_was_right"] = (g["stage"] == "rewritten") & (g["suggested"] != "") & (g["suggested"] == g["src_unit"])

    stats = {"rows": int(n_all), "median": 10 ** m_h, "tol_value": 10 ** tol_v, "tol_value_range": tol_vr,
             "tol_range": args.range_margin}
    return g, canon, stats


GROUP_KEYS = ["test", "src_unit", "unit", "h_unit", "stage", "verdict", "suggested", "src_was_right"]


def group_table(g, min_n):
    keys = GROUP_KEYS
    rows = []
    for k, d in g.groupby(keys, sort=False):
        n = d["n"].sum()
        w = d["n"]
        rows.append(dict(zip(keys, k), n=int(n),
                         range_lower=wq(d["lo"], w), range_upper=wq(d["hi"], w), value=10 ** wq(d["lv"], w),
                         value_h=10 ** wq(d["lh"], w),
                         x_range=10 ** wq(d["d_ru"], w), x_value_vs_range=10 ** wq(d["d_vr"], w),
                         x_value=10 ** wq(d["d_vu"], w)))
    t = pd.DataFrame(rows).sort_values(["test", "n"], ascending=[True, False])
    t["flagged"] = t["verdict"].isin(FLAGGED)
    # % of the rows in the shown groups (>= min_n): a share of the full total would give the total away, and the
    # total minus the shown groups is the masked ones
    t["pct"] = 100 * t["n"] / t.loc[t["n"] >= min_n, "n"].sum()
    return t


def ks_tests(g, min_n, ks_min_n, d_flag):
    """Each test name's harmonized values vs the rest of the concept: the KS D statistic (largest gap between the two
    cumulative distributions, on the cached 0.01-log10 bins), from the retired explore_omop.py. D, not the p-value, which is ~0
    at these row counts even for comparable tests (serum vs plasma). It catches what the scale checks miss: another
    analyte or specimen on a similar scale (ap-na under sodium). With the share of the test's rows already flagged:
    a high D with few flagged rows points to a mapping problem rather than a unit one."""
    h = g[g["harm"]].assign(b=(g["lh"] / 0.01).round())
    h = h[h["b"].notna()]
    tot = h.groupby("b")["n"].sum().sort_index()
    rows = []
    for test, d in h.groupby("test"):
        n = d["n"].sum()
        rest = tot - d.groupby("b")["n"].sum().reindex(tot.index, fill_value=0)
        if n < ks_min_n or rest.sum() < ks_min_n:
            continue
        own = d.groupby("b")["n"].sum().reindex(tot.index, fill_value=0)
        dist = (own.cumsum() / n - rest.cumsum() / rest.sum()).abs().max()
        sh = g[(g["test"] == test) & g["shown"]]  # counts only over the groups the TSV shows
        rows.append({"test": test, "rows": int(sh["n"].sum()), "D": float(dist), "differs": bool(dist >= d_flag),
                     "flagged_pct": 100 * sh.loc[sh["verdict"].isin(FLAGGED), "n"].sum() / sh["n"].sum()
                     if sh["n"].sum() else np.nan,
                     "q25": 10 ** wq(d["lh"], d["n"], 0.25), "q50": 10 ** wq(d["lh"], d["n"]),
                     "q75": 10 ** wq(d["lh"], d["n"], 0.75)})
    cols = ["test", "rows", "D", "differs", "flagged_pct", "q25", "q50", "q75"]
    return pd.DataFrame(rows, columns=cols).sort_values("rows", ascending=False, ignore_index=True)


KS_COLS = ["test", "rows", "D (KS)", "differs", "% rows flagged", "Q1", "median", "Q3"]


def ks_rows(ks, min_n):
    return [[r.test, f"{r.rows}" if r.rows >= min_n else BELOW, f"{r.D:.2f}", "yes" if r.differs else "",
             fmt_pct(r.flagged_pct) if r.rows >= min_n else "", fmt(r.q25), fmt(r.q50), fmt(r.q75)]
            for r in ks.itertuples()]


def masked(t, min_n):
    t = t.copy()
    small = t["n"] < min_n
    stat_cols = ["pct", "range_lower", "range_upper", "value", "value_h", "x_range", "x_value_vs_range", "x_value"]
    t[stat_cols] = t[stat_cols].map(lambda x: f"{x:.4g}" if np.isfinite(x) else "")
    t["pct"] = [fmt_pct(x) for x in t["pct"].replace("", np.nan).astype(float)]
    t.loc[small, stat_cols] = ""
    t["n"] = t["n"].astype(object)
    t.loc[small, "n"] = BELOW
    return t


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

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
    if not np.isfinite(x):
        return ""
    y = x if x >= 1 else 1 / x
    return ("x" if x >= 1 else "/") + (f"{y:.0f}" if y >= 10 else f"{y:.2g}")


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


def top_tests(g, n_top):
    order = g.groupby("test")["n"].sum().sort_values(ascending=False)
    keep = list(order.index[:n_top])
    return keep, {t: TEST_COLORS[i] for i, t in enumerate(keep)}


def offset_panel(ax, g, col, keep, colors, tol, title, question, xlabel, min_n):
    edges = np.arange(-XLIM, XLIM + HBIN / 2, HBIN)
    sub = g[g[col].notna()]
    for test in keep + ["other"]:
        d = sub[sub["test"] == test] if test != "other" else sub[~sub["test"].isin(keep)]
        if d.empty:
            continue
        x = d[col].clip(-XLIM, XLIM - 1e-9)
        hist, _ = np.histogram(x, bins=edges, weights=d["n"])
        hist[hist < min_n] = 0  # small bins left out
        if hist.sum() == 0:
            continue
        ax.step(edges[:-1], hist / hist.sum(), where="post", lw=1.4, alpha=0.8,
                color=colors.get(test, OTHER_COLOR), label=test)
    for lo, hi in ([(-tol, tol)] if np.isscalar(tol) else tol):
        ax.axvspan(lo, hi, color=MUTED, alpha=0.08, lw=0)
    factor_axis(ax)
    titled(ax, title, question)
    ax.set_xlabel(xlabel, fontsize=8, color=MUTED)
    ax.set_ylabel("share of the test's rows (log scale)", fontsize=8, color=MUTED)
    ax.set_yscale("log")
    ax.set_ylim(bottom=1e-4)
    ax.grid(axis="x", color=LINE, lw=0.5)
    style(ax)


def decision_map(ax, g, tol_v, rtol, min_n):
    d = g[g["harm"] & g["d_ru"].notna()].copy()
    d["bx"] = (d["d_vu"].clip(-XLIM, XLIM) / 0.1).round() * 0.1
    d["by"] = (d["d_ru"].clip(-XLIM, XLIM) / 0.1).round() * 0.1
    pts = d.groupby(["bx", "by", "verdict"], as_index=False)["n"].sum()
    pts = pts[pts["n"] >= min_n]
    smax = pts["n"].max() if len(pts) else 1
    ax.axhspan(-rtol, rtol, color=MUTED, alpha=0.06, lw=0)
    ax.axvspan(-tol_v, tol_v, color=MUTED, alpha=0.06, lw=0)
    ax.plot([-XLIM, XLIM], [-XLIM, XLIM], color=LINE, lw=1, ls="--", zorder=0)
    for v, c, _ in VERDICTS:
        p = pts[pts["verdict"] == v]
        if len(p):
            ax.scatter(p["bx"], p["by"], s=12 + 500 * np.sqrt(p["n"] / smax), color=c, alpha=0.85,
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
    leg = ax.legend(fontsize=7, loc="lower right", frameon=True, facecolor=SURFACE, edgecolor=LINE)
    for h in leg.legend_handles:
        h.set_sizes([40])
    ax.grid(color=LINE, lw=0.5)
    style(ax)


def verdict_bars(ax, g, keep, min_n, ks=None):
    g = g[g["shown"]]
    d = g.assign(test=np.where(g["test"].isin(keep), g["test"], "other"))
    ct = d.pivot_table(index="test", columns="verdict", values="n", aggfunc="sum", fill_value=0)
    ct = ct.where(ct >= min_n, 0)  # small cells left out; shares are of the shown rows only
    order = [t for t in keep + ["other"] if t in ct.index][::-1]
    ct = ct.loc[order]
    share = ct.div(ct.sum(axis=1).replace(0, np.nan), axis=0).fillna(0)
    left = np.zeros(len(ct))
    for v, c, _ in VERDICTS:
        if v in share.columns and share[v].sum() > 0:
            ax.barh(range(len(ct)), share[v], left=left, color=c, label=v, height=0.7, edgecolor=SURFACE, lw=1)
            left += share[v].to_numpy()
    ax.set_yticks(range(len(ct)))
    dmap = dict(zip(ks["test"], ks["D"])) if ks is not None else {}
    ax.set_yticklabels([f"{x}\nD {dmap[x]:.2f}" if x in dmap else x for x in ct.index], fontsize=8)
    if ks is not None:  # tests whose distribution differs from the rest: label in the critical color
        for lab, x in zip(ax.get_yticklabels(), ct.index):
            if x in set(ks.loc[ks["differs"], "test"]):
                lab.set_color(dict((v, c) for v, c, _ in VERDICTS)["wrong unit"])
                lab.set_fontweight("bold")
    ax.set_xlim(0, 1)
    ax.set_xlabel("share of the test's rows", fontsize=8, color=MUTED)
    titled(ax, "Verdict per test (shares of the rows shown; D = KS vs the other tests, bold: differs)",
           "Which tests have problems, and which don't belong here?")
    ax.legend(fontsize=7, loc="upper center", bbox_to_anchor=(0.5, -0.1), ncol=3, frameon=False)
    style(ax)


def stage_bars(ax, g, min_n):
    d = g[g["shown"] & g["verdict"].isin(FLAGGED)]
    ct = d.pivot_table(index="verdict", columns="stage", values="n", aggfunc="sum", fill_value=0)
    ct = ct.where(ct >= min_n, 0)
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


TABLE_COLS = ["test", "source -> unit -> harmonized", "stage", "verdict", "rows", "% rows", "range (source scale)",
              "median pre-harm.", "median harmonized", "range x", "value x", "suggested unit"]


def flagged_pct(t, min_n):
    """Share of the rows in the shown groups that are flagged, the overall impact."""
    shown = t[t["n"] >= min_n]
    return fmt_pct(100 * shown.loc[shown["flagged"], "n"].sum() / shown["n"].sum()) + " %"


def flagged_rows(t, min_n, top):
    f = t[t["flagged"] & (t["n"] >= min_n)].sort_values("n", ascending=False).head(top)
    out = []
    for r in f.itertuples():
        rng = (f"{fmt(r.range_lower)}-{fmt(r.range_upper)}" if np.isfinite(r.range_lower) and np.isfinite(r.range_upper)
               else f"<{fmt(r.range_upper)}" if np.isfinite(r.range_upper)
               else f">{fmt(r.range_lower)}" if np.isfinite(r.range_lower) else "")
        stage = r.stage + (" (source unit was right)" if r.src_was_right else "")
        out.append([r.test, f"{r.src_unit} -> {r.unit} -> {r.h_unit}", stage, r.verdict, f"{r.n}", fmt_pct(r.pct), rng,
                    fmt(r.value), fmt(r.value_h), fmt_x(r.x_range), fmt_x(r.x_value), r.suggested])
    return out


def report(g, t, canon, stats, cands, args, title, path, ks=None):
    keep, colors = top_tests(g, args.top_n_tests)
    rtol = math.log10(args.range_margin)
    tol_v = math.log10(stats["tol_value"])
    tol_vr = stats["tol_value_range"]
    rows = flagged_rows(t, args.min_n, args.table_rows)

    plt.rcParams["font.family"] = "DejaVu Sans"
    fig = plt.figure(figsize=(22, 15 + 0.32 * max(len(rows), 1)), facecolor=SURFACE)
    gs = fig.add_gridspec(3, 3, height_ratios=[1, 1.15, 0.25 + 0.075 * max(len(rows), 1)],
                          hspace=0.38, wspace=0.32, left=0.06, right=0.98, top=0.88, bottom=0.02)
    offset_panel(fig.add_subplot(gs[0, 0]), g, "d_ru", keep, colors, rtol,
                 "1 · Range vs canonical",
                 "Does the row's unit match its reference range? (range harmonized with the unit, vs the other rows')",
                 "harmonized range / canonical range (shaded: same scale)", args.min_n)
    offset_panel(fig.add_subplot(gs[0, 1]), g, "d_vr", keep, colors, max(tol_vr.values()),
                 "2 · Value vs its own range", "Does the row agree with itself? (the value vs its own range)",
                 "value / own range, over the concept's typical", args.min_n)
    offset_panel(fig.add_subplot(gs[0, 2]), g, "d_vu", keep, colors, tol_v,
                 "3 · Harmonized value vs concept", "Is the harmonized value on scale? (vs the other rows')",
                 "harmonized value / concept median", args.min_n)
    shown = keep + (["other"] if (~g["test"].isin(keep)).any() else [])
    handles = [plt.Line2D([], [], color=colors.get(t, OTHER_COLOR), lw=2) for t in shown]
    fig.legend(handles, shown, loc="upper right", bbox_to_anchor=(0.98, 0.925), ncol=len(handles),
               fontsize=8, frameon=False, title="test (top row)", title_fontsize=8)

    decision_map(fig.add_subplot(gs[1, 0]), g, tol_v, rtol, args.min_n)
    verdict_bars(fig.add_subplot(gs[1, 1]), g, keep, args.min_n, ks)
    stage_bars(fig.add_subplot(gs[1, 2]), g, args.min_n)

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
        titled(tx, f"Flagged groups (largest {len(rows)}; all flagged: {flagged_pct(t, args.min_n)} of the rows)"
                   f": range x / value x = offset of the harmonized range / "
                   f"value from the canonical range / concept median", "What should be fixed, and to which unit?")
    else:
        tx.text(0.5, 0.5, "no flagged groups", ha="center", color=MUTED, transform=tx.transAxes)

    c = canon[canon["use"]]
    ctext = "   ".join(f"{r.sex}/{r.age}: {fmt(r.lower)}-{fmt(r.upper)}" for r in c.itertuples())
    cand = ", ".join(f"{u} {fmt_x(10 ** f)}" for u, f in sorted(cands.items(), key=lambda kv: kv[1])) or "none found"
    fig.suptitle(title, fontsize=14, fontweight="bold", color=INK, x=0.06, y=0.985, ha="left")
    fig.text(0.06, 0.95,
             f"canonical range ({stats['target'] or 'harmonized unit'}): {ctext}      concept median "
             f"{fmt(stats['median'])}      conversion into {stats['target']}: {cand}\n"
             f"same scale when within: range x{args.range_margin:g}, value x{stats['tol_value']:.2g}, "
             f"value vs range x{10 ** max(tol_vr.values()):.2g}   ·   groups and bins under {args.min_n} rows left out",
             fontsize=9, color=MUTED, ha="left", va="top")
    fig.savefig(path, dpi=130, facecolor=SURFACE)
    plt.close(fig)
    log(f"Wrote {path}")


def markdown(rows, cols=None):
    cols = cols or TABLE_COLS
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(description=" ".join(__doc__.strip().splitlines()[2:4]),
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("omop_id", help="OMOP_ID to audit")
    p.add_argument("--input", type=Path, default=latest_engine_output(),
                   help=f"Engine output (or release) parquet (default: the most recent run in {ENGINE_DIR}, "
                        f"now %(default)s)")
    p.add_argument("--out-dir", type=Path, default=Path("/mnt/disks/data/kanta/qc/omop"),
                   help="Report folder (sp_qc.py writes next to it, in qc/sp)")
    p.add_argument("--cache-dir", type=Path, default=Path("/mnt/disks/data/kanta/qc/dump_omop"),
                   help="Cache of the per-OMOP groups (rebuilt when older than the input)")
    p.add_argument("--force", action="store_true", help="Rebuild the cached groups")
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
    p.add_argument("--min-rows", type=int, default=100, help="Rows a sex x age stratum needs for its own canonical range")
    p.add_argument("--min-n", type=int, default=5, help="Groups / bins with fewer rows are left out of the outputs")
    p.add_argument("--top-n-tests", type=int, default=6, help="Tests drawn by name (the rest pooled as 'other')")
    p.add_argument("--table-rows", type=int, default=15, help="Flagged groups listed in the report")
    p.add_argument("--ks-d", type=float, default=0.3,
                   help="A test name's distribution differs from the rest of the concept at this KS D")
    p.add_argument("--ks-min-n", type=int, default=20, help="Harmonized rows a test name (and the rest) needs for the KS test")
    p.add_argument("--memory-limit", default="8GB", help="duckdb memory limit")
    p.add_argument("--threads", type=int, default=8, help="duckdb threads")
    return p


def main():
    args = build_parser().parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    name, quantity, target = concept_info(args.omop_id, args.names, args.harmonization_counts)
    if args.input is None:
        raise SystemExit(f"No engine output found in {ENGINE_DIR}: pass --input <engine_output>.parquet")
    log(f"Input: {args.input}")
    g = build_groups(args.input, args.omop_id, args.cache_dir, args.force, args.memory_limit, args.threads)
    if g.empty:
        raise SystemExit(f"No rows for OMOP_ID {args.omop_id}")
    if not target:  # not in the harmonization table: the input's most common harmonized unit
        hu = g[g["h_unit"] != "NA"].groupby("h_unit")["n"].sum()
        target = hu.idxmax() if len(hu) else None
    cands = candidate_units(args.conversion, args.omop_id, quantity, target)
    log(f"OMOP {args.omop_id} ({name or 'name not found'}): {quantity} in {target}; candidate units {cands}")

    excluded = int(g.loc[g["excluded"], "n"].sum())
    g, canon, stats = analyse(g, cands, args)
    stats["target"] = target
    # no exact row total here: with the per-verdict counts it would give away a masked one
    log(f"{len(g)} groups analysed (rows without a value > 0 left out: "
        f"{f'{excluded}' if excluded == 0 or excluded >= args.min_n else BELOW}); concept median {stats['median']:.4g}")

    t = group_table(g, args.min_n)
    # every count below (verdict and stage totals) sums only the groups the TSV shows, so none minus the shown
    # groups gives a masked one away
    g["shown"] = g.groupby(GROUP_KEYS, dropna=False)["n"].transform("sum") >= args.min_n
    stem = args.out_dir / f"omop_{args.omop_id}_ref_audit"
    masked(t, args.min_n).to_csv(f"{stem}_groups.tsv", sep="\t", index=False, float_format="%.4g")
    # the pooled row is the sum of the strata (when every row has a sex and age): mask it too when the masked strata
    # would follow from it
    small = (canon["rows"] < args.min_n) & (canon["sex"] != "all")
    pooled_hidden = 0 < canon.loc[small, "rows"].sum() < args.min_n
    canon.astype({"rows": object}).assign(rows=[
        BELOW if r < args.min_n or (s == "all" and pooled_hidden) else r
        for r, s in zip(canon["rows"], canon["sex"])]).to_csv(
        f"{stem}_canonical.tsv", sep="\t", index=False, float_format="%.4g")
    ks = ks_tests(g, args.min_n, args.ks_min_n, args.ks_d)
    pd.DataFrame(ks_rows(ks, args.min_n), columns=KS_COLS).to_csv(f"{stem}_tests.tsv", sep="\t", index=False)
    log(f"Wrote {stem}_groups.tsv, {stem}_canonical.tsv, {stem}_tests.tsv")

    title = f"OMOP {args.omop_id}" + (f" — {name}" if name else "") + ": injection / harmonization audit with reference ranges"
    report(g, t, canon, stats, cands, args, title, f"{stem}.png", ks)

    v = g[g["shown"]].groupby("verdict")["n"].sum()
    print("\nRows per verdict (groups of >= --min-n rows):")
    for name_, _, _ in VERDICTS:
        if name_ in v.index:
            n = v[name_]
            print(f"  {name_:<34} {n:>12}" if n >= args.min_n else f"  {name_:<34} {BELOW:>12}")
    shown_canon = canon[(canon["rows"] >= args.min_n) & ~((canon["sex"] == "all") & pooled_hidden)]
    print("\nCanonical range:\n" + shown_canon.to_string(index=False))
    rows = flagged_rows(t, args.min_n, args.table_rows)
    print(f"\nFlagged rows: {flagged_pct(t, args.min_n)} of the rows (in groups of >= --min-n rows)")
    print(f"Flagged groups (largest first; % rows = share of all rows; value x = median harmonized / concept median {fmt(stats['median'])}"
          f"{' ' + target if target else ''}):\n" + (markdown(rows) if rows else "none"))
    print(f"\nTest names vs the rest of the concept (KS on harmonized values, differs: D >= {args.ks_d:g}; "
          f"Q1 / median / Q3 in {target or 'the harmonized unit'}; a high D with few rows flagged: another analyte or "
          f"specimen?):\n" + (markdown(ks_rows(ks, args.min_n), KS_COLS) if len(ks) else "no tests to compare"))


if __name__ == "__main__":
    main()
