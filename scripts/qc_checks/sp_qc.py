#!/usr/bin/env python3
"""
sp_qc.py

Service-provider (SP) and time QC for OMOP concepts: which labs, and when, deviate from the national picture, and why.

0 rows       each OMOP's rows from the engine output (--input, default the latest run in /mnt/disks/data/kanta/engine),
             cached under --cache/rows_<input name>/omop=<id>/: one row per result with a value (SP, test, unit, QC
             note, hashed person, year, month, sex, age, harmonized and raw value, missing unit before injection,
             extracted, decimals, QC pass, TEST_OUTCOME, numeric reference limits, range source). The OMOPs not cached
             yet are extracted together, in one pass over the input (duckdb, a partitioned write: no sort); the cache
             is dropped when the input file changes (size or mtime) or with --force. Units and harmonization are
             omop_ref_audit.py's job; here they only show up as cause hints.

Per OMOP (cached under --cache/omop=<id>/, rebuilt when its rows change or the parameters do):
  1 baseline   per sex x 10-year age band: median and Tukey fences (--fence-k) of log10 harmonized value, and the share
               of rows outside them; per sex x minor/adult: the typical (modal) reference limits, converted with each
               row's own factor (harmonized / raw value) so that all SPs share one scale
  2 counts     per SP x year x month: rows, a histogram of residuals (log10 value minus its stratum median, i.e. a shift
               adjusted for sex and age), observed and expected outliers, QC fails, TEST_OUTCOME against the row's own
               range, jumps from the person's own mean (leave-one-out, Tukey fences on the national jumps), converted
               limits per sex x minor/adult, and the mix of test, unit, unit missing, extracted, decimals, QC note,
               range source and outcome (for the causes)
  3 flags      (thresholds below; the year flags of one SP and kind are merged when consecutive)
     shift         median >= x--shift off national, per SP x year
     step          the best single breakpoint in an SP's monthly medians (>= --min-seg months each side, >= x--step,
                   >= --step-z noise units); "with range change" when its upper limit changed at the same time
                   (a method change, e.g. creatinine), "range unchanged" otherwise (drift or a data problem)
     episode       >= --episode-months consecutive months off the SP's own baseline (median >= x--shift and >=
                   --episode-z noise units, or outliers O/E >= --oe) that return to it afterwards (e.g. HDL, one place
                   for about a year)
     outliers      outliers outside the stratum fences, O/E >= --oe and >= --min-events, per SP x year
     qc_fail       QC-fail rate >= --oe x national and >= --min-events, per SP x year
     range         the SP's typical upper (or lower) limit >= x--range-margin off national, per sex, adults only
                   (children's limits change with age within "minor")
                   (e.g. ferritin HUSLAB, TPO-Ab assays)
     range_change  an SP's typical upper limit changed >= x--range-step between consecutive years
     outcome       TEST_OUTCOME disagrees with the row's own range (H/L vs N) in >= --outcome-rate of rows, >= --oe x
                   national, per SP x year
     person_jump   values far from the same person's other values, O/E >= --oe and >= --min-events, per SP x year
  4 cause      per flag with an SP and a period: the category whose share changed most in the flagged period vs the
               SP's other periods (vs the other SPs when the SP has no other period), e.g. "decimals 1: 3% -> 96%"

Usage
-----
  python3 sp_qc.py [--omop ID ...] [--input PARQUET] [--out DIR] [--cache DIR]

--omop defaults to the QC-thread concepts (creatinine, ALT, ferritin, HDL, TPO-Ab, TSH, holo-TC, sodium); --all runs
every OMOP in the input with >= --min-rows rows (one pass extracts them all). Writes into --out:
  sp_qc_flags.tsv              every flag of every OMOP in --out, most severe first (score = |log10 effect| x log10 rows)
  sp_qc_<omop>.png             SP x year shift heatmap, monthly series of the flagged SPs with their upper limit,
                               upper limit per SP vs national, the flags
  sp_qc_<omop>_flags.tsv       that OMOP's flags
  sp_qc_<omop>_sp_year.tsv     per SP x year: rows, shift, outliers O/E, QC fail %, outcome mismatch %, person-jump O/E,
                               typical upper limit
  sp_qc_<omop>_ranges.tsv      per sex x minor/adult: the national typical range and its spread, and each SP's
Aggregates only: cells under --min-n rows are written as BELOW_THRESHOLD (stats blank) and left out of the plots; no
totals are written next to masked cells. SERVICE_PROVIDER is a pseudonym in R15: SPs are labelled by the organisation
most of their rows' coding systems belong to (CODING_SYSTEM_MAP), plus the end of the pseudonym.
"""
import argparse
import json
import math
import os
import re
import shutil
import time
from pathlib import Path

import duckdb
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import matplotlib.ticker  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

ENGINE_DIR = Path("/mnt/disks/data/kanta/engine")
ORG_MAP = Path(__file__).resolve().parents[2] / "src/kanta/engine/data/coding_system_map.tsv"
ORG_CODE = re.compile(r"^1\.2\.246\.(?:537\.)?10\.(\d+)\.")
OUT = Path("/mnt/disks/data/kanta/qc/sp")  # next to omop_ref_audit.py's qc/omop
CACHE = Path("/mnt/disks/data/kanta/tmp/sp_qc")
THREAD = ["3020564", "3006923", "3001122", "3023602", "3027238", "3009201", "43054903", "3019550"]
BIN = 0.01  # log10 width of the residual and baseline bins
RCLIP = 2.0  # residuals clipped to +- this (log10) before binning
MAX_GAP = 2  # missing months allowed inside a run of an SP's monthly series
CACHE_VERSION = "v4"
BASE_VERSION = "r2"
RAM_GB = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
# base column -> (engine output column, release column); the release has no pre-injection unit
COLUMNS = {
    "omop": ("harmonization_omop::OMOP_ID", "OMOP_CONCEPT_ID"), "sp": ("SERVICE_PROVIDER", "SERVICE_PROVIDER_ID"),
    "test": ("TEST_NAME_ABBREVIATION", "TEST_NAME"), "f": ("FINNGENID", "FINNGENID"),
    "dt": ("APPROX_EVENT_DATETIME", "APPROX_EVENT_DATETIME"), "sex": ("SEX", "SEX"), "age": ("EVENT_AGE", "EVENT_AGE"),
    "v": ("harmonization_omop::MEASUREMENT_VALUE", "MEASUREMENT_VALUE_HARMONIZED"),
    "v_raw": ("MEASUREMENT_VALUE", "MEASUREMENT_VALUE_CLEANED"), "unit": ("MEASUREMENT_UNIT", "MEASUREMENT_UNIT_CLEANED"),
    "unit_pre_inj": ("cleaned-pre-inj::MEASUREMENT_UNIT", None),
    "extracted": ("IS_VALUE_EXTRACTED", "IS_VALUE_EXTRACTED"), "qc_pass": ("QC_PASS", "QC_PASS"),
    "note": ("QC_NOTES", "QC_NOTES"), "outcome": ("TEST_OUTCOME", "TEST_OUTCOME"),
    "ref_low": ("REFERENCE_RANGE_LOWER_VALUE", "REFERENCE_RANGE_LOW_VALUE"),
    "ref_high": ("REFERENCE_RANGE_UPPER_VALUE", "REFERENCE_RANGE_HIGH_VALUE"),
    "ref_src": ("REFERENCE_RANGE_SOURCE", "REFERENCE_RANGE_SOURCE"),
    "org": ("CODING_SYSTEM_MAP", "CODING_SYSTEM_ORG"),  # organisation of the row's coding system: names the SP
    "h_unit": ("harmonization_omop::MEASUREMENT_UNIT", "MEASUREMENT_UNIT_HARMONIZED"),
}
REQUIRED = {"omop", "sp", "f", "dt", "sex", "age", "v", "v_raw"}  # the rest is left null when the input lacks it
BELOW = "BELOW_THRESHOLD"
DIMS = {  # cause dimension -> SQL expression (as text)
    "test": "cast(k_test as varchar)", "unit": "cast(k_unit as varchar)", "note": "cast(k_note as varchar)",
    "unit_missing": "cast(unit_missing as varchar)", "extracted": "cast(extracted as varchar)",
    "decimals": "cast(decimals as varchar)", "ref_src": "cast(ref_src as varchar)",
}
KINDS = ["step", "episode", "shift", "range", "outliers", "person_jump", "qc_fail", "outcome", "range_change"]

INK, MUTED, LINE, SURFACE = "#0b0b0b", "#52514e", "#d6d4cc", "#fcfcfb"
NEG, POS = "#2a78d6", "#eb6834"  # diverging poles (below / above national)
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
KIND_COLOR = {"step": "#d03b3b", "episode": "#ec835a", "shift": "#fab219", "range": "#6250d6", "outliers": "#e87ba4",
              "person_jump": "#8f8e86", "qc_fail": "#52514e", "outcome": "#1baf7a", "range_change": "#86b6ef"}

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def wq_hist(bins, n, q):
    """Weighted quantile of binned values."""
    bins, n = np.asarray(bins, float), np.asarray(n, float)
    if n.sum() <= 0:
        return np.nan
    o = np.argsort(bins)
    cw = np.cumsum(n[o])
    return bins[o][min(np.searchsorted(cw, q * cw[-1]), len(bins) - 1)]


def group_median(df, keys, val, w="n"):
    """Median of the integer bin `val` per `keys`, interpolated within its bin (the grouped-data median), so that
    medians move smoothly instead of snapping to bin centres. In bin units."""
    if df.empty:
        return pd.DataFrame(columns=keys + ["med", "rows"])
    # one row per bin first: the interpolation needs the bin's whole count (rows of several months share a bin)
    d = df.groupby(keys + [val], as_index=False)[w].sum().sort_values(keys + [val])
    d = d.assign(cum=d.groupby(keys)[w].cumsum(), tot=d.groupby(keys)[w].transform("sum"))
    m = d[d["cum"] >= d["tot"] / 2].groupby(keys, as_index=False).first()
    m["med"] = m[val] - 0.5 + (m["tot"] / 2 - (m["cum"] - m[w])) / m[w]
    return m[keys + ["med", "tot"]].rename(columns={"tot": "rows"})


def group_mode(df, keys, val, w="n"):
    """Most common `val` per `keys`, with its share of the group's rows."""
    if df.empty:
        return pd.DataFrame(columns=keys + ["mode", "mode_share", "rows"])
    g = df.groupby(keys + [val], as_index=False)[w].sum()
    g["rows"] = g.groupby(keys)[w].transform("sum")
    top = g.sort_values(w, ascending=False).drop_duplicates(keys)
    return top.assign(mode_share=top[w] / top["rows"])[keys + [val, "mode_share", "rows"]].rename(columns={val: "mode"})


def fx(log_ratio):
    """A log10 ratio as a factor: 0.1 -> 'x1.26', -0.1 -> '/1.26'."""
    if log_ratio is None or not np.isfinite(log_ratio):
        return ""
    y = 10 ** abs(log_ratio)
    return ("x" if log_ratio >= 0 else "/") + (f"{y:.2f}" if y < 10 else f"{y:.0f}")


def fnum(x):
    if x is None or not np.isfinite(x):
        return ""
    return f"{x:.0f}" if abs(x) >= 1000 else f"{x:.3g}"


def ym(t):
    """Month index (year * 12 + month - 1) -> 'YYYY-MM'."""
    return f"{int(t) // 12}-{int(t) % 12 + 1:02d}"


def main_limit(r):
    """The limit an SP's range is followed by: its upper limit, or the lower one when it has no upper (e.g. HDL), in
    its main sex x minor/adult group (adults when there are any: children's limits change with age within "minor").
    Returns (rows of that group with that limit, column, 'upper'/'lower', label)."""
    col, name = ("lhi", "upper") if r["lhi"].notna().any() else ("llo", "lower")
    r = r[r[col].notna()]
    if (r["ag"] == "adult").any():
        r = r[r["ag"] == "adult"]
    main = r.groupby(["sx", "ag"])["n"].sum().idxmax()
    return r[(r["sx"] == main[0]) & (r["ag"] == main[1])], col, name, f"{sex_label(main[0])} {main[1]}"


def sex_label(s):
    return {"0": "F", "1": "M"}.get(str(s), "U")


# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

class Labels:
    """SP -> organisation label. SERVICE_PROVIDER is a pseudonym in R15, so the name is the organisation most of the
    SP's rows' coding systems belong to (CODING_SYSTEM_MAP, from the engine's coding_system_map.tsv), as
    sp_region_cs.py did; a real OID (1.2.246[.537].10.<code>...) is looked up by its org code first."""

    def __init__(self, org_map=None):
        self.code, self.org = {}, {}
        if org_map and Path(org_map).exists():
            m = pd.read_csv(org_map, sep="\t", dtype=str, usecols=["code", "org_name"]).dropna()
            self.code = dict(zip(m["code"], m["org_name"]))

    def add_orgs(self, orgs):
        """SP -> its most common coding-system organisation, from a per-OMOP `orgs` table (k_sp, org, n)."""
        if orgs is None or not len(orgs):
            return
        o = orgs[~orgs["org"].isin(["NA", ""])].groupby(["k_sp", "org"], as_index=False)["n"].sum()
        for sp, g in o.groupby("k_sp"):
            if sp not in self.org:
                self.org[sp] = g.sort_values("n", ascending=False)["org"].iloc[0].replace("_", " ")

    @staticmethod
    def name(kind, key):
        if key is None or (isinstance(key, float) and np.isnan(key)) or str(key) in ("", "None", "nan", "<NA>"):
            return "NA"
        return str(key)

    def sp(self, raw):
        if raw is None or (isinstance(raw, float) and np.isnan(raw)):
            return "SP NA"
        raw = str(raw)
        code = ORG_CODE.match(raw)
        org = (self.code.get(code.group(1)) if code else None) or self.org.get(raw)
        tail = raw[-6:]
        return f"{org[:28]} …{tail}" if org else f"SP …{tail}"


# ---------------------------------------------------------------------------
# 0 Rows per OMOP (duckdb, from the engine output, cached until it changes)
# ---------------------------------------------------------------------------

def latest_engine_output(engine_dir=ENGINE_DIR):
    """Most recent engine output: runs sit in <engine_dir>/<prefix>_<YYYY_MM_DD>/<prefix>_<YYYY_MM_DD>.parquet; the one
    with the latest date in its folder name (ties: the newest file), or None if there is none."""
    runs = [(d.name[-10:], (d / f"{d.name}.parquet").stat().st_mtime, d / f"{d.name}.parquet")
            for d in engine_dir.glob("*_[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]") if (d / f"{d.name}.parquet").is_file()]
    return max(runs)[2] if runs else None


def sql_str(x):
    """A duckdb string literal (quotes doubled)."""
    return "'" + str(x).replace("'", "''") + "'"


def extract_query(inp, types, omops):
    """Engine (or release) columns -> the slim row columns: "NA" / '' -> null, numbers parsed whatever the input type,
    the person hashed; rows of the given OMOPs (None: every mapped one) with a value."""
    col = {k: next((c for c in cols if c and c in types), None) for k, cols in COLUMNS.items()}
    missing = sorted(k for k in REQUIRED if col[k] is None)
    if missing:
        raise SystemExit(f"{inp}: missing columns " + ", ".join(f"{COLUMNS[k][0]} / {COLUMNS[k][1]}" for k in missing))

    def s(k):  # cleaned string, or null
        if col[k] is None:
            return "cast(null as varchar)"
        return f"""nullif(nullif(trim(cast("{col[k]}" as varchar)), ''), 'NA')"""

    def num(k):
        return f"try_cast(replace({s(k)}, ',', '.') as double)"

    x = f"replace({s('v_raw')}, ',', '.')"
    if types[col["v_raw"]] in ("DOUBLE", "FLOAT"):  # a float prints as 7.2 / 100.0: drop the trailing zeros
        x = f"(case when strpos({x}, '.') > 0 then rtrim(rtrim({x}, '0'), '.') else {x} end)"
    flag = lambda k: (f"(case when lower({s(k)}) in ('1', 'true') then 1 when lower({s(k)}) in ('0', 'false') "  # noqa: E731
                      f"then 0 end)::utinyint")
    omop = f"try_cast({s('omop')} as bigint)"
    where = f"{omop} > 0" if omops is None else f"{omop} in ({', '.join(str(int(o)) for o in omops)})"
    return f"""
select {omop} omop, {s('sp')} sp, {s('test')} test,
       case when {s('f')} is not null then hash({s('f')}) end f,
       try_cast(substr({s('dt')}, 1, 4) as smallint) "year", try_cast(substr({s('dt')}, 6, 2) as tinyint) "month",
       (case lower({s('sex')}) when 'male' then 1 when 'm' then 1 when 'female' then 0 when 'f' then 0 end)::tinyint sex,
       try_cast({num('age')} as float) age, {num('v')} v, {num('v_raw')} v_raw, {s('unit')} unit,
       {"null::utinyint" if col["unit_pre_inj"] is None else f"({s('unit_pre_inj')} is null)::utinyint"} unit_missing,
       {flag('extracted')} extracted,
       (case when try_cast({x} as double) is null then null when strpos({x}, '.') > 0
             then length({x}) - strpos({x}, '.') else 0 end)::tinyint decimals,
       try_cast({s('qc_pass')} as tinyint) qc_pass, {s('note')} note, {s('outcome')} outcome,
       {num('ref_low')} ref_low, {num('ref_high')} ref_high, {s('ref_src')} ref_src, {s('org')} org,
       {s('h_unit')} h_unit
from read_parquet({sql_str(inp)})
where {where} and ({num('v')} is not null or {num('v_raw')} is not null)"""


def ensure_rows(omops, args, con):
    """The rows of `omops` (None: every OMOP) from --input, cached per OMOP under --cache/rows_<input name>/omop=<id>/.
    The OMOPs not cached yet are extracted together in one pass over the input (a partitioned write, no sort); the
    cache is dropped and rebuilt when the input file changes (size or mtime) or with --force. Returns the folder."""
    inp = Path(args.input).resolve()
    if not inp.is_file():
        raise SystemExit(f"{inp}: no such input file")
    root = args.cache / f"rows_{inp.stem}"
    meta_p = root / "input.json"
    st = inp.stat()
    want = {"version": BASE_VERSION, "input": str(inp), "input_size": st.st_size, "input_mtime": st.st_mtime}
    meta = json.loads(meta_p.read_text()) if meta_p.exists() else {}
    if args.force or {k: meta.get(k) for k in want} != want:
        if root.exists():
            log(f"Rows: {'--force' if args.force else 'the input changed'}: dropping the cached rows in {root}")
            shutil.rmtree(root)
        meta = dict(want, empty=[], all=False)
    root.mkdir(parents=True, exist_ok=True)
    have = {p.name.split("=", 1)[1] for p in root.glob("omop=*")}
    if omops is None:
        todo = None if not meta["all"] else []
    else:
        todo = [o for o in omops if o not in have and o not in meta["empty"] and not meta["all"]]
    if todo == []:
        log(f"Rows: cached ({root})")
        return root
    types = {r[0]: r[1] for r in con.sql(f"describe select * from read_parquet({sql_str(inp)})").fetchall()}
    part = root / ".part"
    shutil.rmtree(part, ignore_errors=True)
    log(f"Rows: one pass over {inp} for " + ("every OMOP" if todo is None else f"{len(todo)} OMOP(s): {', '.join(todo)}")
        + " ...")
    con.sql(f"copy ({extract_query(inp, types, todo)}) to {sql_str(part)} (format parquet, partition_by (omop), "
            f"compression zstd)")
    got = []
    for d in sorted(part.glob("omop=*")):
        o = d.name.split("=", 1)[1]
        shutil.rmtree(root / d.name, ignore_errors=True)
        os.replace(d, root / d.name)
        got.append(o)
    shutil.rmtree(part, ignore_errors=True)
    if todo is None:
        meta["all"] = True
    else:
        meta["empty"] = sorted(set(meta["empty"]) | (set(todo) - set(got)))
    meta_p.write_text(json.dumps(meta, indent=1) + "\n")
    log(f"Rows: {len(got)} OMOP(s) cached in {root}" + (f"; no rows for {', '.join(sorted(set(todo) - set(got)))}"
                                                         if todo and set(todo) - set(got) else ""))
    return root


def part_mtime(folder):
    files = list(Path(folder).glob("*.parquet"))
    return max(p.stat().st_mtime for p in files) if files else None


# ---------------------------------------------------------------------------
# 1-2 Build (duckdb, cached per OMOP)
# ---------------------------------------------------------------------------

TABLES = ["strata", "jump", "cells", "res", "rng", "dims", "orgs"]


def build(omop, args, con, rows):
    part = rows / f"omop={omop}"
    if not part.is_dir():
        log(f"OMOP {omop}: no rows in {Path(args.input).name}, skipped")
        return None
    cdir = args.cache / f"omop={omop}"
    meta_p = cdir / "meta.json"
    params = {"version": CACHE_VERSION, "fence_k": args.fence_k, "bin": BIN, "min_stratum": args.min_stratum}
    src_mtime = part_mtime(part)
    if meta_p.exists() and not args.force:
        meta = json.loads(meta_p.read_text())
        if meta.get("params") == params and meta.get("base") == str(part) and meta.get("base_mtime", 0) >= src_mtime \
                and all((cdir / f"{t}.parquet").exists() for t in TABLES):
            log(f"OMOP {omop}: cached ({cdir})")
            return {t: pd.read_parquet(cdir / f"{t}.parquet") for t in TABLES} | {"meta": meta}

    P = f"read_parquet({sql_str(str(part) + '/*.parquet')})"
    log(f"OMOP {omop}: aggregating {part} ...")
    con.sql(f"""create or replace temp view r as
        select sp k_sp, test k_test, f, year, month, coalesce(cast(sex as varchar), 'U') sx,
               case when age is null then -1 when age >= 90 then 90 else (floor(age / 10) * 10)::int end band,
               case when age is null then 'U' when age < 18 then 'minor' else 'adult' end ag,
               v, v_raw, case when v > 0 then log10(v) end lv,
               case when v > 0 and v_raw > 0 and ref_high > 0 then round(ref_high * v / v_raw, 6) end hi_h,
               case when v > 0 and v_raw > 0 and ref_low > 0 then round(ref_low * v / v_raw, 6) end lo_h,
               ref_low, ref_high, qc_pass, outcome, unit k_unit, note k_note, unit_missing, extracted, decimals, ref_src,
               org, h_unit
        from {P}""")

    # --- 1 baseline per sex x age band (small strata take the pooled values)
    h = con.sql(f"select sx, band, round(lv / {BIN})::int b, count(*) n from r where lv is not null group by all").df()
    if h.empty:
        log(f"OMOP {omop}: no harmonized values > 0, skipped")
        return None

    def stratum(d):
        b = d["b"].to_numpy() * BIN
        p25, p50, p75 = (wq_hist(b, d["n"], q) for q in (0.25, 0.5, 0.75))
        iqr = max(p75 - p25, 2 * BIN)
        flo, fhi = p25 - args.fence_k * iqr, p75 + args.fence_k * iqr
        n = d["n"].sum()
        out = d.loc[(b < flo) | (b > fhi), "n"].sum()
        return pd.Series({"med": p50, "flo": flo, "fhi": fhi, "p_out": out / n, "rows": n})

    pooled = stratum(h)
    st = h.groupby(["sx", "band"]).apply(stratum, include_groups=False).reset_index()
    small = st["rows"] < args.min_stratum
    st.loc[small, ["med", "flo", "fhi", "p_out"]] = pooled[["med", "flo", "fhi", "p_out"]].to_numpy()
    st["pooled"] = small
    con.register("st_df", st)
    con.sql("create or replace temp table st as select * from st_df")

    # --- persons: mean log10 value over the person's rows inside the stratum fences (so one gross error doesn't move
    # it), leaving the row itself out; national fences of the jumps
    con.sql("""create or replace temp table pers as
               select f, count(*) pn, sum(lv) ps from r join st using (sx, band)
               where lv between flo and fhi and f is not null group by f""")
    con.sql("""create or replace temp view rj as
               select x.*, case when x.lv is not null and x.n_oth >= 2
                                then x.lv - (x.ps - case when x.inside then x.lv else 0 end) / x.n_oth end jr
               from (select r.*, st.p_out, st.flo, st.fhi, coalesce(r.lv between st.flo and st.fhi, false) inside, p.ps,
                            coalesce(p.pn, 0) - case when coalesce(r.lv between st.flo and st.fhi, false) then 1 else 0 end
                            n_oth
                     from r join st using (sx, band) left join pers p using (f)) x""")
    jh = con.sql(f"""select round(greatest(least(jr, {RCLIP}), -{RCLIP}) / {BIN})::int b, count(*) n
                     from rj where jr is not null group by all""").df()
    if len(jh):
        b = jh["b"].to_numpy() * BIN
        q1, q3 = wq_hist(b, jh["n"], 0.25), wq_hist(b, jh["n"], 0.75)
        iqr = max(q3 - q1, 2 * BIN)
        jlo, jhi = q1 - args.fence_k * iqr, q3 + args.fence_k * iqr
        p_jump = jh.loc[(b < jlo) | (b > jhi), "n"].sum() / jh["n"].sum()
    else:
        jlo, jhi, p_jump = -RCLIP, RCLIP, 0.0
    jump = pd.DataFrame([{"jlo": jlo, "jhi": jhi, "p_jump": p_jump}])

    # --- 2 counts per SP x year x month
    oc_obs = ("case when upper(outcome) in ('H', 'HH') then 'H' when upper(outcome) in ('L', 'LL') then 'L' "
              "when upper(outcome) = 'N' then 'N' when upper(outcome) in ('A', 'AA') then 'A' end")
    oc_exp = ("case when v_raw is null then null when ref_high is not null and v_raw > ref_high then 'H' "
              "when ref_low is not null and v_raw < ref_low then 'L' "
              "when ref_high is not null or ref_low is not null then 'N' end")
    cells = con.sql(f"""
        with x as (select rj.*, {oc_obs} oo, {oc_exp} oe from rj)
        select k_sp, year, month, count(*) n, count(lv) n_v,
               sum(case when lv is not null then p_out end) exp_out,
               count(*) filter (where lv < flo or lv > fhi) obs_out,
               count(qc_pass) n_qc, count(*) filter (where qc_pass = 0) qc_fail,
               count(jr) n_j, count(*) filter (where jr < {jlo} or jr > {jhi}) obs_jump,
               count(*) filter (where oo is not null and oe is not null) n_oc,
               count(*) filter (where oo = 'N' and oe in ('H', 'L')) oc_said_normal,
               count(*) filter (where oo in ('H', 'L', 'A') and oe = 'N') oc_said_abnormal,
               count(*) filter (where (oo = 'H' and oe = 'L') or (oo = 'L' and oe = 'H')) oc_flipped,
               count(*) filter (where ref_low is not null or ref_high is not null) n_ref
        from x group by all""").df()
    res = con.sql(f"""
        select k_sp, year, month, round(greatest(least(lv - st.med, {RCLIP}), -{RCLIP}) / {BIN})::smallint rb, count(*) n
        from r join st using (sx, band) where lv is not null group by all""").df()
    rng = con.sql("""select k_sp, year, month, sx, ag, hi_h, lo_h, count(*) n from r
                     where hi_h is not null or lo_h is not null group by all""").df()
    dims = con.sql(" union all ".join(
        f"select k_sp, year, month, '{d}' dim, {e} cat, count(*) n from r group by all" for d, e in DIMS.items())).df()

    orgs = con.sql("select k_sp, org, count(*) n from r where org is not null group by all").df()
    hu = con.sql("select h_unit, count(*) n from r where h_unit is not null group by all order by n desc limit 1").fetchall()
    cdir.mkdir(parents=True, exist_ok=True)
    out = {"strata": st, "jump": jump, "cells": cells, "res": res, "rng": rng, "dims": dims, "orgs": orgs}
    for k, df in out.items():
        df.to_parquet(cdir / f"{k}.parquet", index=False)
    meta = {"omop": omop, "params": params, "base_mtime": src_mtime, "base": str(part), "h_unit": hu[0][0] if hu else None,
            "created": time.strftime("%Y-%m-%dT%H:%M:%S"), "pooled": pooled.to_dict()}
    meta_p.write_text(json.dumps(meta, indent=1, default=float) + "\n")
    log(f"OMOP {omop}: {int(cells['n'].sum())} rows -> {cdir} ({len(cells)} SP-months, {len(res)} residual bins)")
    return out | {"meta": meta}


# ---------------------------------------------------------------------------
# 3-4 Flags
# ---------------------------------------------------------------------------

def add_t(df):
    df = df[df["year"].notna() & df["month"].notna()].copy()
    df["t"] = df["year"].astype(int) * 12 + df["month"].astype(int) - 1
    return df


def merge_years(rows, key_cols):
    """Merge consecutive-year flags of the same SP (and key) into one flag."""
    if not rows:
        return []
    df = pd.DataFrame(rows).sort_values(key_cols + ["year"])
    out = []
    for _, g in df.groupby(key_cols, sort=False):
        run = [g.iloc[0]]
        for r in list(g.itertuples(index=False))[1:]:
            if r.year == run[-1].year + 1 and np.sign(r.effect) == np.sign(run[-1].effect):
                run.append(pd.Series(r._asdict()))
            else:
                out.append(run)
                run = [pd.Series(r._asdict())]
        out.append(run)
    merged = []
    for run in out:
        rows_ = sum(r["rows"] for r in run)
        first, last = run[0], run[-1]
        extra = {}
        if isinstance(first.get("tmpl"), str):  # counts: totals over the run
            ev, ex = sum(r["events"] for r in run), sum(r["expected"] for r in run)
            eff = math.log10(ev / ex) if ex > 0 else first["effect"]
            extra["detail"] = first["tmpl"].format(events=int(ev), oe=ev / ex if ex > 0 else float("nan"),
                                                   rate=100 * ev / rows_)
        else:
            eff = sum(r["effect"] * r["rows"] for r in run) / rows_
        merged.append({**first.to_dict(), **extra, "rows": int(rows_), "effect": eff,
                       "start": int(first["year"]) * 12, "end": int(last["year"]) * 12 + 11,
                       "period": str(int(first["year"])) if len(run) == 1 else f"{int(first['year'])}..{int(last['year'])}"})
    return merged


def detect(omop, data, args, labels):
    cells, res, rng, dims, st = (data[k] for k in ("cells", "res", "rng", "dims", "strata"))
    p_jump = float(data["jump"]["p_jump"].iloc[0])
    flags = []
    lshift, lstep, lrange = math.log10(args.shift), math.log10(args.step), math.log10(args.range_margin)

    # --- per SP x year table
    cy = cells[cells["year"].notna()].groupby(["k_sp", "year"], as_index=False).sum(numeric_only=True)
    cy = cy.drop(columns=[c for c in ("month",) if c in cy.columns])
    sh = group_median(res[res["year"].notna()], ["k_sp", "year"], "rb")
    sh["shift"] = sh["med"] * BIN
    cy = cy.merge(sh[["k_sp", "year", "shift"]], on=["k_sp", "year"], how="left")
    nat_qc = cells["qc_fail"].sum() / max(cells["n_qc"].sum(), 1)
    nat_oc = (cells[["oc_said_normal", "oc_said_abnormal", "oc_flipped"]].sum().sum()) / max(cells["n_oc"].sum(), 1)
    cy["oe_out"] = cy["obs_out"] / cy["exp_out"].where(cy["exp_out"] > 0)
    cy["exp_jump"] = cy["n_j"] * p_jump
    cy["oe_jump"] = cy["obs_jump"] / cy["exp_jump"].where(cy["exp_jump"] > 0)
    cy["qc_rate"] = cy["qc_fail"] / cy["n_qc"].where(cy["n_qc"] > 0)
    cy["oc_mis"] = cy[["oc_said_normal", "oc_said_abnormal", "oc_flipped"]].sum(axis=1)
    cy["oc_rate"] = cy["oc_mis"] / cy["n_oc"].where(cy["n_oc"] > 0)
    up = group_mode(rng[rng["lhi"].notna() & rng["year"].notna()], ["k_sp", "year"], "lhi")
    cy = cy.merge(up[["k_sp", "year", "mode"]].rename(columns={"mode": "lhi_mode"}), on=["k_sp", "year"], how="left")

    def base_flag(kind, sp, year, rows, effect, detail, events=None, expected=None, tmpl=None):
        return {"omop": omop, "kind": kind, "k_sp": sp, "year": int(year), "rows": int(rows), "effect": effect,
                "detail": detail, "events": events, "expected": expected, "tmpl": tmpl}

    def count_flag(kind, r, rows, events, expected, tmpl):
        oe = events / expected
        return base_flag(kind, r.k_sp, r.year, rows, math.log10(oe),
                         tmpl.format(events=int(events), oe=oe, rate=100 * events / rows), events, expected, tmpl)

    # shift
    rows = [base_flag("shift", r.k_sp, r.year, r.n_v, r.shift, "median vs national (same sex x age)")
            for r in cy.itertuples() if r.n_v >= args.min_cell and pd.notna(r.shift) and abs(r.shift) >= lshift]
    flags += merge_years(rows, ["k_sp", "kind"])
    # outliers, person jumps, qc fails, outcome
    rows = [count_flag("outliers", r, r.n_v, r.obs_out, r.exp_out,
                       "{events} outside the national fences (same sex x age), {oe:.1f}x expected")
            for r in cy.itertuples() if r.obs_out >= args.min_events and pd.notna(r.oe_out) and r.oe_out >= args.oe]
    flags += merge_years(rows, ["k_sp", "kind"])
    rows = [count_flag("person_jump", r, r.n_j, r.obs_jump, r.exp_jump,
                       "{events} values far from the same person's other values, {oe:.1f}x expected")
            for r in cy.itertuples() if r.obs_jump >= args.min_events and pd.notna(r.oe_jump) and r.oe_jump >= args.oe]
    flags += merge_years(rows, ["k_sp", "kind"])
    if nat_qc >= 0:
        rows = [count_flag("qc_fail", r, r.n_qc, r.qc_fail, r.n_qc * max(nat_qc, 1e-4),
                           "QC fail {rate:.1f}% vs " + f"{100 * nat_qc:.2f}% national")
                for r in cy.itertuples() if r.qc_fail >= args.min_events and pd.notna(r.qc_rate)
                and r.qc_rate >= args.oe * nat_qc and r.qc_rate - nat_qc >= 0.01]
        flags += merge_years(rows, ["k_sp", "kind"])
    rows = []
    for r in cy.itertuples():
        if r.oc_mis >= args.min_events and pd.notna(r.oc_rate) and r.oc_rate >= args.outcome_rate \
                and r.oc_rate >= args.oe * nat_oc:
            parts = [(r.oc_said_normal, "N where the range says H/L"), (r.oc_said_abnormal, "H/L where the range says N"),
                     (r.oc_flipped, "H/L the wrong way")]
            main = max(parts)[1]
            rows.append(count_flag("outcome", r, r.n_oc, r.oc_mis, r.n_oc * max(nat_oc, 1e-4),
                                   "TEST_OUTCOME vs own range: {rate:.0f}% disagree " +
                                   f"(national {100 * nat_oc:.1f}%), mostly {main}"))
    flags += merge_years(rows, ["k_sp", "kind"])

    # --- monthly series: steps and episodes
    rm = add_t(res)
    cm = add_t(cells)
    mm = group_median(rm, ["k_sp", "t"], "rb")
    mm["m"] = mm["med"] * BIN
    mm = mm.merge(cm.groupby(["k_sp", "t"], as_index=False)[["obs_out", "exp_out"]].sum(), on=["k_sp", "t"], how="left")
    mm = mm[mm["rows"] >= args.min_month]
    rng_t = add_t(rng)
    series = {}
    for sp, s in mm.groupby("k_sp"):
        s = s.sort_values("t").reset_index(drop=True)
        # runs of months with data (a new run after more than MAX_GAP missing months); steps and episodes only on
        # runs of >= --min-seg months, so that a few scattered early months don't make a "step" at the real start
        run = np.concatenate([[0], np.cumsum(np.diff(s["t"].to_numpy()) > MAX_GAP + 1)])
        size = pd.Series(run).map(pd.Series(run).value_counts()).to_numpy()
        s = s.assign(run=run, long=size >= args.min_seg)
        series[sp] = s
        s = s[s["long"]].reset_index(drop=True)
        x = s["m"].to_numpy()
        n = len(x)
        if n == 0:
            continue
        run = s["run"].to_numpy()
        # month-to-month noise of the SP's monthly medians, within runs (robust to one step)
        dif = np.diff(x)[np.diff(run) == 0]
        noise = 1.4826 * np.median(np.abs(dif)) / math.sqrt(2) if len(dif) > 1 else 0
        noise = max(noise, BIN / 2)
        # step: the break that best splits the series (difference of means, weighted by segment sizes), its size as
        # the difference of the two segments' medians
        step = None
        if n >= 2 * args.min_seg:
            cs = np.concatenate([[0], np.cumsum(x)])
            best = None
            for k in range(args.min_seg, n - args.min_seg + 1):
                dm = (cs[n] - cs[k]) / (n - k) - cs[k] / k
                w = abs(dm) * math.sqrt(k * (n - k) / n)
                if best is None or w > best[0]:
                    best = (w, k)
            k = best[1]
            d = np.median(x[k:]) - np.median(x[:k])
            tail = np.median(x[-args.min_seg:]) - np.median(x[:k])
            if abs(d) >= lstep and abs(d) / noise >= args.step_z and abs(tail) >= abs(d) / 2:
                tb = int(s["t"].iloc[k])
                step = (k, tb, d)
                # did the reference limit change at the same time? (the SP's main limit and sex x age group)
                r_sp = rng_t[rng_t["k_sp"] == sp]
                detail = f"median {fx(d)} from {ym(tb)} ({abs(d) / noise:.0f} noise units)"
                if run[k] != run[k - 1]:
                    detail += (f"; after a gap in the data (no month with >= {args.min_month} rows between "
                               f"{ym(s['t'].iloc[k - 1])} and {ym(tb)}): a new system, or a data problem?")
                if len(r_sp):
                    rs, col, lim, grp = main_limit(r_sp)
                    before = rs[(rs["t"] < tb) & (rs["t"] >= tb - 12)]
                    after = rs[(rs["t"] >= tb) & (rs["t"] < tb + 12)]
                    if len(before) and len(after):
                        hb = before.groupby(col)["n"].sum().idxmax()
                        ha = after.groupby(col)["n"].sum().idxmax()
                        if abs(ha - hb) >= math.log10(args.range_step):
                            detail += (f"; {lim} limit changed too ({grp}: {fnum(10 ** hb)} -> {fnum(10 ** ha)}): "
                                       f"method change?")
                        else:
                            detail += f"; {lim} limit unchanged ({grp}: {fnum(10 ** hb)}): drift or data problem?"
                flags.append({"omop": omop, "kind": "step", "k_sp": sp, "rows": int(s["rows"].iloc[k:].sum()),
                              "effect": d, "start": tb, "end": int(s["t"].iloc[-1]), "period": f"from {ym(tb)}",
                              "detail": detail})
        # episodes: months off the SP's own baseline (per segment when there is a step) by >= x--shift and >=
        # --episode-z noise units, or with an outlier burst, that return to the baseline
        basem = np.full(n, np.median(x))
        if step is not None:
            basem[:step[0]], basem[step[0]:] = np.median(x[:step[0]]), np.median(x[step[0]:])
        oe = (s["obs_out"] / s["exp_out"].where(s["exp_out"] > 0)).to_numpy()
        odd = (np.abs(x - basem) >= max(lshift, args.episode_z * noise)) | (
            (s["obs_out"].to_numpy() >= args.min_events) & (np.nan_to_num(oe) >= args.oe))
        ts = s["t"].to_numpy()
        i = 0
        while i < n:
            if not odd[i]:
                i += 1
                continue
            j = i
            while j + 1 < n and odd[j + 1] and ts[j + 1] - ts[j] <= 2:
                j += 1
            length = ts[j] - ts[i] + 1
            ends_series = j >= n - 2
            covered_by_step = step is not None and abs(ts[i] - step[1]) <= 3 and ends_series
            if length >= args.episode_months and not ends_series and not covered_by_step:
                seg = s.iloc[i:j + 1]
                d = np.median(x[i:j + 1] - basem[i:j + 1])
                o, e = seg["obs_out"].sum(), seg["exp_out"].sum()
                detail = f"median {fx(d)} vs the SP's own baseline"
                if e > 0 and o >= args.min_events:
                    detail += f"; outliers {o / e:.1f}x expected"
                flags.append({"omop": omop, "kind": "episode", "k_sp": sp, "rows": int(seg["rows"].sum()), "effect": d,
                              "start": int(ts[i]), "end": int(ts[j]), "period": f"{ym(ts[i])}..{ym(ts[j])}",
                              "detail": detail})
            i = j + 1

    # --- ranges: SP typical limit vs national, per sex x minor/adult; and changes between years
    range_rows = []
    nat = {}
    for lim in ("lhi", "llo"):
        rr = rng[rng[lim].notna()]
        if rr.empty:
            continue
        natm = group_mode(rr, ["sx", "ag"], lim)
        spm = group_mode(rr, ["k_sp", "sx", "ag"], lim)
        spm = spm.merge(natm[["sx", "ag", "mode", "rows"]].rename(columns={"mode": "nat", "rows": "nat_rows"}),
                        on=["sx", "ag"])
        for r in natm.itertuples():
            g = rr[(rr["sx"] == r.sx) & (rr["ag"] == r.ag)]
            within = g.loc[(g[lim] - r.mode).abs() <= math.log10(1.1), "n"].sum() / g["n"].sum()
            nat[(lim, r.sx, r.ag)] = (r.mode, r.rows, within)
        for r in spm.itertuples():
            range_rows.append({"limit": "upper" if lim == "lhi" else "lower", "sex": sex_label(r.sx), "age": r.ag,
                               "k_sp": r.k_sp, "rows": int(r.rows), "sp_typical": 10 ** r.mode,
                               "sp_share_at_typical": r.mode_share, "national_typical": 10 ** r.nat,
                               "x_vs_national": r.mode - r.nat})
        # adults only: children's limits change with age within "minor", so its typical limit mixes ages
        dev = spm[(spm["ag"] == "adult") & (spm["rows"] >= args.min_cell) & ((spm["mode"] - spm["nat"]).abs() >= lrange)]
        for sp, g in dev.groupby("k_sp"):  # one flag per SP and limit, its sex x age groups listed
            g = g.sort_values("rows", ascending=False)
            groups = "; ".join(f"{sex_label(r.sx)} {r.ag} {fnum(10 ** r.mode)} vs {fnum(10 ** r.nat)}"
                               for r in g.itertuples())
            flags.append({"omop": omop, "kind": "range", "k_sp": sp, "rows": int(g["rows"].sum()),
                          "effect": float(np.average(g["mode"] - g["nat"], weights=g["rows"])), "start": None,
                          "end": None, "period": "all years",
                          "detail": f"{'upper' if lim == 'lhi' else 'lower'} limit vs national: {groups}"})
    # range changes between consecutive years, in the SP's main sex x age group
    rc = []
    ry = rng[rng["year"].notna()]
    for sp, g in ry.groupby("k_sp"):
        gm, col, lim, grp = main_limit(g)
        yr = group_mode(gm, ["year"], col)
        yr = yr[yr["rows"] >= args.min_cell].sort_values("year")
        for a, b in zip(yr.itertuples(), list(yr.itertuples())[1:]):
            if b.year == a.year + 1 and abs(b.mode - a.mode) >= math.log10(args.range_step):
                rc.append({"omop": omop, "kind": "range_change", "k_sp": sp, "rows": int(b.rows),
                           "effect": b.mode - a.mode, "start": int(b.year) * 12, "end": int(b.year) * 12 + 11,
                           "period": f"{int(a.year)}->{int(b.year)}",
                           "detail": f"typical {lim} limit {fnum(10 ** a.mode)} -> {fnum(10 ** b.mode)} ({grp})"})
    flags += rc

    # --- causes
    dt = add_t(dims)
    for fl in flags:
        fl["cause"] = cause(fl, dt, dims, labels, args)
    fdf = pd.DataFrame(flags)
    if len(fdf):
        fdf["sp"] = fdf["k_sp"].map(labels.sp)
        fdf["effect_x"] = fdf.apply(lambda r: fx(r["effect"]) if r["kind"] in ("shift", "step", "episode", "range",
                                                                              "range_change") else
                                    f"{10 ** r['effect']:.1f}x", axis=1)
        fdf["score"] = (fdf["effect"].abs() * np.log10(fdf["rows"].clip(lower=1) + 1)).round(3)
        fdf = fdf.sort_values("score", ascending=False)
        fdf = fdf[["omop", "kind", "sp", "period", "rows", "effect_x", "detail", "cause", "score", "k_sp", "start",
                   "end"]]
    else:
        fdf = pd.DataFrame(columns=["omop", "kind", "sp", "period", "rows", "effect_x", "detail", "cause", "score",
                                    "k_sp", "start", "end"])
    cy["sp"] = cy["k_sp"].map(labels.sp)
    ranges = pd.DataFrame(range_rows)
    if len(ranges):
        ranges["sp"] = ranges["k_sp"].map(labels.sp)
    return fdf, cy, ranges, nat, series


def cause(fl, dt, dims, labels, args):
    """The category whose share changed most between the flagged period and the SP's other periods."""
    sp = fl["k_sp"]
    d = dims[dims["k_sp"] == sp]
    if fl.get("start") is not None and pd.notna(fl.get("start")):
        dsp = dt[dt["k_sp"] == sp]
        inside = (dsp["t"] >= fl["start"]) & (dsp["t"] <= fl["end"])
        a, b = dsp[inside], dsp[~inside]
        ref = "rest of the SP's data"
        if not len(b) or b["n"].sum() < args.min_cell * len(DIMS):
            a, b, ref = d, dims[dims["k_sp"] != sp], "other SPs"
    else:  # no period (range): this SP vs the others
        a, b, ref = d, dims[dims["k_sp"] != sp], "other SPs"
    best = None
    for dim in DIMS:
        sa = a[a["dim"] == dim].groupby("cat", dropna=False)["n"].sum()
        sb = b[b["dim"] == dim].groupby("cat", dropna=False)["n"].sum()
        if sa.sum() < args.min_n or sb.sum() < args.min_n:
            continue
        pa, pb = sa / sa.sum(), sb / sb.sum()
        diff = pa.subtract(pb, fill_value=0)
        cat = diff.abs().idxmax()
        if best is None or abs(diff[cat]) > abs(best[2]):
            best = (dim, cat, diff[cat], pb.get(cat, 0.0), pa.get(cat, 0.0))
    if best is None or abs(best[2]) < 0.1:
        return ""
    dim, cat, _, before, now = best
    name = labels.name(dim, cat) if dim in ("test", "unit", "note") else ("NA" if pd.isna(cat) else str(cat))
    return f"{dim} {name}: {100 * before:.0f}% ({ref}) -> {100 * now:.0f}%"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def titled(ax, title, question):
    ax.set_title(title, fontsize=10, color=INK, loc="left", pad=19)
    ax.text(0, 1.015, question, transform=ax.transAxes, fontsize=8.5, color=MUTED, style="italic", ha="left",
            va="bottom")


def style(ax):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(LINE)
    ax.tick_params(colors=MUTED, labelsize=7.5)


def heatmap(ax, cy, args):
    top = cy.groupby("k_sp")["n_v"].sum().sort_values(ascending=False).head(args.top_sp).index
    d = cy[cy["k_sp"].isin(top)]
    years = sorted(int(y) for y in d["year"].unique())
    order = list(top)
    grid = np.full((len(order), len(years)), np.nan)
    for r in d.itertuples():
        if r.n_v >= args.min_cell and pd.notna(r.shift):
            grid[order.index(r.k_sp), years.index(int(r.year))] = r.shift
    lim = math.log10(1.3)
    cmap = LinearSegmentedColormap.from_list("div", [NEG, "#e8e6df", POS])
    cmap.set_bad(SURFACE)
    im = ax.imshow(np.clip(grid, -lim, lim), cmap=cmap, vmin=-lim, vmax=lim, aspect="auto", interpolation="nearest")
    ax.set_yticks(range(len(order)))
    ax.set_yticklabels([cy.loc[cy["k_sp"] == k, "sp"].iloc[0] for k in order], fontsize=7)
    ax.set_xticks(range(len(years)))
    ax.set_xticklabels(years, fontsize=7.5)
    for i in range(len(order)):
        for j in range(len(years)):
            if np.isfinite(grid[i, j]) and abs(grid[i, j]) >= math.log10(args.shift):
                ax.text(j, i, fx(grid[i, j]), ha="center", va="center", fontsize=6.5, color=INK)
    cb = plt.colorbar(im, ax=ax, fraction=0.02, pad=0.01)
    ticks = [-lim, -math.log10(1.1), 0, math.log10(1.1), lim]
    cb.set_ticks(ticks)
    cb.set_ticklabels([fx(t) for t in ticks])
    cb.ax.tick_params(labelsize=7, colors=MUTED)
    cb.outline.set_edgecolor(LINE)
    for s in ax.spines.values():
        s.set_visible(False)
    ax.tick_params(colors=MUTED, length=0)
    titled(ax, f"Median vs national per SP x year (largest {len(order)} SPs; blank: under {args.min_cell} rows; "
               f"labelled when >= {fx(math.log10(args.shift))})",
           "Which labs sit above or below the national values, and since when?")


def series_panel(fig, spec, sp, s, flags_sp, rng_t, label, color, args):
    sub = spec.subgridspec(2, 1, height_ratios=[2.2, 1], hspace=0.08)
    ax = fig.add_subplot(sub[0])
    ax2 = fig.add_subplot(sub[1], sharex=ax)
    s = s.reset_index(drop=True)
    dates = pd.to_datetime({"year": s["t"] // 12, "month": s["t"] % 12 + 1, "day": 1})
    long = s["long"].to_numpy() if "long" in s else np.ones(len(s), bool)
    ax.axhspan(-math.log10(args.shift), math.log10(args.shift), color=MUTED, alpha=0.07, lw=0)
    ax.axhline(0, color=LINE, lw=1)
    # one line per run of months (broken at gaps); months outside the runs used for steps drawn faint
    for _, g in s.groupby(s["run"] if "run" in s else pd.Series(0, index=s.index)):
        ax.plot(dates[g.index], g["m"], color=color, lw=1.6, marker="o", ms=2.5,
                alpha=1.0 if long[g.index[0]] else 0.3)
    for fl in flags_sp.itertuples():
        if fl.kind == "episode":
            a = pd.Timestamp(year=int(fl.start) // 12, month=int(fl.start) % 12 + 1, day=1)
            b = pd.Timestamp(year=int(fl.end) // 12, month=int(fl.end) % 12 + 1, day=28)
            ax.axvspan(a, b, color=KIND_COLOR["episode"], alpha=0.15, lw=0)
        if fl.kind == "step":
            a = pd.Timestamp(year=int(fl.start) // 12, month=int(fl.start) % 12 + 1, day=1)
            for x in (ax, ax2):
                x.axvline(a, color=KIND_COLOR["step"], lw=1.2, ls="--")
    yt = [v for v in (-0.2, -0.1, -0.05, 0, 0.05, 0.1, 0.2)]
    ax.set_yticks(yt)
    ax.set_yticklabels([fx(v) for v in yt])
    m = s.loc[long, "m"] if long.any() else s["m"]
    lo, hi = min(np.nanmin(m), -0.06) - 0.02, max(np.nanmax(m), 0.06) + 0.02
    ax.set_ylim(lo, hi)
    off = s[(s["m"] < lo) | (s["m"] > hi)]  # off the scale: a marker at the edge
    if len(off):
        ax.scatter(dates[off.index], off["m"].clip(lo, hi), marker="^", s=18, color=color, alpha=0.4,
                   clip_on=False, zorder=3)
    ax.set_ylabel("median vs\nnational", fontsize=7.5, color=MUTED)
    kinds = ", ".join(sorted(set(flags_sp["kind"])))
    titled(ax, f"{label}: monthly median vs national ({kinds})",
           "Did the lab's values change, and did its reference range change with them?")
    plt.setp(ax.get_xticklabels(), visible=False)
    style(ax)
    r = rng_t[rng_t["k_sp"] == sp]
    rm = pd.DataFrame()
    if len(r):
        rs, col, lim, grp = main_limit(r)
        rm = group_mode(rs, ["t"], col)
        rm = rm[rm["rows"] >= args.min_n].sort_values("t")
        if len(rm):
            d2 = pd.to_datetime({"year": rm["t"] // 12, "month": rm["t"] % 12 + 1, "day": 1})
            ax2.step(d2, 10 ** rm["mode"], where="post", color=MUTED, lw=1.4)
            ax2.set_ylabel(f"{lim} limit\n({grp})", fontsize=7.5, color=MUTED)
            vals = 10 ** rm["mode"]
            pad = max((vals.max() - vals.min()) * 0.3, vals.max() * 0.05)
            ax2.set_ylim(vals.min() - pad, vals.max() + pad)
    if not len(rm):
        ax2.text(0.5, 0.5, "no reference limits", transform=ax2.transAxes, ha="center", fontsize=7.5, color=MUTED)
        ax2.set_yticks([])
    ax2.set_xlabel(f"month (shaded band: within x{args.shift:g}; dashed: step; orange: episode; faint: months "
                   f"outside a run of >= {args.min_seg}, not used for steps; triangles: off the scale)", fontsize=7.5,
                   color=MUTED)
    style(ax2)


def fmt_range(lo, hi):
    if pd.notna(lo) and pd.notna(hi):
        return f"{fnum(lo)}-{fnum(hi)}"
    return f"<{fnum(hi)}" if pd.notna(hi) else (f">{fnum(lo)}" if pd.notna(lo) else "")


def range_panel(ax, ranges, src, h_unit, args):
    """Typical limit per SP vs national (the upper limit, or the lower one when the concept has (almost) no upper),
    with the actual ranges written out: national per sex x age in the title, each SP's on the right with the share of
    its limits from the curated text table."""
    limit = "upper"
    if len(ranges):
        by = ranges.groupby("limit")["rows"].sum()
        limit = "upper" if by.get("upper", 0) >= by.get("lower", 0) else "lower"
    up = ranges[(ranges["limit"] == limit) & (ranges["rows"] >= args.min_cell)] if len(ranges) else ranges
    if up is None or not len(up):
        ax.text(0.5, 0.5, "no reference limits", transform=ax.transAxes, ha="center", color=MUTED)
        ax.axis("off")
        return
    unit = f" {h_unit}" if h_unit else ""
    ok = ranges[ranges["rows"] >= args.min_cell]
    # typical (lower, upper) per SP x sex x age, and national
    sp_rng = ok.pivot_table(index=["k_sp", "sex", "age"], columns="limit", values="sp_typical", aggfunc="first")
    nat_rng = ok.groupby(["sex", "age", "limit"])["national_typical"].first().unstack("limit")
    groups = up.groupby(["sex", "age"])["rows"].sum().sort_values(ascending=False).head(2).index.tolist()
    sps = up.groupby("k_sp")["rows"].sum().sort_values(ascending=False).head(args.top_sp).index.tolist()
    lr = math.log10(args.range_margin)
    ax.axvspan(-lr, lr, color=MUTED, alpha=0.07, lw=0)
    ax.axvline(0, color=LINE, lw=1)
    for gi, (sx, ag) in enumerate(groups):
        g = up[(up["sex"] == sx) & (up["age"] == ag) & up["k_sp"].isin(sps)]
        y = [sps.index(k) + (gi - 0.5) * 0.25 for k in g["k_sp"]]
        nr = nat_rng.loc[(sx, ag)] if (sx, ag) in nat_rng.index else pd.Series(dtype=float)
        ax.scatter(g["x_vs_national"].clip(-1, 1), y, s=28, color=SERIES[gi], edgecolor=SURFACE, lw=1, zorder=3,
                   label=f"{sx} {ag}: national {fmt_range(nr.get('lower'), nr.get('upper'))}{unit}")
    ax.set_yticks(range(len(sps)))
    ax.set_yticklabels([up.loc[up["k_sp"] == k, "sp"].iloc[0] for k in sps], fontsize=7)
    ax.set_ylim(len(sps) - 0.5, -0.5)
    # right: each SP's own typical ranges in the two main groups, and where its limits come from
    right = ax.twinx()
    right.set_ylim(ax.get_ylim())
    right.set_yticks(range(len(sps)))
    labels = []
    for k in sps:
        parts = []
        for sx, ag in groups:
            if (k, sx, ag) in sp_rng.index:
                r = sp_rng.loc[(k, sx, ag)]
                parts.append(f"{sx} {fmt_range(r.get('lower'), r.get('upper'))}")
        txt = src.get(k)
        labels.append("  ".join(parts) + (f"   text {txt:.0f}%" if txt is not None and np.isfinite(txt) else ""))
    right.set_yticklabels(labels, fontsize=7, color=INK)
    right.tick_params(length=0)
    for side in ("top", "right", "left", "bottom"):
        right.spines[side].set_visible(False)
    ticks = [-math.log10(4), -math.log10(2), 0, math.log10(2), math.log10(4)]
    ax.set_xticks(ticks)
    ax.set_xticklabels([fx(t) for t in ticks])
    ax.set_xlim(-0.8, 0.8)
    ax.set_xlabel(f"SP's typical {limit} limit / national (shaded: within x{args.range_margin:g}); right: the SP's "
                  f"typical ranges{unit}, and the share of its limits from the curated text table", fontsize=7.5,
                  color=MUTED)
    ax.legend(fontsize=7, loc="lower left", frameon=True, facecolor=SURFACE, edgecolor=LINE)
    ax.grid(axis="x", color=LINE, lw=0.5)
    style(ax)
    # national ranges per sex x age, and where the limits come from overall
    nat_txt = "   ".join(f"{sx} {ag} {fmt_range(r.get('lower'), r.get('upper'))}"
                         for (sx, ag), r in nat_rng.iterrows())
    tot = src.get("__all__")
    src_txt = (f"; limits from the curated text table for {tot:.0f}% of rows, sent by the lab for {100 - tot:.0f}%"
               if tot is not None and np.isfinite(tot) else "")
    titled(ax, f"Reference ranges per SP ({h_unit or 'harmonized unit'})",
           f"Which labs use a different reference range?   National typical: {nat_txt}{src_txt}")


def clip(text, n):
    """Cut text at a word boundary, with an ellipsis."""
    text = str(text)
    return text if len(text) <= n else text[:n].rsplit(" ", 1)[0] + " …"


def flag_table(ax, fdf, args):
    ax.axis("off")
    rows = fdf.head(args.table_rows)
    if not len(rows):
        ax.text(0.5, 0.5, "no flags", ha="center", va="center", transform=ax.transAxes, color=MUTED)
        return
    cells = [[r.kind, r.sp, r.period, f"{r.rows}", r.effect_x, clip(r.detail, 115), clip(r.cause or "", 70)]
             for r in rows.itertuples()]
    cols = ["kind", "SP", "period", "rows", "effect", "detail", "likely cause"]
    tb = ax.table(cellText=cells, colLabels=cols, loc="upper center", cellLoc="left", colLoc="left")
    tb.auto_set_font_size(False)
    tb.set_fontsize(7.5)
    tb.auto_set_column_width(list(range(len(cols))))
    tb.scale(1, 1.4)
    for (r, c), cell in tb.get_celld().items():
        cell.set_edgecolor(LINE)
        cell.set_facecolor(SURFACE)
        cell.get_text().set_color(MUTED if r == 0 else INK)
        if r > 0 and c == 0:
            cell.set_facecolor(KIND_COLOR.get(cells[r - 1][0], LINE) + "40")
    kinds = fdf["kind"].value_counts()
    counts = ", ".join(f"{k} {kinds[k]}" for k in KINDS if k in kinds.index)
    titled(ax, f"Flags: {len(fdf)} ({counts}); the largest {len(cells)} by score = |log10 effect| x log10 rows",
           "What should be checked with the lab, and what probably caused it?")


def ref_src_share(dims):
    """% of the limits that come from the curated text table, per SP and overall ("__all__")."""
    d = dims[(dims["dim"] == "ref_src") & dims["cat"].notna() & (dims["cat"] != "NA")]
    if d.empty:
        return {}
    by = d.groupby(["k_sp", "cat"])["n"].sum().unstack("cat", fill_value=0)
    text = by.get("text", pd.Series(0, index=by.index))
    out = (100 * text / by.sum(axis=1)).where(by.sum(axis=1) >= 1).to_dict()
    out["__all__"] = 100 * text.sum() / by.values.sum()
    return out


def report(omop, title, fdf, cy, ranges, nat, series, rng, args, path, dims=None, h_unit=None):
    flagged_sps = [k for k in fdf.loc[fdf["kind"].isin(["step", "episode", "shift"]), "k_sp"].drop_duplicates()
                   if k in series][:4]
    n_tab = max(min(len(fdf), args.table_rows), 1)
    top_n = min(cy["k_sp"].nunique(), args.top_sp)
    n_series = math.ceil(len(flagged_sps) / 2)
    heights = [0.9 + 0.22 * top_n] + [3.6] * n_series + [max(0.9 + 0.22 * top_n, 2.5), 0.9 + 0.26 * n_tab]
    H = sum(heights) + 1.2 + 0.6 * len(heights)
    fig = plt.figure(figsize=(20, H), facecolor=SURFACE)
    gs = fig.add_gridspec(len(heights), 2, height_ratios=heights, hspace=0.6, wspace=0.25, left=0.16, right=0.97,
                          top=1 - 1.0 / H, bottom=0.2 / H)
    heatmap(fig.add_subplot(gs[0, :]), cy, args)
    rng_t = add_t(rng)
    for i, sp in enumerate(flagged_sps):
        series_panel(fig, gs[1 + i // 2, i % 2], sp, series[sp], fdf[fdf["k_sp"] == sp], rng_t,
                     cy.loc[cy["k_sp"] == sp, "sp"].iloc[0], SERIES[i], args)
    # full width, with the SPs' ranges written out on the right
    rgs = gs[1 + n_series, :].subgridspec(1, 2, width_ratios=[1.6, 1])
    range_panel(fig.add_subplot(rgs[0]), ranges, ref_src_share(dims) if dims is not None else {}, h_unit, args)
    flag_table(fig.add_subplot(gs[2 + n_series, :]), fdf, args)
    fig.suptitle(title, fontsize=13, fontweight="bold", color=INK, x=0.01, y=1 - 0.25 / H, ha="left")
    fig.savefig(path, dpi=120, facecolor=SURFACE)
    plt.close(fig)
    log(f"Wrote {path}")


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

def mask(df, ncol, stat_cols, min_n):
    df = df.copy()
    small = df[ncol] < min_n
    for c in stat_cols:
        df[c] = df[c].astype(object)
        df.loc[small, c] = ""
    df[ncol] = df[ncol].astype(object)
    df.loc[small, ncol] = BELOW
    return df


def write_outputs(omop, fdf, cy, ranges, args):
    stem = args.out / f"sp_qc_{omop}"
    fdf.drop(columns=["k_sp", "start", "end"]).to_csv(f"{stem}_flags.tsv", sep="\t", index=False)
    t = cy.assign(year=cy["year"].astype(int), x_shift=cy["shift"].map(fx),
                  outliers_oe=cy["oe_out"].round(2), qc_fail_pct=(100 * cy["qc_rate"]).round(2),
                  outcome_mismatch_pct=(100 * cy["oc_rate"]).round(1), person_jump_oe=cy["oe_jump"].round(2),
                  typical_upper_limit=(10 ** cy["lhi_mode"]).round(4))
    t = t[["sp", "year", "n_v", "x_shift", "outliers_oe", "qc_fail_pct", "outcome_mismatch_pct", "person_jump_oe",
           "typical_upper_limit"]].rename(columns={"n_v": "rows"}).sort_values(["sp", "year"])
    mask(t, "rows", ["x_shift", "outliers_oe", "qc_fail_pct", "outcome_mismatch_pct", "person_jump_oe",
                     "typical_upper_limit"], args.min_n).to_csv(f"{stem}_sp_year.tsv", sep="\t", index=False)
    if len(ranges):
        r = ranges.assign(x_vs_national=ranges["x_vs_national"].map(fx),
                          sp_share_at_typical=(100 * ranges["sp_share_at_typical"]).round(1))
        r = r[["limit", "sex", "age", "sp", "rows", "sp_typical", "sp_share_at_typical", "national_typical",
               "x_vs_national"]].sort_values(["limit", "sex", "age", "rows"], ascending=[False, True, True, False])
        mask(r, "rows", ["sp_typical", "sp_share_at_typical", "x_vs_national"], args.min_n).to_csv(
            f"{stem}_ranges.tsv", sep="\t", index=False, float_format="%.4g")
    log(f"Wrote {stem}_flags.tsv, {stem}_sp_year.tsv" + (f", {stem}_ranges.tsv" if len(ranges) else ""))


def markdown(df, cols):
    lines = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for r in df[cols].itertuples(index=False):
        lines.append("| " + " | ".join(str(x) for x in r) + " |")
    return "\n".join(lines)


def concept_name(omop, names_file):
    if names_file and Path(names_file).exists():
        df = pd.read_csv(names_file, sep="\t", dtype=str,
                         usecols=["harmonization_omop::OMOP_ID", "harmonization_omop::OMOP_NAME"])
        hit = df[df["harmonization_omop::OMOP_ID"] == str(omop)]
        if len(hit):
            return hit["harmonization_omop::OMOP_NAME"].iloc[0]
    return None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    try:
        from kanta import config
        names = Path(config.DATA_DIR) / "LABfi.tsv"
    except ImportError:
        names = Path(__file__).resolve().parents[2] / "src" / "kanta" / "engine" / "data" / "LABfi.tsv"
    p = argparse.ArgumentParser(description=" ".join(__doc__.strip().splitlines()[2:3]),
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--omop", nargs="+", default=THREAD, help="OMOP ids (default: the QC-thread concepts)")
    p.add_argument("--all", action="store_true", help="Every OMOP in the input with >= --min-rows rows")
    p.add_argument("--min-rows", type=int, default=100_000, help="With --all: OMOPs with at least this many rows")
    p.add_argument("--input", type=Path, default=latest_engine_output(),
                   help="Engine output (or release) parquet; default: the latest run in " + str(ENGINE_DIR))
    p.add_argument("--org-map", type=Path, default=ORG_MAP,
                   help="The engine's coding_system_map.tsv: SP organisation names by the org code in the SP's OID")
    p.add_argument("--names", type=Path, default=names, help="Usagi mapping (concept names)")
    p.add_argument("--out", type=Path, default=OUT, help="Report folder")
    p.add_argument("--cache", type=Path, default=CACHE, help="The extracted rows and the aggregates, per OMOP")
    p.add_argument("--force", action="store_true", help="Re-extract the rows and rebuild the aggregates")
    p.add_argument("--fence-k", type=float, default=3.0, help="Tukey fence multiplier (log10) for outliers and jumps")
    p.add_argument("--min-stratum", type=int, default=1000,
                   help="Sex x age bands with fewer rows use the pooled baseline")
    p.add_argument("--shift", type=float, default=1.15,
                   help="Shift / episode: median off by at least this factor (methods differ by 5-10%%, e.g. creatinine)")
    p.add_argument("--step", type=float, default=1.05, help="Step: median change of at least this factor")
    p.add_argument("--step-z", type=float, default=4.0, help="Step: at least this many noise units (monthly MAD)")
    p.add_argument("--min-seg", type=int, default=6, help="Step: months needed on each side")
    p.add_argument("--episode-months", type=int, default=3, help="Episode: consecutive odd months needed")
    p.add_argument("--episode-z", type=float, default=3.0,
                   help="Episode: a month counts as odd only when also this many noise units off (monthly MAD)")
    p.add_argument("--range-margin", type=float, default=1.5, help="Range: SP typical limit off national by this factor")
    p.add_argument("--range-step", type=float, default=1.03, help="Range change: typical upper limit change factor")
    p.add_argument("--oe", type=float, default=3.0, help="Outliers / jumps / QC fails / outcome: observed vs expected")
    p.add_argument("--min-events", type=int, default=10, help="Outliers / jumps / QC fails / outcome: events needed")
    p.add_argument("--outcome-rate", type=float, default=0.1, help="Outcome: share of rows disagreeing with own range")
    p.add_argument("--min-cell", type=int, default=100, help="Rows an SP x year (or SP range) needs to be judged")
    p.add_argument("--min-month", type=int, default=30, help="Rows an SP x month needs in the monthly series")
    p.add_argument("--min-n", type=int, default=5, help="Cells with fewer rows are masked in the outputs")
    p.add_argument("--top-sp", type=int, default=25, help="SPs drawn in the heatmap and the range panel")
    p.add_argument("--table-rows", type=int, default=15, help="Flags listed in the report")
    p.add_argument("--memory-limit", default=f"{int(RAM_GB * 0.45)}GB", help="duckdb memory limit (spills beyond it)")
    p.add_argument("--threads", type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads")
    return p


def main():
    args = build_parser().parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.cache / "duckdb").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"set memory_limit = '{args.memory_limit}'")
    con.sql(f"set threads = {args.threads}")
    con.sql(f"set temp_directory = '{args.cache / 'duckdb'}'")
    con.sql("set preserve_insertion_order = false")
    if args.input is None:
        raise SystemExit(f"No engine output in {ENGINE_DIR}: pass --input")
    omops = [str(o) for o in args.omop]
    rows = ensure_rows(None if args.all else omops, args, con)
    if args.all:
        counts = con.sql(f"""select omop, count(*) n
                             from read_parquet({sql_str(str(rows) + '/*/*.parquet')}, hive_partitioning = true)
                             group by omop having count(*) >= {args.min_rows} order by n desc""").fetchall()
        omops = [str(o) for o, _ in counts]
    labels = Labels(args.org_map)
    log(f"{len(omops)} OMOP(s): {', '.join(omops)}; out {args.out}")
    for omop in omops:
        data = build(omop, args, con, rows)
        if data is None:
            continue
        labels.add_orgs(data["orgs"])
        r = data["rng"]
        data["rng"] = r.assign(lhi=np.log10(r["hi_h"].astype(float)), llo=np.log10(r["lo_h"].astype(float)))
        fdf, cy, ranges, nat, series = detect(omop, data, args, labels)
        write_outputs(omop, fdf, cy, ranges, args)
        name = concept_name(omop, args.names)
        title = f"OMOP {omop}" + (f" — {name}" if name else "") + ": service providers over time"
        report(omop, title, fdf, cy, ranges, nat, series, data["rng"], args, args.out / f"sp_qc_{omop}.png",
               data["dims"], data["meta"].get("h_unit"))
        counts = fdf["kind"].value_counts()
        print(f"\nOMOP {omop}: {len(fdf)} flags" + (" (" + ", ".join(f"{k} {counts[k]}" for k in KINDS
                                                                  if k in counts.index) + ")" if len(fdf) else ""))
        if len(fdf):
            print(markdown(fdf.head(args.table_rows), ["kind", "sp", "period", "rows", "effect_x", "detail", "cause"]))
    files = sorted(args.out.glob("sp_qc_*_flags.tsv"))
    if files:
        allf = pd.concat([pd.read_csv(f, sep="\t", dtype={"omop": str}) for f in files], ignore_index=True)
        allf.sort_values("score", ascending=False).to_csv(args.out / "sp_qc_flags.tsv", sep="\t", index=False)
        log(f"Wrote {args.out / 'sp_qc_flags.tsv'} ({len(allf)} flags over {len(files)} OMOPs)")


if __name__ == "__main__":
    main()
