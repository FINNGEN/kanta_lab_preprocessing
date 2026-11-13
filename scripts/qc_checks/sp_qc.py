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
  python3 sp_qc.py [--omop ID ...] [--input PARQUET] [--out DIR] [--cache DIR] [--scale all|raw|uln|lln]

--scale all (default): raw values and multiples of ULN and of LLN, in one PDF (pages 1-2: per lab three heatmap rows and
three boxes, raw / x ULN / x LLN; a scaled run is left out when under --min-scaled of the rows have that limit; page 4:
the flags of all runs); TSVs per run (_uln, _lln for the scaled ones).
--scale uln / lln: every value as a multiple of its own row's upper / lower reference limit (raw value over raw limit),
so labs on different assays compare; rows without that limit are left out; outputs and caches get _uln / _lln.

--omop defaults to the QC-thread concepts (creatinine, ALT, ferritin, HDL, TPO-Ab, TSH, holo-TC, CRP, sodium); --all runs
every OMOP in the input with >= --min-rows rows (one pass extracts them all). Writes into --out:
  sp_qc_<omop>.pdf             3 pages: (1) the --top-sp SPs by rows, (2) the next --top-sp most deviating SPs (largest
                               yearly median vs national, >= --min-dev-rows rows): per SP its name, rows, median vs
                               national per year and its values (p5-p95); (3) adult reference ranges of the top SPs by
                               rows and of the most deviating others, F and M, sorted by the multiplier vs national, and
                               one row per lab: its limits per year over the national band, and upright bars of
                               its typical ranges next to the national ones;
                               (4+) the flags, most important first (--table-rows), with a key to the kinds and in
                               plain words: what we see (shares of rows, not only ratios) and the likely cause
  sp_qc_<omop>_flags.tsv       that OMOP's flags
  sp_qc_<omop>_sp_year.tsv     per SP x year: rows, shift, outliers O/E, QC fail %, outcome mismatch %, person-jump O/E,
                               typical upper limit
  sp_qc_<omop>_ranges.tsv      per sex x minor/adult: the national typical range and its spread, and each SP's
Aggregates only: cells under --min-n rows are written as BELOW_THRESHOLD (stats blank) and left out of the plots; no
totals are written next to masked cells. SERVICE_PROVIDER is a pseudonym in R15: SPs are labelled by the organisation
most of their rows' coding systems belong to (CODING_SYSTEM_MAP), plus the end of the pseudonym.
"""
import argparse
import textwrap
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
from matplotlib.backends.backend_pdf import PdfPages  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402

ENGINE_DIR = Path("/mnt/disks/data/kanta/engine")
ORG_MAP = Path(__file__).resolve().parents[2] / "src/kanta/engine/data/coding_system_map.tsv"
ORG_CODE = re.compile(r"^1\.2\.246\.(?:537\.)?10\.(\d+)\.")
OUT = Path("/mnt/disks/data/kanta/qc/sp")  # next to omop_ref_audit.py's qc/omop
CACHE = Path("/mnt/disks/data/kanta/tmp/sp_qc")
THREAD = ["3020564", "3006923", "3001122", "3023602", "3027238", "3009201", "43054903", "3048689", "3019550"]
BIN = 0.01  # log10 width of the residual and baseline bins
RCLIP = 2.0  # residuals clipped to +- this (log10) before binning
MAX_GAP = 2  # missing months allowed inside a run of an SP's monthly series
CACHE_VERSION = "v8"
# an upper limit of 999, 9999, 99999, ...: "no upper limit" (some labs send it for one-sided ranges, e.g. HDL)
SENTINEL = "({x} >= 999 and abs({x} + 1 - power(10, round(log10({x} + 1)))) < 1e-6)"
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
DIM_LABEL = {"test": "test name", "unit": "unit", "note": "QC note", "unit_missing": "unit missing before injection",
             "extracted": "value extracted from text", "decimals": "decimals", "ref_src": "range source"}
KIND_TEXT = {  # one line per kind, for the report
    "step": "a lasting change in the lab's monthly median; 'limit changed too' points to a method change, "
            "'unchanged' to drift or a data problem",
    "episode": "a run of odd months that then returns to the lab's usual level",
    "shift": "the lab's level is off national (same sex and age) for years: often its patients, or its method",
    "outliers": "values outside the national fences (3 IQR on a log scale, same sex and age): normally very rare",
    "person_jump": "values far from the same person's other values of the test: wrong scale, wrong test, wrong person",
    "qc_fail": "rows failing the engine's own QC (QC_PASS = 0); the reason is in QC_NOTES",
    "outcome": "TEST_OUTCOME (H/L/N) disagrees with the row's own reference limits",
    "range": "the lab's typical adult limits differ from national by >= the range margin",
    "range_change": "the lab's typical limit changed from one year to the next",
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
    names_p = root / "sp_orgs.parquet"
    if not names_p.exists():
        # SP -> coding-system organisation counts over the whole input (only some rows of an SP carry an organisation,
        # and for a given OMOP maybe none: naming from one OMOP's rows leaves e.g. HUS unnamed)
        types = {r[0]: r[1] for r in con.sql(f"describe select * from read_parquet({sql_str(inp)})").fetchall()}
        csp = next((c for c in COLUMNS["sp"] if c in types), None)
        corg = next((c for c in COLUMNS["org"] if c and c in types), None)
        if csp and corg:
            log(f"Rows: lab names (SP x coding-system organisation) from {inp.name} ...")
            con.sql(f"""copy (select "{csp}" k_sp, "{corg}" org, count(*) n from read_parquet({sql_str(inp)})
                              where "{corg}" is not null and "{corg}" not in ('NA', '') group by all)
                        to {sql_str(names_p)} (format parquet)""")
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

TABLES = ["strata", "jump", "cells", "res", "rng", "dims", "orgs", "vals"]


def build(omop, args, con, rows):
    part = rows / f"omop={omop}"
    if not part.is_dir():
        log(f"OMOP {omop}: no rows in {Path(args.input).name}, skipped")
        return None
    cdir = args.cache / f"omop={omop}{scale_sfx(args)}"
    meta_p = cdir / "meta.json"
    params = {"version": CACHE_VERSION, "fence_k": args.fence_k, "bin": BIN, "min_stratum": args.min_stratum,
              "scale": args.scale}
    src_mtime = part_mtime(part)
    if meta_p.exists() and not args.force:
        meta = json.loads(meta_p.read_text())
        fresh = meta.get("params") == params and meta.get("base") == str(part) and meta.get("base_mtime", 0) >= src_mtime
        if fresh and meta.get("empty"):  # nothing to analyse last time either (e.g. no lower limits for x LLN)
            log(f"OMOP {omop}: cached, no rows{'' if args.scale == 'raw' else ' with that limit'} ({cdir})")
            return None
        if fresh and all((cdir / f"{t}.parquet").exists() for t in TABLES):
            log(f"OMOP {omop}: cached ({cdir})")
            return {t: pd.read_parquet(cdir / f"{t}.parquet") for t in TABLES} | {"meta": meta}

    P = f"read_parquet({sql_str(str(part) + '/*.parquet')})"
    log(f"OMOP {omop}: aggregating {part} ...")
    con.sql(f"""create or replace temp view r0 as
        select sp k_sp, test k_test, f, year, month, coalesce(cast(sex as varchar), 'U') sx,
               case when age is null then -1 when age >= 90 then 90 else (floor(age / 10) * 10)::int end band,
               case when age is null then 'U' when age < 18 then 'minor' else 'adult' end ag,
               v, v_raw, case when v > 0 then log10(v) end lv,
               case when v > 0 and v_raw > 0 and ref_high > 0 and not {SENTINEL.format(x="ref_high")}
                    then round(ref_high * v / v_raw, 6) end hi_h,
               case when v > 0 and v_raw > 0 and ref_low > 0 then round(ref_low * v / v_raw, 6) end lo_h,
               ref_low, ref_high, qc_pass, outcome, unit k_unit, note k_note, unit_missing, extracted, decimals, ref_src,
               org, h_unit
        from {P}""")
    # --scale uln / lln: every value as a multiple of its own row's upper / lower limit (the same as raw value over raw
    # limit, so independent of the unit conversion); rows without that limit left out
    lim = {"uln": "hi_h", "lln": "lo_h"}.get(args.scale)
    con.sql("create or replace temp view r as " + (
        "select * from r0" if lim is None else
        f"select * replace (case when v > 0 then log10(v / {lim}) end as lv) from r0 where {lim} is not null"))

    # --- 1 baseline per sex x age band (small strata take the pooled values)
    h = con.sql(f"select sx, band, round(lv / {BIN})::int b, count(*) n from r where lv is not null group by all").df()
    if h.empty:
        cdir.mkdir(parents=True, exist_ok=True)
        meta_p.write_text(json.dumps({"omop": omop, "params": params, "base_mtime": src_mtime, "base": str(part),
                                      "empty": True, "created": time.strftime("%Y-%m-%dT%H:%M:%S")}, indent=1) + "\n")
        log(f"OMOP {omop}: " + (f"no rows with a {'lower' if args.scale == 'lln' else 'upper'} reference limit > 0, "
                                f"x {args.scale.upper()} skipped" if lim else "no harmonized values > 0, skipped"))
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
               count(*) filter (where ref_low is not null or ref_high is not null) n_ref,
               count(hi_h) n_hi, count(*) filter (where v > hi_h) above,
               count(lo_h) n_lo, count(*) filter (where v < lo_h) below
        from x group by all""").df()
    res = con.sql(f"""
        select k_sp, year, month, round(greatest(least(lv - st.med, {RCLIP}), -{RCLIP}) / {BIN})::smallint rb, count(*) n
        from r join st using (sx, band) where lv is not null group by all""").df()
    rng = con.sql("""select k_sp, year, month, sx, ag, hi_h, lo_h, count(*) n from r
                     where hi_h is not null or lo_h is not null group by all""").df()
    dims = con.sql(" union all ".join(
        f"select k_sp, year, month, '{d}' dim, {e} cat, count(*) n from r group by all" for d, e in DIMS.items())).df()

    orgs = con.sql("select k_sp, org, count(*) n from r where org is not null group by all").df()
    vals = con.sql(f"select k_sp, round(lv / {BIN})::int b, count(*) n from r where lv is not null group by all").df()
    hu = con.sql("select h_unit, count(*) n from r where h_unit is not null group by all order by n desc limit 1").fetchall()
    cdir.mkdir(parents=True, exist_ok=True)
    out = {"strata": st, "jump": jump, "cells": cells, "res": res, "rng": rng, "dims": dims, "orgs": orgs, "vals": vals}
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


MIN_N = 5  # outputs: no count (or remainder of a shown total) of 1..MIN_N-1; set from --min-n in main()


def small(k, n):
    """True when k, or the remainder n - k, is a count of 1..MIN_N-1 (so k / n would give a small count away).
    Works on numbers and on arrays."""
    k, n = np.asarray(k, float), np.asarray(n, float)
    r = n - k
    return ((k >= 0.5) & (k < MIN_N - 0.5)) | ((r >= 0.5) & (r < MIN_N - 0.5))


def scale_sfx(args):
    """'' for raw values, '_uln' / '_lln' for --scale uln / lln (file and cache names)."""
    return "" if args.scale == "raw" else f"_{args.scale}"


def above_pct(cy, k, side="uln"):
    """% of an SP's rows above their own upper limit (side uln) or below their own lower limit (lln), over the rows
    that have that limit, or None (too few, or small)."""
    g = cy[cy["k_sp"] == k]
    nc, kc = ("n_hi", "above") if side == "uln" else ("n_lo", "below")
    n, a = g[nc].sum() if nc in g else 0, g[kc].sum() if kc in g else 0
    return None if n < 100 or small(a, n) else 100 * a / n


def fmt_counts(tmpl, events, expected, rows):
    """Fill a count flag's text: events, rows, rate (% of rows), exp_rate (% expected at the national rate), oe (the
    ratio, written x560 or x3.4)."""
    oe = events / expected if expected > 0 else float("nan")
    if small(events, rows):  # the count, or the rows without it, would be under MIN_N
        return tmpl.format(events=BELOW, rows=int(rows), rate=BELOW, exp_rate=100 * expected / rows, oe=BELOW)
    return tmpl.format(events=int(events), rows=int(rows), rate=f"{100 * events / rows:.1f}%",
                       exp_rate=100 * expected / rows, oe=f"{oe:.0f}" if oe >= 10 else f"{oe:.1f}")


def year_spans(years):
    """[2016, 2017, 2018, 2021] -> '2016..2018, 2021'."""
    out, a = [], years[0]
    for p, y in zip(years, years[1:] + [None]):
        if y != p + 1:
            out.append(str(a) if a == p else f"{a}..{p}")
            a = y
    return ", ".join(out)


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
            extra["detail"] = fmt_counts(first["tmpl"], ev, ex, rows_)
            extra["events"] = ev
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
                         fmt_counts(tmpl, events, expected, rows), events, expected, tmpl)

    # shift: one flag per SP, over its years off national (a level, often the lab's patient mix, not an event)
    judged = cy[(cy["n_v"] >= args.min_cell) & cy["shift"].notna()]
    for sp, g in judged.groupby("k_sp"):
        off = g[g["shift"].abs() >= lshift]
        if off.empty:
            continue
        side = np.sign(np.average(off["shift"], weights=off["n_v"]))
        off = off[np.sign(off["shift"]) == side]
        eff = float(np.average(off["shift"], weights=off["n_v"]))
        yrs = sorted(int(y) for y in off["year"])
        flags.append({"omop": omop, "kind": "shift", "k_sp": sp, "rows": int(off["n_v"].sum()), "effect": eff,
                      "start": yrs[0] * 12, "end": yrs[-1] * 12 + 11, "period": year_spans(yrs),
                      "detail": f"median {fx(eff)} vs national (same sex and age) " + (
                          f"in {yrs[0]}" if len(yrs) == 1 else
                          f"in {len(yrs)} of {len(g)} years ({fx(off['shift'].min())} to {fx(off['shift'].max())})")
                                + ": a steady level, often the lab's patients (who gets tested), or its method"})
    # outliers, person jumps, qc fails, outcome
    rows = [count_flag("outliers", r, r.n_v, r.obs_out, r.exp_out,
                       "{events} of {rows} values ({rate}) are extreme for this test (outside the national "
                       "fences, same sex and age); nationally {exp_rate:.2f}% ({oe}x expected)")
            for r in cy.itertuples() if r.obs_out >= args.min_events and pd.notna(r.oe_out) and r.oe_out >= args.oe]
    flags += merge_years(rows, ["k_sp", "kind"])
    rows = [count_flag("person_jump", r, r.n_j, r.obs_jump, r.exp_jump,
                       "{events} of {rows} values ({rate}) are far from the same person's other values of "
                       "this test; nationally {exp_rate:.2f}% ({oe}x expected)")
            for r in cy.itertuples() if r.obs_jump >= args.min_events and pd.notna(r.oe_jump) and r.oe_jump >= args.oe]
    flags += merge_years(rows, ["k_sp", "kind"])
    if nat_qc >= 0:
        rows = [count_flag("qc_fail", r, r.n_qc, r.qc_fail, r.n_qc * max(nat_qc, 1e-4),
                           "{events} of {rows} rows ({rate}) fail the engine's QC (QC_PASS = 0); nationally " +
                           f"{100 * nat_qc:.2f}%")
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
                                   "TEST_OUTCOME (H/L/N) disagrees with the row's own reference limits in {rate} "
                                   f"of rows (nationally {100 * nat_oc:.1f}%), mostly {main}"))
    flags += merge_years(rows, ["k_sp", "kind"])

    # --- monthly series: steps and episodes
    rm = add_t(res)
    cm = add_t(cells)
    mm = group_median(rm, ["k_sp", "t"], "rb")
    mm["m"] = mm["med"] * BIN
    mm = mm.merge(cm.groupby(["k_sp", "t"], as_index=False)[["obs_out", "exp_out"]].sum(), on=["k_sp", "t"], how="left")
    mm = mm[mm["rows"] >= args.min_month]
    rng_t = add_t(rng)
    rng_by_sp = {sp: g for sp, g in rng_t.groupby("k_sp")}
    series = {}
    for sp, s in mm.sort_values(["k_sp", "t"]).groupby("k_sp", sort=False):
        # runs of months with data (a new run after more than MAX_GAP missing months); steps and episodes only on
        # runs of >= --min-seg months, so that a few scattered early months don't make a "step" at the real start
        run = np.concatenate([[0], np.cumsum(np.diff(s["t"].to_numpy()) > MAX_GAP + 1)])
        long = np.bincount(run)[run] >= args.min_seg
        series[sp] = s.assign(run=run, long=long)
        if not long.any():
            continue
        s = s[long].assign(run=run[long])
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
                r_sp = rng_by_sp.get(sp, rng_t.iloc[:0])
                detail = f"median {fx(d)} from {ym(tb)} on, for good ({abs(d) / noise:.0f}x its month-to-month noise)"
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
                detail = (f"median {fx(d)} vs the lab's own usual level for {int(ts[j] - ts[i] + 1)} months, then "
                          f"back to it")
                if e > 0 and o >= args.min_events:
                    detail += f"; outliers {o / e:.1f}x expected"
                flags.append({"omop": omop, "kind": "episode", "k_sp": sp, "rows": int(seg["rows"].sum()), "effect": d,
                              "start": int(ts[i]), "end": int(ts[j]), "period": f"{ym(ts[i])}..{ym(ts[j])}",
                              "detail": detail})
            i = j + 1

    # --- ranges: SP typical limit vs national, per sex x minor/adult; and changes between years
    range_rows, range_dev = [], []
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
        range_dev.append(dev.assign(lim=lim))
    # one flag per SP: its adult ranges vs national, the limits off by >= x--range-margin driving the effect
    if range_dev:
        dev = pd.concat(range_dev, ignore_index=True)
        rr = pd.DataFrame(range_rows)
        for sp, g in dev.groupby("k_sp"):
            own = rr[(rr["k_sp"] == sp) & (rr["age"] == "adult")]

            def rng_txt(col):
                parts = []
                for sx in ("F", "M"):
                    o = own[own["sex"] == sx].set_index("limit")[col]
                    if len(o):
                        parts.append(f"{sx} {fmt_range(o.get('lower'), o.get('upper'))}")
                return " ".join(parts)
            main = g.loc[(g["mode"] - g["nat"]).abs().idxmax()]
            flags.append({"omop": omop, "kind": "range", "k_sp": sp, "rows": int(g.groupby(["sx", "ag"])["rows"].max().sum()),
                          "effect": float(main["mode"] - main["nat"]), "start": None, "end": None, "period": "all years",
                          "detail": f"adult range {rng_txt('sp_typical')} vs national {rng_txt('national_typical')}"})
    # range changes between consecutive years, in the SP's main sex x age group
    rc = []
    ry = rng[rng["year"].notna()]
    # per SP, as main_limit(): the upper limit (lower when it has none), adults when it has any, its largest group
    has_hi = ry.groupby("k_sp")["lhi"].apply(lambda x: x.notna().any())
    ry = ry.assign(col=np.where(ry["k_sp"].map(has_hi).astype(bool), "lhi", "llo"))
    ry = ry.assign(lv=np.where(ry["col"] == "lhi", ry["lhi"], ry["llo"]))
    ry = ry[ry["lv"].notna()]
    grp_n = ry.groupby(["k_sp", "sx", "ag"], as_index=False)["n"].sum().assign(adult=lambda d: d["ag"] == "adult")
    main = grp_n.sort_values(["adult", "n"], ascending=False).drop_duplicates("k_sp")[["k_sp", "sx", "ag"]]
    ymode = group_mode(ry.merge(main, on=["k_sp", "sx", "ag"]), ["k_sp", "sx", "ag", "year"], "lv")
    ymode = ymode[ymode["rows"] >= args.min_cell].sort_values(["k_sp", "year"])
    for (sp, sx_, ag_), yr in ymode.groupby(["k_sp", "sx", "ag"]):
        lim = "upper" if has_hi[sp] else "lower"
        grp = f"{sex_label(sx_)} {ag_}"
        for a, b in zip(yr.itertuples(), list(yr.itertuples())[1:]):
            if b.year == a.year + 1 and abs(b.mode - a.mode) >= math.log10(args.range_step):
                rc.append({"omop": omop, "kind": "range_change", "k_sp": sp, "rows": int(b.rows),
                           "effect": b.mode - a.mode, "start": int(b.year) * 12, "end": int(b.year) * 12 + 11,
                           "period": f"{int(a.year)}->{int(b.year)}",
                           "detail": f"typical {lim} limit {fnum(10 ** a.mode)} -> {fnum(10 ** b.mode)} ({grp})"})
    flags += rc

    # --- causes
    ctx = cause_context(dims)
    for fl in flags:
        fl["cause"] = cause(fl, ctx, labels, args)
    fdf = pd.DataFrame(flags)
    if len(fdf):
        fdf["sp"] = fdf["k_sp"].map(labels.sp)
        fdf["effect_x"] = fdf.apply(lambda r: fx(r["effect"]) if r["kind"] in ("shift", "step", "episode", "range",
                                                                              "range_change") else
                                    f"{10 ** r['effect']:.1f}x", axis=1)
        if "events" in fdf.columns:
            hide = fdf["events"].notna() & small(fdf["events"].fillna(0), fdf["rows"])
            fdf.loc[hide, "effect_x"] = BELOW
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


def cause_context(dims):
    """The category counts as integer arrays, once per OMOP: each (dim, category) a code; per SP its rows' month, code
    and count; totals per code. A flag's cause is then a few bincounts."""
    dt = add_t(dims)
    cat = dt["cat"].astype(object).where(dt["cat"].notna(), "NA")
    code, uniq = pd.factorize(pd.Series(list(zip(dt["dim"], cat))))
    k = len(uniq)
    dim_id, dim_names = pd.factorize(pd.Series([u[0] for u in uniq]))
    n = dt["n"].to_numpy(float)
    t = dt["t"].to_numpy(float)
    sp_id, sp_names = pd.factorize(dt["k_sp"])
    order = np.argsort(sp_id, kind="stable")
    bounds = np.searchsorted(sp_id[order], np.arange(len(sp_names) + 1))
    per_sp = {}
    for i, sp in enumerate(sp_names):
        ix = order[bounds[i]:bounds[i + 1]]
        per_sp[sp] = (t[ix], code[ix], n[ix], np.bincount(code[ix], n[ix], k))
    return {"k": k, "uniq": uniq, "dim_id": dim_id, "n_dim": len(dim_names), "total": np.bincount(code, n, k),
            "per_sp": per_sp}


def cause(fl, ctx, labels, args):
    """The category whose share changed most between the flagged period and the SP's other periods (or between the SP
    and the other SPs, when there is no period or too little else)."""
    sp = fl["k_sp"]
    if sp not in ctx["per_sp"]:
        return ""
    t, code, n, own = ctx["per_sp"][sp]
    k, dim_id, n_dim = ctx["k"], ctx["dim_id"], ctx["n_dim"]
    a = None
    if fl.get("start") is not None and pd.notna(fl.get("start")):
        m = (t >= fl["start"]) & (t <= fl["end"])
        a = np.bincount(code[m], n[m], k)
        b = own - a
        ref = "the lab's other periods"
        if b.sum() < args.min_cell * len(DIMS):
            a = None
    if a is None:  # no period (range), or too little outside it: this SP vs the others
        a, b, ref = own, ctx["total"] - own, "other labs"
    sa, sb = np.bincount(dim_id, a, n_dim)[dim_id], np.bincount(dim_id, b, n_dim)[dim_id]
    ok = (sa >= args.min_n) & (sb >= args.min_n) & ~small(a, sa) & ~small(b, sb)
    if not ok.any():
        return ""
    with np.errstate(divide="ignore", invalid="ignore"):
        pa, pb = np.where(ok, a / sa, 0.0), np.where(ok, b / sb, 0.0)
    diff = pa - pb
    i = int(np.argmax(np.abs(diff)))
    if abs(diff[i]) < 0.1:
        return ""
    dim, cat = ctx["uniq"][i]
    name = labels.name(dim, cat) if dim in ("test", "unit", "note") else ("NA" if pd.isna(cat) else str(cat))
    return f"{DIM_LABEL.get(dim, dim)} {name}: {100 * pb[i]:.0f}% of rows in {ref} -> {100 * pa[i]:.0f}% here"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def titled(ax, title, question):
    ax.set_title(title, fontsize=10, color=INK, loc="left", pad=19)
    ax.annotate(question, (0, 1), xycoords="axes fraction", xytext=(0, 4), textcoords="offset points", fontsize=8.5,
                color=MUTED, style="italic", ha="left", va="bottom")


def style(ax):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(LINE)
    ax.tick_params(colors=MUTED, labelsize=7.5)


def plain_log_x(ax, decades=None):
    """Log x axis with plain labels (1-1.5-2-3-5-7 steps, or 1-2-5 when it spans more than about 2 decades)."""
    ax.set_xscale("log")
    subs = (1, 2, 5) if decades is not None and decades > 2 else (1, 1.5, 2, 3, 5, 7)
    ax.xaxis.set_major_locator(matplotlib.ticker.LogLocator(base=10, subs=subs))
    ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: fnum(v)))
    ax.xaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    ax.tick_params(axis="x", labelsize=8)


def fmt_range(lo, hi):
    if pd.notna(lo) and pd.notna(hi):
        return f"{fnum(lo)}-{fnum(hi)}"
    return f"<{fnum(hi)}" if pd.notna(hi) else (f">{fnum(lo)}" if pd.notna(lo) else "")


def ref_src_share(dims):
    """% of the limits that come from the curated text table, per SP and overall ("__all__")."""
    d = dims[(dims["dim"] == "ref_src") & dims["cat"].notna() & (dims["cat"] != "NA")]
    if d.empty:
        return {}
    by = d.groupby(["k_sp", "cat"])["n"].sum().unstack("cat", fill_value=0)
    text = by.get("text", pd.Series(0, index=by.index))
    tot = by.sum(axis=1)
    out = (100 * text / tot).where((tot >= 1) & ~small(text, tot)).to_dict()
    out["__all__"] = 100 * text.sum() / by.values.sum()
    return out


def pick_value_labs(cy, args):
    """Top --top-sp SPs by rows with a value, and the next --top-sp most deviating ones (largest yearly |shift| vs
    national in a year with >= --min-cell rows; SPs with >= --min-dev-rows rows; ties by rows), with that shift."""
    vol = cy.groupby("k_sp")["n_v"].sum().sort_values(ascending=False)
    top = list(vol.head(args.top_sp).index)
    j = cy[(cy["n_v"] >= args.min_cell) & cy["shift"].notna()]
    dev = j.loc[j.groupby("k_sp")["shift"].apply(lambda x: x.abs().idxmax())][["k_sp", "shift", "year"]]
    dev = dev[dev["k_sp"].isin(vol[vol >= args.min_dev_rows].index) & ~dev["k_sp"].isin(top)]
    dev = dev.assign(a=dev["shift"].abs(), n=dev["k_sp"].map(vol)).sort_values(["a", "n"], ascending=False)
    return top, dev.head(args.top_sp).set_index("k_sp"), vol


def sp_label(k, cy, vol, min_n=5):
    n = int(vol[k])
    a = above_pct(cy, k)
    return (f"{cy.loc[cy['k_sp'] == k, 'sp'].iloc[0]}   {n if n >= min_n else BELOW} rows"
            + (f" · {a:.0f}% > ULN" if a is not None else ""))


def heatmap(ax, cy, order, args):
    """Median vs national (same sex x age) per SP x year, for the SPs in `order` (top to bottom)."""
    d = cy[cy["k_sp"].isin(order)]
    years = sorted(int(y) for y in cy["year"].dropna().unique())
    grid = np.full((len(order), len(years)), np.nan)
    for r in d.itertuples():
        if r.n_v >= args.min_cell and pd.notna(r.shift):
            grid[order.index(r.k_sp), years.index(int(r.year))] = r.shift
    lim = math.log10(1.5)
    cmap = LinearSegmentedColormap.from_list("div", [NEG, "#e8e6df", POS])
    cmap.set_bad(SURFACE)
    im = ax.imshow(np.clip(grid, -lim, lim), cmap=cmap, vmin=-lim, vmax=lim, aspect="auto", interpolation="nearest")
    ax.set_xticks(range(len(years)))
    ax.set_xticklabels(years, fontsize=8)
    for i in range(len(order)):
        for j in range(len(years)):
            if np.isfinite(grid[i, j]):
                ax.text(j, i, fx(grid[i, j]), ha="center", va="center", fontsize=6.5,
                        color=INK if abs(grid[i, j]) < 0.6 * lim else SURFACE)
    cb = plt.colorbar(im, ax=ax, fraction=0.03, pad=0.08, location="bottom", aspect=40)
    ticks = [-lim, -math.log10(1.2), 0, math.log10(1.2), lim]
    cb.set_ticks(ticks)
    cb.set_ticklabels([fx(t) for t in ticks])
    cb.ax.tick_params(labelsize=7, colors=MUTED)
    cb.outline.set_edgecolor(LINE)
    for sp_ in ax.spines.values():
        sp_.set_visible(False)
    ax.tick_params(colors=MUTED, length=0)
    titled(ax, f"Median vs national per year (same sex x age; blank: under {args.min_cell} rows)",
           "Is the lab above or below the national values, and since when?")


def value_boxes(ax, vals, order, h_unit, scale="raw", min_rows=100):
    """Each SP's harmonized values (log scale): box p25-p75, whiskers p5-p95, median; national p25-p75 shaded."""
    def q(d, p):
        return 10 ** wq_hist(d["b"].to_numpy() * BIN, d["n"], p)
    nat = vals.groupby("b", as_index=False)["n"].sum()
    n25, n50, n75 = q(nat, 0.25), q(nat, 0.5), q(nat, 0.75)
    ax.axvspan(n25, n75, color=MUTED, alpha=0.1, lw=0)
    ax.axvline(n50, color=MUTED, lw=1.2)
    xs = []
    for i, k in enumerate(order):
        d = vals[vals["k_sp"] == k]
        if d["n"].sum() < min_rows:  # quantiles of a few rows are close to single values: not drawn
            ax.text(1.01, i, f"under {min_rows} rows: not shown", transform=ax.get_yaxis_transform(), va="center",
                    fontsize=8, color=MUTED)
            continue
        p5, p25, p50, p75, p95 = (q(d, p) for p in (0.05, 0.25, 0.5, 0.75, 0.95))
        xs += [p5, p95]
        ax.plot([p5, p25], [i, i], color=SERIES[0], lw=1.2)
        ax.plot([p75, p95], [i, i], color=SERIES[0], lw=1.2)
        ax.add_patch(plt.Rectangle((p25, i - 0.3), p75 - p25, 0.6, facecolor=SERIES[0] + "55", edgecolor=SERIES[0], lw=1))
        ax.plot([p50, p50], [i - 0.3, i + 0.3], color=INK, lw=2)
        ax.text(1.01, i, f"median {fnum(p50)}  ({fx(math.log10(p50 / n50))})", transform=ax.get_yaxis_transform(),
                va="center", fontsize=8, color=INK)
    plain_log_x(ax, math.log10(max(xs) / min(xs)) if xs and min(xs) > 0 else None)
    ax.set_ylim(len(order) - 0.5, -0.5)
    ax.set_yticks([])
    if scale != "raw":  # the limit itself
        ax.axvline(1, color=POS, lw=1.2, ls=":")
        ax.text(1, len(order) - 0.5, f" {scale.upper()}", color=POS, fontsize=8, va="bottom")
    ax.set_xlabel(f"value / {scale.upper()} (multiples of the row's own {'upper' if scale == 'uln' else 'lower'} "
                  f"limit, log scale)" if scale != "raw" else f"value ({h_unit or 'harmonized unit'}, log scale)",
                  fontsize=8, color=MUTED)
    ax.grid(axis="x", color=LINE, lw=0.5, which="both")
    style(ax)
    titled(ax, f"Values: box p25-p75, whiskers p5-p95, black line: median",
           f"grey: national p25-p75 and median {fnum(n50)}; not adjusted for sex and age")


def page_values(pdf, title, subtitle, order, cy, vals, vol, h_unit, args):
    fig = plt.figure(figsize=(17, 2.6 + 0.62 * max(len(order), 1)), facecolor=SURFACE)
    if not order:
        fig.text(0.5, 0.5, "no service providers to show", ha="center", color=MUTED)
    else:
        gs = fig.add_gridspec(1, 2, width_ratios=[1.25, 1], wspace=0.04, left=0.24, right=0.9, top=0.8, bottom=0.14)
        ax = fig.add_subplot(gs[0])
        heatmap(ax, cy, order, args)
        ax.set_yticks(range(len(order)))
        ax.set_yticklabels([sp_label(k, cy, vol) for k in order], fontsize=9.5, color=INK)
        value_boxes(fig.add_subplot(gs[1]), vals, order, h_unit, args.scale, args.min_cell)
    fig.suptitle(title, fontsize=13, fontweight="bold", color=INK, x=0.01, y=0.985, ha="left")
    fig.text(0.01, 0.94, subtitle, fontsize=10, color=MUTED, ha="left", va="top")
    pdf.savefig(fig, facecolor=SURFACE)
    plt.close(fig)


def national_limits(a):
    """National typical adult limits per (sex, limit), only for limits that at least half of the sex's rows with limits
    carry (HDL: a lower limit only, though a few labs also send an upper one)."""
    nat = {}
    for sx, g in a.groupby("sex"):
        n = g.groupby("limit")["rows"].sum()
        for lim, h in g.groupby("limit"):
            if n[lim] >= 0.5 * n.max():
                nat[(sx, lim)] = float(h["national_typical"].iloc[0])
    return nat


def range_table(ranges, src, nat):
    """Per SP (adults): typical lower / upper limit per sex, the multiplier vs national (mean log ratio over the limits
    both the SP and national have), the largest deviation, rows with limits, % from the text table. Whole-table
    operations (a loop over SPs took minutes for tests with many of them)."""
    a = ranges[ranges["age"] == "adult"]
    if a.empty:
        return pd.DataFrame()
    piv = a.pivot_table(index="k_sp", columns=["sex", "limit"], values="sp_typical", aggfunc="first")
    t = pd.DataFrame(index=piv.index)
    t["sp"] = a.groupby("k_sp")["sp"].first()
    t["rows"] = a.groupby(["k_sp", "sex"])["rows"].max().groupby("k_sp").sum().astype(int)
    xs = []
    for sx in ("F", "M"):
        for lim in ("lower", "upper"):
            v = piv[(sx, lim)] if (sx, lim) in piv.columns else pd.Series(np.nan, index=piv.index)
            t[f"{sx}_{lim}"] = v.astype(float)
            x = np.log10(v.astype(float) / nat[(sx, lim)]) if (sx, lim) in nat else pd.Series(np.nan, index=piv.index)
            t[f"{sx}_{lim}_x"] = x
            xs.append(x.rename(f"{sx}_{lim}"))
    xx = pd.concat(xs, axis=1)
    t["mult"] = xx.mean(axis=1).fillna(0.0)
    t["dev"] = xx.abs().max(axis=1).fillna(0.0)
    t["text"] = [src.get(k) for k in t.index]
    return t.reset_index()


def unbound_arrow(ax, v, y, side, color, lw=2.5, alpha=0.8):
    """A short arrow from a one-sided limit towards its unbounded side (side = +1 upwards, -1 downwards), a fixed
    length on screen whatever the axis scale."""
    # the head is drawn at xytext: 28 points from the limit, on the unbounded side
    ax.annotate("", xy=(v, y), xycoords="data", xytext=(side * 28, 0), textcoords="offset points",
                arrowprops=dict(arrowstyle="<|-", color=color, lw=lw, alpha=alpha, mutation_scale=12,
                                shrinkA=0, shrinkB=0))


F_COL, M_COL = "#E58FB5", "#6FA8DC"  # women pink, men light blue (limits over time)


def limits_by_year(rng, labs, args):
    """Typical adult limit per lab, sex, side and year (years with >= --min-cell rows), for the per-lab plots of
    limits over time: a DataFrame k_sp, sex ('F'/'M'), side ('upper'/'lower'), year, value."""
    r = rng[rng["k_sp"].isin(labs) & (rng["ag"] == "adult") & rng["year"].notna()]
    out = []
    for col, side in (("lhi", "upper"), ("llo", "lower")):
        m = group_mode(r[r[col].notna()], ["k_sp", "sx", "year"], col)
        m = m[m["rows"] >= args.min_cell]
        out.append(m.assign(side=side, value=10 ** m["mode"].astype(float)))
    d = pd.concat(out, ignore_index=True)
    d["sex"] = d["sx"].map(sex_label)
    return d[d["sex"].isin(["F", "M"])][["k_sp", "sex", "side", "year", "value"]]


def limits_row(ax, d, nat, years, show_x, extra=()):
    """One lab's adult limits per year (women pink, men light blue; upper above lower; a step = a change) against the
    national typical limits (dashed, same colours). The axis spans the lab's and the national values, so each row
    reads against national by itself."""
    vals = list(d["value"]) + [v for v in nat.values() if v > 0] + [v for v in extra if pd.notna(v) and v > 0]
    if vals:
        ax.set_yscale("log")
        y0, y1 = min(vals) / 1.35, max(vals) * 1.35
        ax.set_ylim(y0, y1)
        for sx, c in (("F", F_COL), ("M", M_COL)):  # national range per sex as a light band (one-sided: to the edge)
            lo, hi = nat.get((sx, "lower")), nat.get((sx, "upper"))
            if lo is not None or hi is not None:
                ax.axhspan(lo if lo is not None else y0, hi if hi is not None else y1, color=c, alpha=0.13, lw=0)
    for sx, c, lw in (("F", F_COL, 2.6), ("M", M_COL, 1.4)):  # women a bit wider underneath: both show when equal
        for side in ("upper", "lower"):
            g = d[(d["sex"] == sx) & (d["side"] == side)].sort_values("year")
            if len(g):
                ax.step(g["year"].astype(int), g["value"], where="mid", color=c, lw=lw)
                ax.plot(g["year"].astype(int), g["value"], ls="none", marker="o", ms=2.5, color=c)
    if years:
        ax.set_xlim(years[0] - 0.5, years[-1] + 0.5)
        ax.set_xticks(years)
    # the limit values on the right-hand axis, plain numbers at 1-2-5 steps
    ax.yaxis.tick_right()
    ax.yaxis.set_major_locator(matplotlib.ticker.LogLocator(base=10, subs=(1, 2, 5), numticks=6))
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: fnum(v)))
    ax.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    ax.tick_params(axis="y", labelsize=7, colors=MUTED, length=2)
    ax.tick_params(axis="x", labelsize=8, colors=MUTED, length=0, labelbottom=show_x)
    ax.grid(axis="x", color=LINE, lw=0.4)
    for sp_ in ("top", "left"):
        ax.spines[sp_].set_visible(False)
    ax.spines["bottom"].set_color(LINE)
    ax.spines["right"].set_color(LINE)


def range_bars(axb, r, nat, show_x):
    """The lab's typical ranges next to the national ones, as upright bars on the row's limit axis: women (pink) and
    national women (grey), men (blue) and national men (grey); a one-sided range runs to the axis edge, lighter."""
    y0, y1 = axb.get_ylim()
    bars = [(r["F_lower"], r["F_upper"], F_COL), (nat.get(("F", "lower")), nat.get(("F", "upper")), "#B9B7B0"),
            (r["M_lower"], r["M_upper"], M_COL), (nat.get(("M", "lower")), nat.get(("M", "upper")), "#B9B7B0")]
    for x, (lo, hi, c) in enumerate(bars):
        lo = None if lo is None or pd.isna(lo) else lo
        hi = None if hi is None or pd.isna(hi) else hi
        if lo is None and hi is None:
            continue
        one_sided = lo is None or hi is None
        axb.plot([x, x], [lo if lo is not None else y0, hi if hi is not None else y1], color=c, lw=9,
                 solid_capstyle="butt", alpha=0.45 if one_sided else 0.95)
    axb.set_xlim(-0.7, 3.7)
    axb.set_xticks(range(4))
    axb.set_xticklabels(["F", "ref", "M", "ref"], fontsize=7)
    axb.tick_params(axis="x", colors=MUTED, length=0, labelbottom=show_x)
    axb.tick_params(axis="y", left=False, labelleft=False, right=False, labelright=False)
    for sp_ in ("top", "left", "right"):
        axb.spines[sp_].set_visible(False)
    axb.spines["bottom"].set_color(LINE)


def range_block(fig, spec, t, nat, block_title, h_unit, args, years_df=None):
    """One row per lab, sorted by the multiplier vs national (high first): its adult limits per year against the
    national ones; on the left its name, rows, typical ranges, multiplier and limit source."""
    t = t.sort_values("mult", ascending=False).reset_index(drop=True)
    d_all = years_df if years_df is not None else pd.DataFrame(columns=["k_sp", "sex", "side", "year", "value"])
    years = sorted(int(y) for y in d_all["year"].unique()) if len(d_all) else []
    rows = spec.subgridspec(len(t), 1, hspace=0.35)
    for i, r in t.iterrows():
        cells = rows[i].subgridspec(1, 2, width_ratios=[7, 1], wspace=0.09)
        ax = fig.add_subplot(cells[0])
        last = i == len(t) - 1
        limits_row(ax, d_all[d_all["k_sp"] == r["k_sp"]], nat, years, show_x=last,
                   extra=[r["F_lower"], r["F_upper"], r["M_lower"], r["M_upper"]])
        range_bars(fig.add_subplot(cells[1], sharey=ax), r, nat, show_x=last)
        ranges_txt = "  ".join(f"{x} {fmt_range(r[f'{x}_lower'], r[f'{x}_upper'])}" for x in ("F", "M")
                               if pd.notna(r[f"{x}_lower"]) or pd.notna(r[f"{x}_upper"]))
        src_txt = f" · text {r['text']:.0f}%" if r["text"] is not None and pd.notna(r["text"]) else ""
        ax.text(-0.01, 0.62, f"{r['sp']}   {r['rows']} rows", transform=ax.transAxes, ha="right", va="center",
                fontsize=9, color=INK)
        ax.text(-0.01, 0.22, f"{ranges_txt}  {fx(r['mult'])}{src_txt}", transform=ax.transAxes, ha="right",
                va="center", fontsize=8.5, color=POS if r["mult"] > math.log10(1.05) else
                NEG if r["mult"] < -math.log10(1.05) else MUTED)
        if i == 0:
            nat_txt = "  ".join(f"{x} {fmt_range(nat.get((x, 'lower')), nat.get((x, 'upper')))}" for x in ("F", "M")
                                if (x, "lower") in nat or (x, "upper") in nat)
            titled(ax, f"{block_title}: adult limits per year ({h_unit or 'harmonized unit'}, log scale)",
                   f"lines: the lab, pink women, blue men · bands and grey bars (ref): national ({nat_txt}) · "
                   f"right: the lab's typical ranges")
    return []


def page_ranges(pdf, title, ranges, src, h_unit, args, rng=None):
    a = ranges[ranges["age"] == "adult"] if len(ranges) else pd.DataFrame()
    nat = national_limits(a) if len(a) else {}
    t = range_table(ranges, src, nat) if len(a) else pd.DataFrame()
    top = dev = pd.DataFrame()
    if not t.empty:
        t = t[t["rows"] >= args.min_cell]
        top = t.sort_values("rows", ascending=False).head(args.top_sp)
        dev = t[~t["k_sp"].isin(top["k_sp"]) & (t["dev"] >= math.log10(1.05))]
        dev = dev.sort_values(["dev", "rows"], ascending=False).head(args.top_sp)
    n_rows = len(top) + len(dev)
    labs = list(top["k_sp"]) + list(dev["k_sp"]) if not t.empty else []
    years = limits_by_year(rng, labs, args) if rng is not None and len(labs) else None
    fig = plt.figure(figsize=(17, 2.4 + 1.3 * (1 + (len(dev) > 0)) + 0.9 * max(n_rows, 2)), facecolor=SURFACE)
    if top.empty:
        fig.text(0.5, 0.5, "no reference limits (adults)", ha="center", color=MUTED)
    else:
        tot = src.get("__all__")
        fig.text(0.01, 1 - 0.55 / fig.get_figheight(), "Reference limits, adults, per lab and year (converted to the "
                 "harmonized unit; years with at least " + str(args.min_cell) + " rows).\nUnder each lab: its typical "
                 "ranges and multiplier vs national (orange above, blue below); labs sorted by the multiplier, high first" + (f".  Limits from the curated text table for {tot:.0f}% "
                 f"of rows, sent by the lab for {100 - tot:.0f}%." if tot is not None else ""),
                 fontsize=10, color=MUTED, ha="left", va="top")
        blocks = [(top, f"Top {len(top)} labs by rows with limits")] + \
                 ([(dev, f"Top {len(dev)} other labs by deviation (>= x1.05)")] if len(dev) else [])
        H = fig.get_figheight()
        gs = fig.add_gridspec(len(blocks), 1, height_ratios=[max(len(b), 1) for b, _ in blocks], hspace=0.3,
                              left=0.3, right=0.97, top=1 - 1.5 / H, bottom=0.7 / H)
        for bi, (b, name) in enumerate(blocks):
            range_block(fig, gs[bi], b, nat, name, h_unit, args, years)

    fig.suptitle(title, fontsize=13, fontweight="bold", color=INK, x=0.01, y=1 - 0.2 / fig.get_figheight(), ha="left")
    pdf.savefig(fig, facecolor=SURFACE)
    plt.close(fig)


LINE_IN = 0.15  # one table text line (7.5 pt), in inches
CHAR_IN = 0.052  # an average character at 7.5 pt, in inches (for wrapping to the column's drawn width)


def flag_key(fig, top):
    """The key to the kinds of flag and to 'effect' / 'likely cause', from `top` down; returns where it ends."""
    for i, k in enumerate(KINDS):
        y = top - 0.019 * i
        fig.text(0.02, y, k, fontsize=8.5, color=INK, fontweight="bold", va="top",
                 bbox=dict(boxstyle="round,pad=0.2", facecolor=KIND_COLOR[k] + "40", edgecolor="none"))
        fig.text(0.1, y, KIND_TEXT[k], fontsize=8.5, color=INK, va="top")
    y = top - 0.019 * len(KINDS) - 0.006
    fig.text(0.02, y, "effect: the size of the change (x1.24 = 24% higher, /1.09 = 9% lower) or, for counts, observed "
             "vs expected at the national rate; when the national rate is tiny the ratio gets huge (x560): read the % "
             "in 'what we see'. Likely cause: the test name, unit, decimals, QC note or range source whose share of "
             "the lab's rows changed most in the flagged period.", fontsize=8.5, color=MUTED, va="top", wrap=True)
    return y - 0.045


def pages_flags(pdf, title, fdf, args, first_page=4):
    """The flags, most important first (score = |log10 effect| x log10 rows), --table-rows at most, as many pages as
    their wrapped text needs; a key to the kinds on the first page."""
    one_page = not args.table_rows  # default: as many as fit on one page
    f = fdf if one_page else fdf.head(args.table_rows)
    counts = fdf["kind"].value_counts()
    widths = {"kind": 12, "lab": 32, "period": 16, "rows": 7, "effect": 8, "what we see": 80, "likely cause": 52}
    if "values" in fdf.columns:
        widths = {"kind": 12, "values": 7, **{k: v for k, v in widths.items() if k != "kind"}}
        widths["what we see"] = 74
    cols = list(widths)
    # wrap each column to the characters its drawn width holds (the table is 0.97 x 0.98 of the 17 in page)
    pad = 0.012  # cell padding, as a share of the table width (matplotlib's default is 10% of each cell: wasteful)
    fit = {c: max(6, int((0.97 * 0.98 * 17) * (widths[c] / sum(widths.values()) - 2 * pad) / CHAR_IN)) for c in cols}
    cells = []
    for r in f.itertuples():
        vals = [r.kind] + ([r.values] if "values" in fdf.columns else []) + \
               [r.sp, r.period, str(r.rows), r.effect_x, r.detail, r.cause or ""]
        cells.append([textwrap.fill(str(v), fit[c]) for v, c in zip(vals, cols)])
    n_lines = [max(c.count("\n") + 1 for c in row) for row in cells]
    H = 11.5
    key_end = None
    pages, start = [], 0
    while start < len(cells) or not pages:
        top = 0.92 if pages else (key_end or 0.92 - 0.019 * len(KINDS) - 0.051)
        avail = (top - 0.03) * H / LINE_IN - 1.6  # in lines, less the header row
        end, used = start, 0.0
        while end < len(cells) and (used + n_lines[end] + 0.6 <= avail or end == start):
            used += n_lines[end] + 0.6
            end += 1
        pages.append((start, end))
        start = end
        if not cells or one_page:
            break
    if one_page:
        f = f.iloc[:pages[0][1]]
    for pg, (a, b) in enumerate(pages):
        part = f.iloc[a:b]
        fig = plt.figure(figsize=(17, H), facecolor=SURFACE)
        fig.suptitle(title, fontsize=13, fontweight="bold", color=INK, x=0.01, y=0.985, ha="left")
        head = (f"Page {first_page + pg}: flags, {len(fdf)} in all (" + ", ".join(f"{k} {counts[k]}" for k in KINDS
                                                                     if k in counts.index) + ")"
                + (f"; the {len(f)} most important here" if len(fdf) > len(f) else "")
                + (f" (continued, {a + 1}-{b})" if pg else ""))
        fig.text(0.01, 0.95, head, fontsize=10, color=MUTED, ha="left", va="top")
        top = flag_key(fig, 0.92) if pg == 0 else 0.92
        ax = fig.add_axes([0.01, 0.02, 0.98, top - 0.02])
        ax.axis("off")
        if not len(part):
            ax.text(0.5, 0.9, "no flags", ha="center", color=MUTED, transform=ax.transAxes)
        else:
            rows = cells[a:b]
            tb = ax.table(cellText=rows, colLabels=cols, cellLoc="left", colLoc="left",
                          colWidths=[0.97 * widths[c] / sum(widths.values()) for c in cols], loc="upper center")
            tb.auto_set_font_size(False)
            tb.set_fontsize(7.5)
            line = LINE_IN / (ax.get_position().height * H)  # one text line, in axes fraction
            total = sum(widths.values())
            for (ri, ci), cell in tb.get_celld().items():
                cell.PAD = pad * total / widths[cols[ci]]  # the same small padding in every column
                nl = n_lines[a + ri - 1] if ri > 0 else 1
                cell.set_height(line * (nl + 0.6))
                cell.set_edgecolor(LINE)
                cell.set_facecolor(SURFACE)
                cell.get_text().set_color(MUTED if ri == 0 else INK)
                if ri > 0 and ci == 0:
                    cell.set_facecolor(KIND_COLOR.get(part.iloc[ri - 1]["kind"], LINE) + "40")
        pdf.savefig(fig, facecolor=SURFACE)
        plt.close(fig)


# one diverging palette per run (rows of the heatmap) and the run's box colour
ULN_NEG, ULN_POS = "#6250d6", "#1baf7a"  # values as multiples of ULN
LLN_NEG, LLN_POS = "#9c5b1f", "#178a94"  # values as multiples of LLN
RUN = {"raw": {"neg": NEG, "pos": POS, "box": SERIES[0], "tag": "raw", "name": "raw values"},
       "uln": {"neg": ULN_NEG, "pos": ULN_POS, "box": ULN_NEG, "tag": "x ULN",
               "name": "multiples of each row's own upper limit (ULN)"},
       "lln": {"neg": LLN_NEG, "pos": LLN_POS, "box": LLN_NEG, "tag": "x LLN",
               "name": "multiples of each row's own lower limit (LLN)"}}


def pick_value_labs_multi(runs, args):
    """Top --top-sp SPs by rows, and the next --top-sp most deviating ones: the largest yearly |shift| in any run
    (raw, x ULN, x LLN; >= --min-cell rows that year; SPs with >= --min-dev-rows rows; ties by rows)."""
    vol = runs["raw"]["cy"].groupby("k_sp")["n_v"].sum().sort_values(ascending=False)
    top = list(vol.head(args.top_sp).index)
    j = pd.concat([c[(c["n_v"] >= args.min_cell) & c["shift"].notna()][["k_sp", "shift"]]
                   for c in (r["cy"] for r in runs.values())])
    dev = j.assign(a=j["shift"].abs()).groupby("k_sp")["a"].max()
    dev = dev[dev.index.isin(vol[vol >= args.min_dev_rows].index) & ~dev.index.isin(top)]
    dev = pd.DataFrame({"a": dev, "n": vol.reindex(dev.index)}).sort_values(["a", "n"], ascending=False)
    return top, list(dev.head(args.top_sp).index), vol


def year_grid(cy, order, years, args):
    grid = np.full((len(order), len(years)), np.nan)
    for r in cy[cy["k_sp"].isin(order)].itertuples():
        if r.n_v >= args.min_cell and pd.notna(r.shift) and pd.notna(r.year) and int(r.year) in years:
            grid[order.index(r.k_sp), years.index(int(r.year))] = r.shift
    return grid


def heatmap_multi(fig, ax, runs, order, args):
    """One row per SP and run: the median vs national per year in raw values (blue-orange), as multiples of the row's
    own ULN (purple-green) and of its own LLN (brown-teal)."""
    sc = list(runs)
    m, n = len(sc), len(order)
    years = sorted(int(y) for y in pd.concat([r["cy"]["year"] for r in runs.values()]).dropna().unique())
    lim = math.log10(1.5)
    grids, ims = [], []
    for i, s in enumerate(sc):
        g = np.full((m * n, len(years)), np.nan)
        g[i::m] = year_grid(runs[s]["cy"], order, years, args)
        grids.append(g)
        cmap = LinearSegmentedColormap.from_list("div", [RUN[s]["neg"], "#e8e6df", RUN[s]["pos"]])
        cmap.set_bad((0, 0, 0, 0))
        ims.append(ax.imshow(np.ma.masked_invalid(np.clip(g, -lim, lim)), cmap=cmap, vmin=-lim, vmax=lim,
                             aspect="auto", interpolation="nearest"))
    for i in range(m * n):
        g = grids[i % m]
        for j in range(len(years)):
            if np.isfinite(g[i, j]):
                ax.text(j, i, fx(g[i, j]), ha="center", va="center", fontsize=6,
                        color=INK if abs(g[i, j]) < 0.6 * lim else SURFACE)
    for i in range(1, n):  # a line between SPs
        ax.axhline(m * i - 0.5, color=SURFACE, lw=2.5)
    ax.set_xticks(range(len(years)))
    ax.set_xticklabels(years, fontsize=8)
    ax.set_xlim(-0.5, len(years) - 0.5)
    ticks = [-lim, -math.log10(1.2), 0, math.log10(1.2), lim]
    # colour bars side by side under the heatmap (axes fractions, so they don't shrink the heatmap)
    w = 1 / m
    for i, (s, im) in enumerate(zip(sc, ims)):
        cax = ax.inset_axes([i * w + 0.1 * w, -0.9 / (fig.get_figheight() * ax.get_position().height), 0.8 * w,
                             0.12 / (fig.get_figheight() * ax.get_position().height)])
        cb = fig.colorbar(im, cax=cax, orientation="horizontal")
        cb.set_ticks(ticks)
        cb.set_ticklabels([fx(t) for t in ticks])
        cb.ax.tick_params(labelsize=7, colors=MUTED)
        cb.outline.set_edgecolor(LINE)
        cb.set_label(RUN[s]["tag"] if s != "raw" else "raw values", fontsize=7.5, color=RUN[s]["box"])
    for sp_ in ax.spines.values():
        sp_.set_visible(False)
    ax.tick_params(colors=MUTED, length=0)
    titled(ax, f"Median vs national per year (same sex x age; blank: under {args.min_cell} rows)",
           "per lab: " + ", ".join(RUN[s]["tag"] for s in sc) + (
               " (multiples of the row's own upper / lower limit)" if len(sc) > 1 else ""))


def boxes_multi(ax, runs, order, h_unit, args):
    """One box per SP and run on one axis, each relative to its run's national median: the raw values, and the values
    as multiples of ULN / LLN. Raw off but x ULN not: the lab's assay or limits, not its values."""
    def q(d, p):
        return 10 ** wq_hist(d["b"].to_numpy() * BIN, d["n"], p)
    sc = list(runs)
    m = len(sc)
    xs, meds = [], {}
    for dy, s in enumerate(sc):
        vals, color = runs[s]["vals"], RUN[s]["box"]
        nat = vals.groupby("b", as_index=False)["n"].sum()
        n50 = q(nat, 0.5) if len(nat) else np.nan
        meds[s] = n50
        for i, k in enumerate(order):
            d = vals[vals["k_sp"] == k]
            y = m * i + dy
            if d["n"].sum() < args.min_cell:
                ax.text(1.01, y, f"under {args.min_cell} rows: not shown", transform=ax.get_yaxis_transform(),
                        va="center", fontsize=7.5, color=MUTED)
                continue
            p5, p25, p50, p75, p95 = (q(d, p) / n50 for p in (0.05, 0.25, 0.5, 0.75, 0.95))
            xs += [p5, p95]
            ax.plot([p5, p25], [y, y], color=color, lw=1.1)
            ax.plot([p75, p95], [y, y], color=color, lw=1.1)
            ax.add_patch(plt.Rectangle((p25, y - 0.32), p75 - p25, 0.64, facecolor=color + "55", edgecolor=color,
                                       lw=1))
            ax.plot([p50, p50], [y - 0.32, y + 0.32], color=INK, lw=2)
            txt = f"median {fnum(p50 * n50)} " + (h_unit or "" if s == "raw" else RUN[s]["tag"].split()[1])
            ax.text(1.01, y, f"{txt}  ({fx(math.log10(p50))})", transform=ax.get_yaxis_transform(), va="center",
                    fontsize=7.5, color=INK)
    ax.axvline(1, color=MUTED, lw=1.2)
    for i in range(1, len(order)):
        ax.axhline(m * i - 0.5, color=LINE, lw=0.6)
    plain_log_x(ax, math.log10(max(xs) / min(xs)) if xs and min(xs) > 0 else None)
    ax.set_ylim(m * len(order) - 0.5, -0.5)
    ax.set_yticks([])
    cols = {"raw": "blue raw values", "uln": "purple x ULN", "lln": "brown x LLN"}
    ax.set_xlabel("value / national median (log scale): " + ", ".join(cols[s] for s in sc), fontsize=8, color=MUTED)
    ax.grid(axis="x", color=LINE, lw=0.5, which="both")
    style(ax)
    nat = [f"{fnum(meds[s])} {h_unit or ''}".strip() if s == "raw" else f"{fnum(meds[s])} {RUN[s]['tag']}"
           for s in sc]
    titled(ax, "Values vs national median: box p25-p75, whiskers p5-p95, black line: median",
           f"national median: {', '.join(nat)}; not adjusted for sex and age")


def page_values_multi(pdf, title, subtitle, order, runs, vol, h_unit, args):
    m = len(runs)
    fig = plt.figure(figsize=(17, 3.4 + 0.47 * m * max(len(order), 1)), facecolor=SURFACE)
    if not order:
        fig.text(0.5, 0.5, "no service providers to show", ha="center", color=MUTED)
    else:
        h = fig.get_figheight()
        gs = fig.add_gridspec(1, 2, width_ratios=[1.25, 1], wspace=0.04, left=0.24, right=0.88, top=1 - 1.2 / h,
                              bottom=1.6 / h)
        ax = fig.add_subplot(gs[0])
        heatmap_multi(fig, ax, runs, order, args)
        ax.set_yticks(range(m * len(order)))
        labels, colors = [], []
        cy = runs["raw"]["cy"]
        for k in order:
            for s in runs:
                if s == "raw":
                    labels.append(sp_label(k, cy, vol).split(" · ")[0])
                else:
                    a = above_pct(cy, k, s)
                    labels.append(f"as {RUN[s]['tag']}" + (
                        "" if a is None else f" · {a:.0f}% " + ("> ULN" if s == "uln" else "< LLN")))
                colors.append(RUN[s]["box"] if s != "raw" else INK)
        ax.set_yticklabels(labels, fontsize=9, color=INK)
        for t, c in zip(ax.get_yticklabels(), colors):
            if c != INK:
                t.set_color(c)
                t.set_fontsize(8)
        boxes_multi(fig.add_subplot(gs[1]), runs, order, h_unit, args)
    fig.suptitle(title, fontsize=13, fontweight="bold", color=INK, x=0.01, y=1 - 0.2 / fig.get_figheight(), ha="left")
    fig.text(0.01, 1 - 0.55 / fig.get_figheight(), subtitle, fontsize=10, color=MUTED, ha="left", va="top")
    pdf.savefig(fig, facecolor=SURFACE)
    plt.close(fig)


def report_multi(omop, title, runs, args, path, h_unit=None):
    """4+ pages: (1) the top SPs by rows and (2) the most deviating others, each with its raw values and as multiples
    of ULN and LLN, one under the other; (3) reference ranges; (4) the flags of all runs."""
    top, dev, vol = pick_value_labs_multi(runs, args)
    raw = runs["raw"]
    what = " and ".join(["its raw values"] + [f"as {RUN[s]['tag']}" for s in runs if s != "raw"])
    with PdfPages(path) as pdf:
        page_values_multi(pdf, title, f"Page 1: the {len(top)} labs with the most rows; each with {what} "
                          f"(multiples of its own upper / lower reference limit)", top, runs, vol, h_unit, args)
        page_values_multi(pdf, title, f"Page 2: the {len(dev)} most deviating other labs (largest yearly median vs "
                          f"national, in any of the rows; >= {args.min_dev_rows} rows)", dev, runs, vol, h_unit, args)
        page_ranges(pdf, title, raw["ranges"], ref_src_share(raw["dims"]), h_unit, args, raw.get("rng"))
        parts = [raw["fdf"].assign(values="raw")]
        for s in runs:  # the limits are the same in every run: their flags come from the raw one
            if s != "raw":
                f = runs[s]["fdf"]
                parts.append(f[~f["kind"].isin(["range", "range_change"])].assign(values=RUN[s]["tag"]))
        pages_flags(pdf, title, pd.concat(parts, ignore_index=True).sort_values("score", ascending=False), args,
                    first_page=4)
    log(f"Wrote {path}")


def report(omop, title, fdf, cy, ranges, vals, args, path, dims=None, h_unit=None, rng=None):
    """A 3-page PDF: the top labs by volume (values), the most deviating ones (values), reference ranges."""
    top, dev, vol = pick_value_labs(cy, args)
    with PdfPages(path) as pdf:
        page_values(pdf, title, f"Page 1: the {len(top)} labs with the most rows, by rows", top, cy, vals, vol,
                    h_unit, args)
        page_values(pdf, title, f"Page 2: the {len(dev)} most deviating other labs (largest yearly median vs "
                                f"national; >= {args.min_dev_rows} rows)", list(dev.index), cy, vals, vol, h_unit, args)
        page_ranges(pdf, title, ranges, ref_src_share(dims) if dims is not None else {}, h_unit, args, rng)
        pages_flags(pdf, title, fdf, args)
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
    stem = args.out / f"sp_qc_{omop}{scale_sfx(args)}"
    fdf.drop(columns=["k_sp", "start", "end"]).to_csv(f"{stem}_flags.tsv", sep="\t", index=False)
    t = cy.assign(year=cy["year"].astype(int), x_shift=cy["shift"].map(fx),
                  outliers_oe=cy["oe_out"].round(2), qc_fail_pct=(100 * cy["qc_rate"]).round(2),
                  outcome_mismatch_pct=(100 * cy["oc_rate"]).round(1), person_jump_oe=cy["oe_jump"].round(2),
                  typical_upper_limit=(10 ** cy["lhi_mode"]).round(4))
    t["above_uln_pct"] = (100 * t["above"] / t["n_hi"].where(t["n_hi"] > 0)).round(1)
    t["below_lln_pct"] = (100 * t["below"] / t["n_lo"].where(t["n_lo"] > 0)).round(1)
    for col, k, n in (("above_uln_pct", "above", "n_hi"), ("below_lln_pct", "below", "n_lo"),
                      ("outliers_oe", "obs_out", "n_v"), ("qc_fail_pct", "qc_fail", "n_qc"),
                      ("outcome_mismatch_pct", "oc_mis", "n_oc"), ("person_jump_oe", "obs_jump", "n_j")):
        t[col] = t[col].astype(object).where(~small(t[k], t[n]), BELOW)
    stats = ["x_shift", "above_uln_pct", "below_lln_pct", "outliers_oe", "qc_fail_pct", "outcome_mismatch_pct",
             "person_jump_oe", "typical_upper_limit"]
    t = t[["sp", "year", "n_v"] + stats].rename(columns={"n_v": "rows"}).sort_values(["sp", "year"])
    mask(t, "rows", stats, args.min_n).to_csv(f"{stem}_sp_year.tsv", sep="\t", index=False)
    if len(ranges):
        share = (100 * ranges["sp_share_at_typical"]).round(1).astype(object)
        share = share.where(~small((ranges["sp_share_at_typical"] * ranges["rows"]).round(), ranges["rows"]), BELOW)
        r = ranges.assign(x_vs_national=ranges["x_vs_national"].map(fx), sp_share_at_typical=share)
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
                   help="The engine's coding_system_map.tsv: fallback SP names when SERVICE_PROVIDER is a real OID (names come "
                        "from the rows' CODING_SYSTEM_MAP)")
    p.add_argument("--names", type=Path, default=names, help="Usagi mapping (concept names)")
    p.add_argument("--out", type=Path, default=OUT, help="Report folder")
    p.add_argument("--cache", type=Path, default=CACHE, help="The extracted rows and the aggregates, per OMOP")
    p.add_argument("--force", action="store_true", help="Re-extract the rows and rebuild the aggregates")
    p.add_argument("--scale", choices=["all", "raw", "uln", "lln"], default="all",
                   help="Values as they are, or as multiples of each row's own upper (uln) / lower (lln) reference "
                        "limit (assay differences drop out; rows without that limit are left out). Outputs get _uln / "
                        "_lln in their names; all (default): raw, uln and lln, in one PDF one under the other")
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
    p.add_argument("--top-sp", type=int, default=10, help="SPs per list in the report (by volume, by deviation)")
    p.add_argument("--min-dev-rows", type=int, default=1000, help="Rows an SP needs to be listed as deviating")
    p.add_argument("--min-scaled", type=float, default=0.05,
                   help="--scale all: share of the rows with a limit needed to draw the x ULN / x LLN rows in the PDF")
    p.add_argument("--table-rows", type=int, default=0,
                   help="Flags in the PDF from page 4 on, over as many pages as they need; 0 (default): as many as fit "
                        "on one page. All flags are in the TSVs")
    p.add_argument("--print-rows", type=int, default=15, help="Flags printed per OMOP in the terminal")
    p.add_argument("--memory-limit", default=f"{int(RAM_GB * 0.45)}GB", help="duckdb memory limit (spills beyond it)")
    p.add_argument("--threads", type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads")
    return p


def main():
    args = build_parser().parse_args()
    global MIN_N
    MIN_N = args.min_n
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
    if (rows / "sp_orgs.parquet").exists():  # names over the whole input first; per-OMOP organisations only fill gaps
        labels.add_orgs(pd.read_parquet(rows / "sp_orgs.parquet"))
    log(f"{len(omops)} OMOP(s): {', '.join(omops)}; out {args.out}")
    scales = ["raw", "uln", "lln"] if args.scale == "all" else [args.scale]
    for omop in omops:
        name = concept_name(omop, args.names)
        res = {}
        for sc in scales:
            a = argparse.Namespace(**{**vars(args), "scale": sc})
            data = build(omop, a, con, rows)
            if data is None:
                continue
            labels.add_orgs(data["orgs"])
            r = data["rng"]
            data["rng"] = r.assign(lhi=np.log10(r["hi_h"].astype(float)), llo=np.log10(r["lo_h"].astype(float)))
            fdf, cy, ranges, nat, series = detect(omop, data, a, labels)
            write_outputs(omop, fdf, cy, ranges, a)
            res[sc] = {"fdf": fdf, "cy": cy, "ranges": ranges, "vals": data["vals"], "dims": data["dims"],
                       "h_unit": data["meta"].get("h_unit"), "rng": data["rng"]}
            counts = fdf["kind"].value_counts()
            print(f"\nOMOP {omop}{' (' + sc + ')' if len(scales) > 1 else ''}: {len(fdf)} flags" + (
                " (" + ", ".join(f"{k} {counts[k]}" for k in KINDS if k in counts.index) + ")" if len(fdf) else ""))
            if len(fdf):
                print(markdown(fdf.head(args.print_rows), ["kind", "sp", "period", "rows", "effect_x", "detail",
                                                           "cause"]))
        if not res:
            continue
        base = f"OMOP {omop}" + (f" — {name}" if name else "") + ": service providers over time"
        if len(scales) > 1 and "raw" in res:
            # a scaled run is drawn when at least --min-scaled of the rows have that limit
            n_raw = res["raw"]["cy"]["n_v"].sum()
            runs = {s: res[s] for s in scales
                    if s in res and (s == "raw" or res[s]["cy"]["n_v"].sum() >= args.min_scaled * n_raw)}
            for s in sorted(set(res) - set(runs)):
                log(f"OMOP {omop}: x {s.upper()} left out of the PDF (under {100 * args.min_scaled:.0f}% of the rows "
                    f"have that limit)")
            report_multi(omop, base, runs, args, args.out / f"sp_qc_{omop}.pdf", res["raw"]["h_unit"])
        else:
            sc = next(iter(res))
            a = argparse.Namespace(**{**vars(args), "scale": sc})
            d = res[sc]
            title = base + (f" (values as multiples of each row's {'upper' if sc == 'uln' else 'lower'} reference "
                            f"limit)" if sc != "raw" else "")
            report(omop, title, d["fdf"], d["cy"], d["ranges"], d["vals"], a,
                   args.out / f"sp_qc_{omop}{scale_sfx(a)}.pdf", d["dims"], d["h_unit"], d["rng"])


if __name__ == "__main__":
    main()
