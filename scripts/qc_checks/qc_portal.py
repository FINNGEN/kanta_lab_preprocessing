#!/usr/bin/env python3
"""
qc_portal.py
A static QC portal for OMOP lab concepts: a folder of HTML pages, inline SVG charts and the reports of sp_qc.py and
omop_ref_audit.py, to put on a shared server (no server code, no external libraries: it works offline).

It only collects: the concepts are those you ran sp_qc.py and / or omop_ref_audit.py for (their reports in --sp-dir /
--audit-dir), with >= --min-rows rows and >= 5 people (fixed, not an option). Two stages (--stage, default both):

  tables  per concept, from sp_qc.py's cached rows (--cache/rows_<input name>/omop=<id>/) and the two scripts' TSVs,
          into --tables/omop_<id>/ (cached; redone when the rows or the reports change, or with --force):
            summary.tsv      people, rows, records per person (median), % people with >= 2 records, % female, age at
                             test (median), test names / units / labs covering >= 5 people, first and last year, the
                             audit's % flagged rows, sp_qc's flags (all runs) by kind
            composition.tsv  per test name x unit x harmonized unit: people, rows, % of rows, median harmonized value,
                             % with a reference range, typical range, % H/L outcome, the audit's % flagged rows (shown
                             groups) and KS D; groups under 5 people pooled into one row
            years.tsv        rows per year; adjacent years pooled until each covers >= 5 people
            labs.tsv         the concept's labs (from sp_qc's lab-years) vs their share of all results
            audit_*.tsv      the audit's figure data (its cached, gated tables; histograms and map as shares only)
            lab_order.tsv    the 20 largest and the 20 most deviating labs (from sp_qc's lab-years)
            lab_values.tsv   those labs' value quantiles vs national, per run, from sp_qc's cached value bins
            lab_limits.tsv   those labs' typical adult reference limits per year x sex (>= 5 people carry them)
  site    from those tables only, into --out (built in <out>.part, then swapped in):
            index.html       one row per concept: search (names, OMOP ids, test names), sort, filters
            omop/<id>.html   the summary, the report cards, results per year, when the samples are taken, the
                             composition table, the audit and the lab report drawn in the page (no images: SVG from
                             the tables, the lab x year heatmap from embedded data), the concept's labs; omop/<id>/
                             holds the copied reports (PDF, TSV) and the concept's TSVs
            omops.tsv        the landing table

Privacy (a hard condition, as in sp_qc.py): every published number covers >= 5 people.
  - Every table is gated when it is built: counts of >= 5 people, shares whose part and rest are each none or
    >= 5 people, medians with >= 5 people on each side; composition groups under 5 people are pooled (secondary
    suppression keeps the pool none or >= 5 people), so the shown rows plus the pool add up to the total; years and
    value bins are pooled with their neighbours, rows without a year join a pool when they are a small group.
  - The site only reads the gated tables; the reports of sp_qc.py and the audit are copied as they are (both gate
    on people), their TSVs checked again on the way.
  - A last scan over the built site (every TSV, every count in the HTML) refuses the build if a count of 1-4 appears.

Usage
-----
  python3 qc_portal.py [--min-rows N] [--omop ID ...] [--stage tables|site|all] [--out DIR]
"""
import argparse
import html
import json
import os
import re
import shutil
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sp_qc as S  # noqa: E402  (row cache, people sketches, publish_tsv)
import omop_ref_audit as A  # noqa: E402  (concept info, verdicts)

MIN_N = 5  # fixed: every published number covers >= 5 people
BELOW = S.BELOW
S.MIN_N = MIN_N
TABLES = Path("/mnt/disks/data/kanta/qc/portal_tables")
SITE = Path("/mnt/disks/data/kanta/qc/portal")
VERSION = "p10"
CELL = ["test", "unit", "h_unit"]
ABNORMAL = ("H", "L", "HH", "LL", "A", "AA")
COUNTS = ("rows", "people", "n")
LABS_VERSION = "l12"
NONE_ORG, OTHER_ORG = "no organisation", "other organisations"
t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


class Progress:
    """A one-line progress bar on a terminal (i of n, elapsed, ETA, the current item), redrawn in place; when the
    output is not a terminal (a log file, tee), a plain log line every 10% instead."""

    def __init__(self, what, n):
        self.what, self.n, self.start, self.tty = what, max(n, 1), time.time(), sys.stdout.isatty()
        self.step = max(1, self.n // 10)

    def update(self, i, item=""):
        """Before item i (1-based) of n."""
        el = time.time() - self.start
        if not self.tty:
            if i == 1 or i % self.step == 0:
                log(f"{self.what}: {i} of {self.n} {item}")
            return
        done = i - 1
        eta = f"  ETA {self.hms(el / done * (self.n - done))}" if done else ""
        w = 30
        fill = done * w // self.n
        line = (f"[{time.time() - t0:7.1f}s] {self.what} [{'#' * fill}{'-' * (w - fill)}] {i}/{self.n} "
                f"{self.hms(el)}{eta}  {item}")
        cols = shutil.get_terminal_size((120, 20)).columns
        print("\r\033[K" + line[:cols - 1], end="", flush=True)

    def close(self):
        if self.tty:
            print(f"\r\033[K[{time.time() - t0:7.1f}s] {self.what}: {self.n} of {self.n} in {self.hms(time.time() - self.start)}",
                  flush=True)

    @staticmethod
    def hms(sec):
        sec = int(sec)
        return f"{sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"


def npeople(sketches):
    return len(S.pset(sketches))


# ---------------------------------------------------------------------------
# Which concepts: whatever sp_qc.py / omop_ref_audit.py were run for
# ---------------------------------------------------------------------------

def report_files(omop, args):
    """The concept's report files: (kind, path) for the audit and sp_qc outputs (pdf / tsv; the pages draw the rest)."""
    sp = [p for p in sorted(args.sp_dir.glob(f"sp_qc_{omop}_*")) + sorted(args.sp_dir.glob(f"sp_qc_{omop}.pdf"))
          if p.suffix in (".pdf", ".tsv")] if args.sp_dir.is_dir() else []
    au = [p for p in sorted(args.audit_dir.glob(f"omop_{omop}_ref_audit*"))
          if p.suffix in (".pdf", ".tsv")] if args.audit_dir.is_dir() else []
    return [("sp", p) for p in sp] + [("audit", p) for p in au]


def find_omops(args):
    ids = set()
    if args.sp_dir.is_dir():
        ids |= {m.group(1) for p in args.sp_dir.glob("sp_qc_*") if (m := re.match(r"sp_qc_(\d+)[_.]", p.name))}
    if args.audit_dir.is_dir():
        ids |= {m.group(1) for p in args.audit_dir.glob("omop_*_ref_audit*")
                if (m := re.match(r"omop_(\d+)_ref_audit", p.name))}
    if args.omop:
        want = {x for o in args.omop for x in o.split(",") if x.strip()}
        ids &= want
    return sorted(ids, key=int)


# ---------------------------------------------------------------------------
# Tables (gated; from the cached rows and the reports)
# ---------------------------------------------------------------------------

def wmedian(x, w):
    o = np.argsort(np.asarray(x, float))
    x, w = np.asarray(x, float)[o], np.asarray(w, float)[o]
    cw = np.cumsum(w)
    return x[min(np.searchsorted(cw, 0.5 * cw[-1]), len(x) - 1)] if len(x) else np.nan


def safe_median_bin(d):
    """The weighted median bin of d (b, n, p) when >= MIN_N people lie at or below it and at or above it, else None."""
    if d.empty:
        return None
    med = wmedian(d["b"], d["n"])
    safe = S.safe_bins(d[["b", "p"]])
    return med if safe is not None and safe[0] <= med <= safe[1] else None


def fmt_lim(lo, hi):
    f = A.fmt
    if pd.notna(lo) and pd.notna(hi):
        return f"{f(lo)}–{f(hi)}"
    return f"<{f(hi)}" if pd.notna(hi) else (f">{f(lo)}" if pd.notna(lo) else "")


def share(k, pk, c, pc):
    """k / (k + c) in %, when the part and the rest are each none or >= MIN_N people; else None."""
    if k + c == 0 or not S.share_ok(k, pk, c, pc):
        return None
    return 100.0 * k / (k + c)


def audit_info(omop, files):
    """From the audit TSVs: per composition cell the % flagged rows of its shown groups; per test KS D / differs; the
    concept's % flagged rows (shown groups, without 'no value > 0')."""
    path = {p.name: p for k, p in files if k == "audit"}
    cell, tests, flagged = {}, {}, None
    g = path.get(f"omop_{omop}_ref_audit_groups.tsv")
    if g is not None:
        d = pd.read_csv(g, sep="\t", dtype=str, keep_default_na=False)
        d["n"] = pd.to_numeric(d["n"], errors="coerce")
        d = d[d["n"].notna() & (d["verdict"] != A.NO_VALUE)]
        d["fl"] = d["verdict"].isin(A.FLAGGED)
        for k, x in d.groupby(CELL):
            cell[k] = 100 * x.loc[x["fl"], "n"].sum() / x["n"].sum()
        if d["n"].sum():
            flagged = 100 * d.loc[d["fl"], "n"].sum() / d["n"].sum()
    t = path.get(f"omop_{omop}_ref_audit_tests.tsv")
    if t is not None:
        d = pd.read_csv(t, sep="\t", dtype=str, keep_default_na=False)
        tests = {r["test"]: (float(r["D (KS)"]), r["differs"] == "yes") for _, r in d.iterrows() if r["D (KS)"]}
    return cell, tests, flagged


def sp_info(omop, files):
    """sp_qc's flags (all runs: raw, x ULN, x LLN) by kind."""
    kinds = {}
    for k, p in files:
        if k == "sp" and p.name.endswith("_flags.tsv"):
            d = pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False)
            for kd, n in d["kind"].value_counts().items():
                kinds[kd] = kinds.get(kd, 0) + int(n)
    return kinds


def omop_tables(omop, part, con, args, files, ids=None):
    """The gated tables of one concept, or None when it covers fewer than MIN_N people / --min-rows rows."""
    R = f"read_parquet({S.sql_str(str(part / '*.parquet'))})"
    X = "coalesce(test, 'NA') test, coalesce(unit, 'NA') unit, coalesce(h_unit, 'NA') h_unit"
    q = lambda sql: con.sql(sql).df()  # noqa: E731
    sk = lambda keys, src: q(S.sketch_sql(keys, src))  # noqa: E731
    pp = q(f"select n, count(*) people from (select f, count(*) n from {R} where f is not null group by f) "
           f"group by n order by n")
    people = int(pp["people"].sum())
    rows = int(q(f"select count(*) n from {R}")["n"].iloc[0])
    if people < MIN_N or rows < args.min_rows:
        return None

    # --- composition: test name x unit x harmonized unit
    cells = sk(CELL, f"(select {X}, f from {R})")
    cp = q(f"select test, unit, h_unit, count(*) people from (select {X}, f from {R} where f is not null group by all) "
           f"group by all")
    cells = cells.merge(cp, on=CELL, how="left").fillna({"people": 0}).sort_values("n", ascending=False,
                                                                                    ignore_index=True)
    ok = np.array(cells["people"] >= MIN_N, dtype=bool)
    ok = S.suppress_group(cells[["n", "p"]], ok)  # the pool: none or >= MIN_N people
    cells["shown"] = ok
    ref = sk(CELL + ["has_ref"], f"(select {X}, (ref_low is not null or ref_high is not null) has_ref, f from {R})")
    rmode = sk(CELL + ["lo", "hi"], f"(select {X}, round(ref_low, 4) lo, round(ref_high, 4) hi, f from {R} "
                                    f"where ref_low is not null or ref_high is not null)")
    ab = ", ".join(S.sql_str(x) for x in ABNORMAL)
    oc = sk(CELL + ["ab"], f"(select {X}, upper(outcome) in ({ab}) ab, f from {R} where outcome is not null)")
    vb = sk(CELL + ["b"], f"(select {X}, round(log10(v) / 0.01)::int b, f from {R} where v > 0)")
    a_cell, a_tests, a_flagged = audit_info(omop, files)
    out = []
    for c in cells[cells["shown"]].itertuples():
        key = (c.test, c.unit, c.h_unit)
        sel = lambda d: d[(d["test"] == c.test) & (d["unit"] == c.unit) & (d["h_unit"] == c.h_unit)]  # noqa: E731
        r = sel(ref)
        yes, no = r[r["has_ref"]], r[~r["has_ref"]]
        with_ref = share(yes["n"].sum(), npeople(yes["p"]), no["n"].sum(), npeople(no["p"]))
        m = sel(rmode).sort_values("n", ascending=False)
        typ = fmt_lim(m["lo"].iloc[0], m["hi"].iloc[0]) if len(m) and npeople([m["p"].iloc[0]]) >= MIN_N else ""
        o = sel(oc)
        y, n_ = o[o["ab"]], o[~o["ab"]]
        hl = share(y["n"].sum(), npeople(y["p"]), n_["n"].sum(), npeople(n_["p"])) if len(o) else None
        mb = safe_median_bin(sel(vb))
        d = a_tests.get(c.test)
        out.append({"test": c.test, "unit": c.unit, "h_unit": c.h_unit, "people": int(c.people), "rows": int(c.n),
                    "pct_rows": 100 * c.n / rows, "median": 10 ** (mb * 0.01) if mb is not None else None,
                    "with_ref_pct": with_ref, "typical_range": typ, "outcome_hl_pct": hl,
                    "audit_flagged_pct": a_cell.get(key), "ks_d": d[0] if d else None,
                    "differs": ("yes" if d[1] else "no") if d else ""})
    comp = pd.DataFrame(out)
    hid = cells[~cells["shown"]]
    if len(hid):  # the pool: >= MIN_N people by construction (checked again here)
        assert npeople(hid["p"]) >= MIN_N
        comp = pd.concat([comp, pd.DataFrame([{
            "test": f"(pooled: {len(hid)} test name / unit group{'s' if len(hid) > 1 else ''})", "unit": "",
            "h_unit": "", "people": None, "rows": int(hid["n"].sum()), "pct_rows": 100 * hid["n"].sum() / rows}])],
            ignore_index=True)

    # --- years: adjacent years pooled until each covers >= MIN_N people; undated rows on their own when they are a
    # big enough group, else in the largest pool (so the years always add up to the total)
    yr = sk(["year"], f"(select year, f from {R})")
    dated, undated = yr[yr["year"].notna()], yr[yr["year"].isna()]
    pools = S.pool_adjacent(dated, "year")
    if pools.empty:
        pools = pd.DataFrame([{"lo": dated["year"].min(), "hi": dated["year"].max(), "n": int(dated["n"].sum())}])
    pools["note"] = ""
    nu = int(undated["n"].sum())
    if nu:
        if npeople(undated["p"]) >= MIN_N:
            pools = pd.concat([pools, pd.DataFrame([{"lo": None, "hi": None, "n": nu, "note": "no date"}])],
                              ignore_index=True)
        else:
            i = pools["n"].idxmax()
            pools.loc[i, "n"] += nu
            pools.loc[i, "note"] = "incl. rows without a date"
    years = pools.rename(columns={"lo": "year_from", "hi": "year_to", "n": "rows"}).astype(
        {"year_from": "Int64", "year_to": "Int64", "rows": "int64"})


    # --- summary
    cum = pp["people"].cumsum()
    med_n = int(pp["n"].iloc[int(np.searchsorted(cum, people / 2))])
    rpp = med_n if pp.loc[pp["n"] <= med_n, "people"].sum() >= MIN_N and pp.loc[pp["n"] >= med_n, "people"].sum() \
        >= MIN_N else None
    one = int(pp.loc[pp["n"] == 1, "people"].sum())
    ge2 = (100.0 * (people - one) / people) if (one == 0 or one >= MIN_N) and \
        (people - one == 0 or people - one >= MIN_N) else None
    sx = q(f"select sex, count(*) people from (select sex, f from {R} where f is not null and sex is not null "
           f"group by all) group by sex")
    nf, nm = int(sx.loc[sx["sex"] == 0, "people"].sum()), int(sx.loc[sx["sex"] == 1, "people"].sum())
    female = 100.0 * nf / (nf + nm) if nf + nm and (nf == 0 or nf >= MIN_N) and (nm == 0 or nm >= MIN_N) else None
    ag = sk(["b"], f"(select floor(age)::int b, f from {R} where age is not null)")
    age = safe_median_bin(ag)
    labs = int(q(f"select count(*) n from (select sp from (select sp, f from {R} where f is not null and sp is not "
                 f"null group by all) group by sp having count(*) >= {MIN_N})")["n"].iloc[0])
    shown_c = comp[comp["people"].notna()]
    hu = shown_c.groupby("h_unit")["rows"].sum()
    name, quantity, target = A.concept_info(omop, args.names, args.harmonization_counts)
    kinds = sp_info(omop, files)
    dated_years = years[years["year_from"].notna()]
    summary = pd.DataFrame([{
        "omop": omop, "name": name or "", "quantity": quantity or "",
        "h_unit": target or (hu.idxmax() if len(hu) else ""), "people": people, "rows": rows,
        "records_per_person": rpp, "ge2_records_pct": ge2, "female_pct": female,
        "age_median": int(age) if age is not None else None,
        "test_names": shown_c["test"].nunique(), "units": shown_c["unit"].nunique(), "labs": labs,
        "year_first": int(dated_years["year_from"].min()) if len(dated_years) else None,
        "year_last": int(dated_years["year_to"].max()) if len(dated_years) else None,
        "audit": any(k == "audit" for k, _ in files), "audit_flagged_pct": a_flagged,
        "sp": any(k == "sp" for k, _ in files), "sp_flags": sum(kinds.values()) if kinds else 0,
        "sp_flag_kinds": ", ".join(f"{k} {v}" for k, v in sorted(kinds.items(), key=lambda kv: -kv[1]))}])
    out = {"summary": summary, "composition": comp, "years": years, "labs": concept_labs(omop, files, ids)}
    out |= audit_tables(omop, args)
    spy = next((p for k, p in files if k == "sp" and p.name == f"sp_qc_{omop}_sp_year.tsv"), None)
    if spy is not None:
        order = lab_order(relabel(pd.read_csv(spy, sep="\t", dtype=str, keep_default_na=False), ids))
        out["lab_order"] = order
        out["lab_values"] = lab_values(omop, args, ids, list(order["label"]))
        rgp = next((p for k, p in files if k == "sp" and p.name == f"sp_qc_{omop}_ranges.tsv"), None)
        rg_labels = []
        if rgp is not None:
            rg = relabel(pd.read_csv(rgp, sep="\t", dtype=str, keep_default_na=False), ids)
            rg_labels = list(rg.loc[rg["age"] == "adult", "sp"].unique())
        out["lab_limits"] = lab_limits(omop, args, ids, list(dict.fromkeys(list(order["label"]) + rg_labels)))
    return out


def sp_tail(label):
    """The pseudonym tail sp_qc prints after a service provider's name (…61b3c1): the same whatever it is named."""
    return str(label).rsplit("…", 1)[-1].strip()


def relabel(df, ids, col="sp"):
    """sp_qc's service-provider names in df[col] replaced by the current ones (ids: a table with a label column), matched
    by the pseudonym tail: reports written before a naming change line up with the lab tables. Others unchanged."""
    if df is None or ids is None or not len(ids) or col not in df.columns:
        return df
    by_tail = {sp_tail(x): x for x in ids["label"]}
    df = df.copy()
    df[col] = [by_tail.get(sp_tail(x), x) if "…" in str(x) else x for x in df[col]]
    return df


def lab_id_of(label, used):
    """A lab's page id: the pseudonym tail sp_qc prints after its name (…8f0f6e), -2, -3 … on a clash."""
    tail = re.sub(r"[^A-Za-z0-9_-]", "_", label.rsplit("…", 1)[-1].strip()) or "lab"
    lid, i = tail, 1
    while lid in used:
        i += 1
        lid = f"{tail}-{i}"
    used.add(lid)
    return lid


def gate_cells(c):
    """Organisation cells (org, n, p) of one whole (a lab, or a lab's period), for shares (these tables have no counts):
    the shown cells (>= MIN_N people), the rows of the hidden ones when together they cover >= MIN_N people ("other
    organisations"), and the rows dropped from the whole's base when they cover fewer (no share of a small group, and
    no big organisation hidden to cover for it). Returns (shown, other_n, dropped_n)."""
    c = c.reset_index(drop=True)
    ok = np.array([npeople([p]) >= MIN_N for p in c["p"]], dtype=bool)
    hid = c[~ok]
    big = len(hid) > 0 and npeople(hid["p"]) >= MIN_N
    return c[ok], int(hid["n"].sum()) if big else 0, 0 if big else int(hid["n"].sum())


def org_cells(d):
    c = d.groupby("org", as_index=False)["n"].sum()
    c["p"] = S.merge_sketch(d, ["org"]).reindex(c["org"]).to_numpy()
    return c


def region_table(con, root, labels, labs, sps, pop_file):
    """Per maakunta: the summed share of all results of the service providers whose region it is, and its population
    (Statistics Finland, scripts/sote/data/maakunta_population.tsv). Percentages and official numbers only."""
    cols = ["region", "results_pct", "population", "pop_year"]
    reg = labs[labs["region"].astype(str) != ""]
    if reg.empty:
        return pd.DataFrame(columns=cols)
    out = reg.groupby("region", as_index=False)["pct_results"].sum().rename(columns={"pct_results": "results_pct"})
    out["results_pct"] = out["results_pct"].round(3)
    if pop_file is not None and Path(pop_file).exists():
        pop = pd.read_csv(pop_file, sep="\t").rename(columns={"maakunta": "region", "year": "pop_year"})
        out = out.merge(pop[["region", "population", "pop_year"]], on="region", how="left")
    else:
        out["population"], out["pop_year"] = None, None
    return out.sort_values("results_pct", ascending=False)[cols]


def lab_tables(args, con):
    """The labs (SPs) over the results of every OMOP in the row cache, into --tables/labs/: labs.tsv (one row per lab of >= MIN_N people: its
    label as sp_qc names it, people, % of the listed labs' results, years, the organisation it is named after and that
    organisation's share, % without an organisation), lab_orgs.tsv (its organisations' shares, small ones pooled),
    lab_years.tsv (the shares per period, years pooled to >= MIN_N people; the lab's top 4 organisations, other, none).
    No row counts: shares and people only, so nothing can be summed against sp_qc's lab-year rows. Returns the private
    id table (k_sp, lab_id, label, n), kept in labs/ids.parquet and never published."""
    rargs = argparse.Namespace(input=args.input, cache=args.cache, force=False, extract_batch=40)
    oy, pp = S.sp_composition(rargs, con)
    root = args.cache / f"rows_{Path(args.input).stem}"
    ldir = args.tables / "labs"
    srcs = [root / "sp_org_year.parquet", root / "sp_people.parquet", root / S.SP_ORGS]
    if args.sp_names:
        srcs.append(args.sp_names)
    srcs.append(args.population)
    want = {"version": LABS_VERSION, "src": {str(p): p.stat().st_mtime for p in srcs if p.exists()}}
    meta_p = ldir / "meta.json"
    if meta_p.exists() and (ldir / "ids.parquet").exists() and (ldir / "regions.tsv").exists() and not args.force and \
            json.loads(meta_p.read_text()).get("want") == want:
        log(f"Labs: cached ({ldir})")
        return pd.read_parquet(ldir / "ids.parquet")
    labels = S.load_labels(argparse.Namespace(sp_names=args.sp_names, org_map=S.ORG_MAP), root)
    oy = oy[oy["k_sp"].notna()].copy()
    oy["org"] = oy["org"].fillna(NONE_ORG).astype(str).str.replace("_", " ")
    people = pp.set_index("k_sp")["people"]
    n_sp = oy.groupby("k_sp")["n"].sum()
    n_sp = n_sp[people.reindex(n_sp.index).fillna(0).to_numpy() >= MIN_N].sort_values(ascending=False)
    oy = oy[oy["k_sp"].isin(n_sp.index)]
    tot = n_sp.sum()
    # every sketch unpacked once: (lab, org, year, person), at most SKETCH persons per cell; people of any union of
    # cells = distinct persons over them, capped (exact for the >= MIN_N test)
    e = oy[["k_sp", "org", "year", "p"]].explode("p")
    e = e[e["p"].notna()].astype({"p": "uint64"})

    def cells(keys, rows, ex):
        """rows (n) and people (pp) per keys, and per keys[:-1] the rows and people of the hidden cells together."""
        c = rows.groupby(keys, as_index=False)["n"].sum()
        c = c.merge(ex.groupby(keys)["p"].nunique().rename("pp").reset_index(), on=keys, how="left")
        c["pp"] = c["pp"].fillna(0)
        c["shown"] = c["pp"] >= MIN_N
        hid = c[~c["shown"]]
        up = keys[:-1]
        h = hid.groupby(up)["n"].sum().rename("hid_n").to_frame()
        h["hid_pp"] = ex.merge(hid[keys], on=keys).groupby(up)["p"].nunique()
        return c, h.fillna({"hid_pp": 0})

    # --- organisations per lab (all years)
    oc, oh = cells(["k_sp", "org"], oy, e)
    # --- periods per lab: adjacent years pooled until each covers >= MIN_N people
    dated = oy[oy["year"].notna()]
    ed = e[e["year"].notna()]
    yset = ed.groupby(["k_sp", "year"])["p"].agg(set)
    yrows = dated.groupby(["k_sp", "year"])["n"].sum()
    period = {}  # (lab, year) -> its period, as first year * 10000 + last year (one index level)
    for sp, g in yrows.groupby(level=0):
        out, cur, seen = [], [], set()
        for (_, y), _n in g.sort_index().items():
            cur.append(y)
            seen |= yset.get((sp, y), set())
            if len(seen) >= MIN_N:
                out.append(cur)
                cur, seen = [], set()
        if cur and out:
            out[-1] += cur
        for ys in out:
            for y in ys:
                period[(sp, y)] = int(min(ys)) * 10000 + int(max(ys))
    key = [period.get(k) for k in zip(dated["k_sp"], dated["year"])]
    dated = dated.assign(per=key)
    dated = dated[dated["per"].notna()]
    ed = ed.assign(per=[period.get(k) for k in zip(ed["k_sp"], ed["year"])])
    ed = ed[ed["per"].notna()]
    pc, ph = cells(["k_sp", "per", "org"], dated, ed)

    labs, orgs, years, ids, used = [], [], [], [], set()
    oc_by, pc_by = dict(tuple(oc.groupby("k_sp"))), dict(tuple(pc.groupby("k_sp")))
    for sp, n in n_sp.items():
        n = int(n)
        label = labels.sp(sp)
        lid = lab_id_of(label, used)
        c = oc_by[sp]
        shown = c[c["shown"]].sort_values("n", ascending=False)
        hid_n, hid_pp = (oh.loc[sp, "hid_n"], oh.loc[sp, "hid_pp"]) if sp in oh.index else (0, 0)
        other = int(hid_n) if hid_pp >= MIN_N else 0
        base = n - (0 if other else int(hid_n))  # a small group of hidden organisations: out of the share base
        orgs += [{"lab_id": lid, "org": r.org, "pct": 100 * r.n / base} for r in shown.itertuples()]
        if other:
            orgs.append({"lab_id": lid, "org": OTHER_ORG, "pct": 100 * other / base})
        code = S.ORG_CODE.match(str(sp))
        named = (labels.code.get(code.group(1)) if code else None) or labels.org.get(sp)
        n_named = int(shown.loc[shown["org"] == named, "n"].sum()) if named else 0
        n_none = int(shown.loc[shown["org"] == NONE_ORG, "n"].sum())
        with_org = base - n_none
        named_pct = 100 * n_named / with_org if n_named and with_org else None
        no_org = 100 * n_none / base if base else None
        # the region as in scripts/sote's sp_region.tsv: reported when its rows are >= REGION_MIN of the rows with an
        # organisation (shares of rows; no counts are published for service providers)
        reg = labels.region.get(sp)
        region = reg[0] if reg and reg[2] >= S.Labels.REGION_MIN else None
        top4 = [o for o in shown["org"] if o != NONE_ORG][:4]
        pers = pc_by.get(sp)
        firsts, lasts = [], []
        if pers is not None:
            n_dated = 0
            rowsets = []
            for per, g in pers.groupby("per"):
                hid_n, hid_pp = (ph.loc[(sp, per), "hid_n"], ph.loc[(sp, per), "hid_pp"]) \
                    if (sp, per) in ph.index else (0, 0)
                other_n = int(hid_n) if hid_pp >= MIN_N else 0
                n_pl = int(g["n"].sum()) - (0 if other_n else int(hid_n))
                buckets = {}
                for r in g[g["shown"]].itertuples():
                    k = r.org if r.org in top4 or r.org == NONE_ORG else OTHER_ORG
                    buckets[k] = buckets.get(k, 0) + int(r.n)
                if other_n:
                    buckets[OTHER_ORG] = buckets.get(OTHER_ORG, 0) + other_n
                per = (int(per) // 10000, int(per) % 10000)
                rowsets.append((per, n_pl, buckets))
                n_dated += n_pl
                firsts.append(per[0])
                lasts.append(per[1])
            for per, n_pl, buckets in rowsets:
                years += [{"lab_id": lid, "year_from": per[0], "year_to": per[1], "org": k, "share": v / n_pl,
                           "pct_of_lab": 100 * n_pl / n_dated} for k, v in buckets.items() if n_pl]
        labs.append({"lab_id": lid, "label": label, "people": int(people[sp]), "pct_results": 100 * n / tot,
                     "year_first": min(firsts) if firsts else None, "year_last": max(lasts) if lasts else None,
                     "named_after": named or "", "named_pct": named_pct, "no_org_pct": no_org,
                     "region": region or "", "region_pct": round(100 * reg[1], 1) if region else None,
                     "regional_pct": round(100 * reg[2], 1) if region else None})
        ids.append({"k_sp": sp, "lab_id": lid, "label": label, "n": n, "region": labels.region_name(sp) or ""})
    ldir.mkdir(parents=True, exist_ok=True)
    S.publish_tsv(pd.DataFrame(labs), ldir / "labs.tsv", count_cols=COUNTS)
    S.publish_tsv(pd.DataFrame(orgs, columns=["lab_id", "org", "pct"]), ldir / "lab_orgs.tsv", count_cols=COUNTS)
    S.publish_tsv(pd.DataFrame(years, columns=["lab_id", "year_from", "year_to", "org", "share", "pct_of_lab"]),
                  ldir / "lab_years.tsv", count_cols=COUNTS)
    ids = pd.DataFrame(ids, columns=["k_sp", "lab_id", "label", "n", "region"])
    ids.to_parquet(ldir / "ids.parquet", index=False)
    S.publish_tsv(region_table(con, root, labels, pd.DataFrame(labs), list(n_sp.index), args.population),
                  ldir / "regions.tsv", count_cols=COUNTS)
    meta_p.write_text(json.dumps({"want": want}, indent=1) + "\n")
    log(f"Labs: {len(labs)} lab(s) of >= {MIN_N} people -> {ldir}")
    return ids



def concept_labs(omop, files, ids):
    """The concept's labs from sp_qc's published lab-years (raw run): per lab the sum of its shown years, as % of the
    shown labs' rows, against the same labs' shares of all results (re-based to them) and the ratio; rows: the sum of
    the lab's shown years in that published table (each >= MIN_N people), so nothing that isn't public already."""
    spy = next((p for k, p in files if k == "sp" and p.name == f"sp_qc_{omop}_sp_year.tsv"), None)
    cols = ["lab_id", "label", "rows", "pct_concept", "pct_dataset", "ratio", "year_first", "year_last"]
    if spy is None or ids is None or ids.empty:
        return pd.DataFrame(columns=cols)
    d = relabel(pd.read_csv(spy, sep="\t", dtype=str, keep_default_na=False), ids)
    d["r"] = pd.to_numeric(d["rows"], errors="coerce")
    d["y"] = pd.to_numeric(d["year"], errors="coerce")
    d = d[d["r"].notna() & (d["r"] > 0)]
    g = d.groupby("sp").agg(rows=("r", "sum"), year_first=("y", "min"), year_last=("y", "max")).reset_index()
    g = g.merge(ids.drop_duplicates("label"), left_on="sp", right_on="label", how="inner")
    if g.empty:
        return pd.DataFrame(columns=cols)
    g["pct_concept"] = 100 * g["rows"] / g["rows"].sum()
    g["pct_dataset"] = 100 * g["n"] / g["n"].sum()
    g["ratio"] = g["pct_concept"] / g["pct_dataset"]
    g = g.astype({"year_first": "Int64", "year_last": "Int64", "rows": "int64"})
    return g.sort_values("pct_concept", ascending=False)[cols]


def audit_tables(omop, args):
    """The audit's figure data, from its cached, already gated tables (--cache/audit/omop=<id>/): offset histograms and
    decision-map points as shares only (their row sums could be set against other totals), verdict and stage bars
    (sums of shown groups, as in its _groups.tsv), the flagged groups, and the figure's parameters. {} when the audit
    was not run for the concept."""
    adir = args.cache / "audit" / f"omop={omop}"
    meta_p = adir / "tables.json"
    if not meta_p.exists():
        return {}
    meta = json.loads(meta_p.read_text())
    t = {n: pd.read_parquet(adir / f"t_{n}.parquet") for n in ("offsets", "map", "vbars", "stages", "flagged")
         if (adir / f"t_{n}.parquet").exists()}
    out = {}
    if "offsets" in t:
        out["audit_offsets"] = t["offsets"][["check", "test", "lo", "hi", "share"]]
    if "map" in t and len(t["map"]):
        m = t["map"]
        out["audit_map"] = m.assign(share=m["rows"] / m["rows"].sum())[["bx", "by", "verdict", "share"]]
    for n in ("vbars", "stages", "flagged"):
        if n in t:
            out[f"audit_{n}"] = t[n]
    st = meta.get("stats", {})
    out["audit_meta"] = {"keep": meta.get("keep", []), "rtol": meta.get("rtol"), "tol_v": meta.get("tol_v"),
                         "tol_vr": meta.get("tol_vr"), "median": st.get("median"),
                         "target": meta.get("params", {}).get("target"), "flagged_pct": meta.get("flagged_pct")}
    return out


def lab_order(spy, n=20):
    """From sp_qc's published lab-years (raw run): the n labs with the most rows, and the n most deviating others
    (largest yearly median vs national, over their shown years). Labels only, no counts."""
    d = spy.copy()
    d["r"] = pd.to_numeric(d["rows"], errors="coerce")
    d = d[d["r"].notna()]
    d["dev"] = [abs(parse_x(x)) if parse_x(x) is not None else np.nan for x in d["x_shift"]]
    g = d.groupby("sp").agg(rows=("r", "sum"), dev=("dev", "max")).reset_index()
    top = list(g.sort_values("rows", ascending=False)["sp"].head(n))
    dev = list(g[~g["sp"].isin(top)].sort_values("dev", ascending=False)["sp"].head(n))
    return pd.DataFrame({"label": top + dev, "group": ["largest"] * len(top) + ["deviating"] * len(dev)})


def lab_values(omop, args, ids, labels, min_rows=100):
    """Each listed lab's values, per sp_qc run (raw, x ULN, x LLN), from sp_qc's cached value bins (log10, with people
    sketches): p5 / p25 / median / p75 / p95 relative to the national median, each kept between the lowest and highest
    bins with >= MIN_N people beyond them (as sp_qc's boxes); labs under min_rows rows or too few people left out. A
    'national' row per run holds the national median."""
    k_of = dict(zip(ids["label"], ids["k_sp"])) if ids is not None and len(ids) else {}
    out = []
    for run, sfx in (("raw", ""), ("uln", "_uln"), ("lln", "_lln")):
        p = args.cache / f"omop={omop}{sfx}" / "vals.parquet"
        if not p.exists():
            continue
        v = pd.read_parquet(p)
        nat = v.groupby("b", as_index=False)["n"].sum()
        med = 10 ** S.wq_hist(nat["b"].to_numpy() * S.BIN, nat["n"], 0.5)
        out.append({"run": run, "label": "national", "median": med})
        by = dict(tuple(v.groupby("k_sp")))
        for lab in labels:
            d = by.get(k_of.get(lab))
            if d is None or d["n"].sum() < min_rows:
                continue
            sb = S.safe_bins(d)
            if sb is None:
                continue
            lo, hi = 10 ** (sb[0] * S.BIN), 10 ** (sb[1] * S.BIN)
            q = {f"q{int(round(x * 100)):02d}": min(max(10 ** S.wq_hist(d["b"].to_numpy() * S.BIN, d["n"], x), lo), hi)
                 / med for x in (0.05, 0.25, 0.5, 0.75, 0.95)}
            out.append({"run": run, "label": lab, **q})
    return pd.DataFrame(out, columns=["run", "label", "median", "q05", "q25", "q50", "q75", "q95"])


def lab_limits(omop, args, ids, labels):
    """The typical adult reference limits per lab x year x sex (upper and lower; harmonized unit), from sp_qc's cached
    limits (with people sketches): a lab-year's most common limit, shown when >= MIN_N people carry it (as sp_qc's
    reference-range page); and the national typical limit per sex (label 'national'). Limit values, no counts."""
    p = args.cache / f"omop={omop}" / "rng.parquet"
    if not p.exists():
        return pd.DataFrame(columns=["label", "year", "sex", "limit", "value", "share"])
    r = pd.read_parquet(p)
    r = r[(r["ag"] == "adult") & r["year"].notna()]
    lab_of = {k: lab for lab, k in zip(ids["label"], ids["k_sp"]) if lab in set(labels)} if ids is not None else {}
    out = []
    for lim, col in (("upper", "hi_h"), ("lower", "lo_h")):
        d = r[r[col].notna()]
        if d.empty:
            continue
        nat = S.group_mode_p(d, ["sx"], col)
        nat = nat[nat["P_mode"] >= MIN_N]
        out += [{"label": "national", "year": None, "sex": "F" if str(x.sx) == "0" else "M", "limit": lim,
                 "value": x.mode, "share": x.mode_share} for x in nat.itertuples()]
        m = S.group_mode_p(d[d["k_sp"].isin(lab_of)], ["k_sp", "year", "sx"], col)
        m = m[m["P_mode"] >= MIN_N]
        out += [{"label": lab_of[x.k_sp], "year": int(x.year), "sex": "F" if str(x.sx) == "0" else "M", "limit": lim,
                 "value": x.mode, "share": x.mode_share} for x in m.itertuples()]
    return pd.DataFrame(out, columns=["label", "year", "sex", "limit", "value", "share"]).astype({"year": "Int64"})


def stage_tables(args):
    omops = find_omops(args)
    log(f"Tables: {len(omops)} concept(s) with sp_qc / audit reports in {args.sp_dir} / {args.audit_dir}")
    if not omops:
        return []
    (args.cache / "duckdb").mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"set memory_limit = '{args.memory_limit}'")
    con.sql(f"set threads = {args.threads}")
    con.sql(f"set temp_directory = '{args.cache / 'duckdb'}'")
    con.sql("set preserve_insertion_order = false")
    root = args.cache / f"rows_{Path(args.input).stem}"
    ids = lab_tables(args, con)
    labs_mtime = (args.tables / "labs" / "meta.json").stat().st_mtime
    kept, dropped = [], {}
    for omop in omops:
        part = root / f"omop={omop}"
        if not part.is_dir():
            dropped[omop] = f"no cached rows in {root} (run sp_qc.py or omop_ref_audit.py with --omop {omop})"
            continue
        files = report_files(omop, args)
        tdir = args.tables / f"omop_{omop}"
        want = {"version": VERSION, "min_rows": args.min_rows, "rows_mtime": S.part_mtime(part),
                "input": str(Path(args.input).resolve()), "labs_mtime": labs_mtime,
                "aux": {str(p): p.stat().st_mtime for p in [args.cache / "audit" / f"omop={omop}" / "tables.json"] +
                        [args.cache / f"omop={omop}{x}" / "vals.parquet" for x in ("", "_uln", "_lln")] +
                        [args.cache / f"omop={omop}" / "rng.parquet"] if p.exists()},
                "files": {str(p): p.stat().st_mtime for _, p in files}}
        meta_p = tdir / "meta.json"
        if meta_p.exists() and not args.force and json.loads(meta_p.read_text()).get("want") == want:
            meta = json.loads(meta_p.read_text())
            if meta.get("dropped"):
                dropped[omop] = meta["dropped"]
            else:
                kept.append(omop)
            continue
        t = omop_tables(omop, part, con, args, files, ids)
        tdir.mkdir(parents=True, exist_ok=True)
        for p in [*tdir.glob("*.tsv"), *tdir.glob("audit_meta.json")]:
            p.unlink()
        if t is None:
            why = f"under {args.min_rows} rows or fewer than {MIN_N} people"
            meta_p.write_text(json.dumps({"want": want, "dropped": why}, indent=1) + "\n")
            dropped[omop] = why
            continue
        for name, df in t.items():
            if isinstance(df, dict):  # figure parameters (no counts)
                (tdir / f"{name}.json").write_text(json.dumps(df, default=float) + "\n")
            else:
                S.publish_tsv(df, tdir / f"{name}.tsv", count_cols=COUNTS)
        meta_p.write_text(json.dumps({"want": want, "files": [[k, str(p)] for k, p in files],
                                      "release": Path(args.input).stem,
                                      "built": time.strftime("%Y-%m-%d %H:%M")}, indent=1) + "\n")
        kept.append(omop)
        log(f"OMOP {omop}: tables -> {tdir}")
    for o, why in dropped.items():
        log(f"OMOP {o}: left out: {why}")
    log(f"Tables: {len(kept)} concept(s) kept, {len(dropped)} left out")
    return kept


# ---------------------------------------------------------------------------
# Site (from the tables only)
# ---------------------------------------------------------------------------

E = html.escape


def num(x):
    return x is not None and not (isinstance(x, float) and np.isnan(x)) and x != ""


def int_s(x):
    return f"{int(x):,}"


def short(n):
    return (f"{n / 1e6:.0f}M" if n >= 1e7 else f"{n / 1e6:.1f}M") if n >= 1e6 else \
        (f"{n / 1e3:.0f}k" if n >= 1e4 else f"{n / 1e3:.1f}k") if n >= 1e3 else f"{n:.0f}"


def pct_s(x, d=1):
    return f"{x:.{d}f}%" if num(x) else '<span class="below" title="BELOW_THRESHOLD">below threshold</span>'


def count_td(n):
    """A count cell: the number with data-n (checked by the last scan), or nothing."""
    if not num(n):
        return '<td class="num"></td>'
    n = int(n)
    return f'<td class="num" data-n="{n}" data-sort="{n}">{n:,}</td>'


def sev_chip(v):
    if not num(v):
        return '<span class="muted">—</span>'
    v = float(v)
    cls, word = ("crit", "Many") if v >= 5 else ("warn", "Some") if v >= 1 else ("ok", "Few")
    return f'<span class="chip {cls}" title="{word} rows flagged by the audit">{v:.{2 if v < 1 else 1}f}%</span>'


def fnum(v):
    a = abs(v)
    d = 0 if a >= 100 else 1 if a >= 10 else 2 if a >= 1 else 3 if a >= 0.01 else 5
    return f"{v:.{d}f}".rstrip("0").rstrip(".") if "." in f"{v:.{d}f}" else f"{v:.{d}f}"


def year_svg(years):
    d = years[years["year_from"].notna()].astype({"year_from": int, "year_to": int}).sort_values("year_from")
    if d.empty:
        return ""
    W, H, ml, mr, mt, mb = 1300, 230, 46, 10, 10, 26
    x0, x1 = d["year_from"].min(), d["year_to"].max() + 1
    per = d["rows"] / (d["year_to"] - d["year_from"] + 1)
    ymax = per.max() * 1.08
    X = lambda y: ml + (y - x0) / (x1 - x0) * (W - ml - mr)  # noqa: E731
    Y = lambda v: H - mb - v / ymax * (H - mt - mb)  # noqa: E731
    step = 10 ** np.floor(np.log10(ymax / 2))
    tick = np.ceil(ymax / 3 / step) * step
    g = ['<defs><pattern id="hatch" width="5" height="5" patternUnits="userSpaceOnUse" '
         'patternTransform="rotate(45)"><rect width="5" height="5" fill="var(--surface)"/>'
         '<line x1="0" y1="0" x2="0" y2="5" stroke="var(--accent)" stroke-width="2.2"/></pattern></defs>']
    v = tick
    while v < ymax:
        g.append(f'<line x1="{ml}" x2="{W - mr}" y1="{Y(v):.1f}" y2="{Y(v):.1f}" stroke="var(--line)"/>'
                 f'<text x="{ml - 6}" y="{Y(v) + 4:.1f}" text-anchor="end">{short(v)}</text>')
        v += tick
    g.append(f'<line x1="{ml}" x2="{W - mr}" y1="{Y(0):.1f}" y2="{Y(0):.1f}" stroke="var(--muted)"/>')
    for (r, pv) in zip(d.itertuples(), per):
        pooled = r.year_to > r.year_from
        xa, xb, y = X(r.year_from) + 1, X(r.year_to + 1) - 1, Y(pv)
        lab = f"{r.year_from}" + (f"–{r.year_to} (pooled)" if pooled else "")
        tip = f"{lab}: {int(r.rows):,} rows" + (f", {int(pv):,} per year" if pooled else "") + \
            (f" ({r.note})" if isinstance(r.note, str) and r.note else "")
        g.append(f'<path d="M{xa:.1f},{Y(0):.1f} V{y + 3:.1f} q0,-3 3,-3 H{xb - 3:.1f} q3,0 3,3 V{Y(0):.1f} Z" '
                 f'fill="{"url(#hatch)" if pooled else "var(--accent)"}" data-n="{int(r.rows)}">'
                 f'<title>{E(tip)}</title></path>')
    span = x1 - x0
    every = 4 if span > 12 else 2 if span > 6 else 1
    y = int(np.ceil(x0 / every) * every)
    while y < x1:
        g.append(f'<text x="{(X(y) + X(y + 1)) / 2:.1f}" y="{H - 8}" text-anchor="middle">{y}</text>')
        y += every
    lg = ""
    if (d["year_to"] > d["year_from"]).any():
        lg = ('<div class="lg"><span><i class="hatchkey"></i>years pooled to cover at least 5 people '
              '(rows per year shown)</span></div>')
    und = years[years["year_from"].isna()]
    if len(und):
        lg += f'<div class="lg"><span>rows without a date: {int(und["rows"].iloc[0]):,}</span></div>'
    return (f'<div class="chart"><h3>Results per year</h3><p class="q">When were the results taken?</p>'
            f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Results per year">{"".join(g)}</svg>{lg}</div>')


CSS = """/* A quiet lab-report shell: one wide column, tables first. System fonts only (the site runs offline). */
:root {
  --bg: #f6f6f3; --surface: #ffffff; --ink: #191917; --muted: #5d5c56; --line: #dedcd3; --soft: #efeee9;
  --accent: #2a6fc4; --accent-ink: #1d4f8f; --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a; --s4: #eda100;
  --s5: #e87ba4; --s6: #008300; --s7: #6250d6; --s8: #e34948;
  --other: #a8a69c; --none: #d9d7cf; --up: #2e9e57; --down: #d03b3b; --fem: #d6589f;
  --warn-bg: #fdf1d6; --warn-ink: #7a5300; --crit-bg: #fbe1df; --crit-ink: #9c2420; --ok-bg: #e3f3e8; --ok-ink: #1d6b39;
  --font: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --mono: ui-monospace, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #141413; --surface: #1c1c1a; --ink: #edece7; --muted: #a9a8a0; --line: #36352f; --soft: #252522;
  --accent: #6ea6ec; --accent-ink: #a9cbf5; --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
  --s5: #d55181; --s6: #008300; --s7: #9085e9; --s8: #e66767;
  --other: #77766e; --none: #45443f; --up: #4cc47a; --down: #e66767; --fem: #e87ba4;
  --warn-bg: #3a2f12; --warn-ink: #f0c869; --crit-bg: #42201d; --crit-ink: #f3a19b; --ok-bg: #18301f; --ok-ink: #8fd3a5;
  color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #141413; --surface: #1c1c1a; --ink: #edece7; --muted: #a9a8a0; --line: #36352f; --soft: #252522;
  --accent: #6ea6ec; --accent-ink: #a9cbf5; --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
  --s5: #d55181; --s6: #008300; --s7: #9085e9; --s8: #e66767;
  --other: #77766e; --none: #45443f; --up: #4cc47a; --down: #e66767; --fem: #e87ba4;
  --warn-bg: #3a2f12; --warn-ink: #f0c869; --crit-bg: #42201d; --crit-ink: #f3a19b; --ok-bg: #18301f; --ok-ink: #8fd3a5;
  color-scheme: dark; }
* { box-sizing: border-box; }
[hidden] { display: none !important; }
body { background: var(--bg); color: var(--ink); font: 17px/1.55 var(--font); margin: 0; }
a { color: var(--accent); text-decoration-thickness: 1px; text-underline-offset: 2px; }
a:hover { color: var(--accent-ink); }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 3px; }
.wrap { max-width: max(80vw, 1240px); margin: 0 auto; padding-left: max(16px, env(safe-area-inset-left, 0px));
  padding-right: max(16px, env(safe-area-inset-right, 0px)); }
.mono { font-family: var(--mono); font-size: 0.93em; }
.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
.muted { color: var(--muted); }
.top { border-bottom: 1px solid var(--line); background: var(--surface); }
.top .wrap { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 20px; padding-block: 12px; }
.brand { font-weight: 650; font-size: 19.5px; color: var(--ink); text-decoration: none; letter-spacing: -0.01em; }
.brand span { color: var(--muted); font-weight: 450; }
.release { color: var(--muted); font-size: 16px; }
.release b { font-family: var(--mono); font-weight: 500; color: var(--ink); font-size: 14.5px; }
main { padding-block: 24px 64px; display: grid; grid-template-columns: minmax(0, 1fr); gap: 28px; }
h1 { font-size: 31px; line-height: 1.25; margin: 0; letter-spacing: -0.015em; text-wrap: balance; font-weight: 650; }
h2 { font-size: 20.5px; margin: 0 0 10px; font-weight: 650; display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap; }
h2 small { font-weight: 400; color: var(--muted); font-size: 16px; }
.lead { margin: 6px 0 0; color: var(--muted); }
.controls { display: flex; flex-wrap: wrap; gap: 10px 18px; align-items: center; margin-bottom: 10px; }
.search { flex: 1 1 280px; max-width: 460px; padding: 8px 11px; font: inherit; color: var(--ink); background: var(--surface);
  border: 1px solid var(--line); border-radius: 6px; }
.controls label { display: flex; align-items: center; gap: 6px; font-size: 16px; color: var(--muted); cursor: pointer; }
.count { font-size: 16px; color: var(--muted); margin-left: auto; font-variant-numeric: tabular-nums; }
.tablebox { overflow-x: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; }
table { border-collapse: collapse; width: 100%; font-size: 16px; }
th, td { padding: 7px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { text-align: left; font-weight: 600; font-size: 15px; color: var(--muted); background: var(--soft); white-space: nowrap;
  cursor: pointer; user-select: none; }
th.num { text-align: right; }
th[aria-sort="ascending"]::after { content: " ▲"; font-size: 10.5px; }
th[aria-sort="descending"]::after { content: " ▼"; font-size: 10.5px; }
tbody tr:last-child td { border-bottom: 0; }
tbody tr:hover td { background: var(--soft); }
tr.pinned td { color: var(--muted); font-style: italic; background: var(--soft); }
td.name { min-width: 260px; }
td.name a { font-weight: 550; }
td .sub { display: block; color: var(--muted); font-size: 15px; }
.below { font-size: 12.5px; color: var(--muted); letter-spacing: 0.02em; white-space: nowrap; }
.chip { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 15px; font-variant-numeric: tabular-nums;
  white-space: nowrap; background: var(--soft); color: var(--muted); }
.chip.ok { background: var(--ok-bg); color: var(--ok-ink); }
.chip.warn { background: var(--warn-bg); color: var(--warn-ink); }
.chip.crit { background: var(--crit-bg); color: var(--crit-ink); }
.pill { display: inline-block; padding: 2px 8px; border: 1px solid var(--line); border-radius: 5px; font-size: 15px;
  text-decoration: none; margin-right: 4px; white-space: nowrap; background: var(--surface); }
.pill.off { color: var(--muted); border-style: dashed; }
.crumb { font-size: 16px; }
.idline { display: flex; flex-wrap: wrap; gap: 6px 14px; margin-top: 8px; color: var(--muted); font-size: 16px; align-items: center; }
.idline .mono { color: var(--ink); }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; }
.tile { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; min-width: 0; }
.tile .k { font-size: 15px; color: var(--muted); }
.tile .v { font-size: 26.5px; font-weight: 600; font-variant-numeric: tabular-nums; letter-spacing: -0.01em; }
.tile .d { font-size: 15px; color: var(--muted); }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(340px, 100%), 1fr)); gap: 14px; align-items: start; }
.card { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; display: grid;
  gap: 10px; align-content: start; min-width: 0; }
.card h3 { margin: 0; font-size: 18.5px; font-weight: 650; display: flex; gap: 8px; align-items: baseline; flex-wrap: wrap; }
.card p { margin: 0; color: var(--muted); font-size: 16px; }
.files { display: flex; flex-wrap: wrap; gap: 6px 10px; }
.files a { font-family: var(--mono); font-size: 15px; overflow-wrap: anywhere; min-width: 0; }
.stack { display: flex; height: 12px; border-radius: 3px; overflow: hidden; gap: 2px; background: var(--surface); }
.stack span { display: block; height: 100%; }
.keyrow { display: flex; flex-wrap: wrap; gap: 4px 12px; font-size: 15px; color: var(--muted); }
.keyrow i { display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 4px; vertical-align: -1px; }
.flaglist { display: flex; flex-wrap: wrap; gap: 6px; }
.charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(340px, 100%), 1fr)); gap: 14px; }
td.links { white-space: nowrap; }
.chart { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; min-width: 0; }
.chart h3 { margin: 0; font-size: 18.5px; font-weight: 650; }
.chart .q { margin: 2px 0 6px; font-size: 16px; color: var(--muted); }
.chart svg { width: 100%; height: auto; display: block; max-width: 2200px; }
.chart svg text { fill: var(--muted); font: 15px var(--font); }
.chart .lg { display: flex; flex-wrap: wrap; gap: 4px 14px; font-size: 15.5px; color: var(--muted); margin-top: 4px; }
.chart .lg i { display: inline-block; width: 14px; height: 3px; border-radius: 2px; margin-right: 5px; vertical-align: 3px; }
.chart .lg { align-items: center; }
.chart .lg i.dotkey { display: inline-block; border-radius: 50%; background: var(--muted); opacity: 0.6;
  vertical-align: middle; margin-right: 5px; }
.chart .lg i.hatchkey { height: 9px; width: 12px; vertical-align: -1px;
  background: repeating-linear-gradient(45deg, var(--accent) 0 2px, transparent 2px 4px); }
code { font-family: var(--mono); font-size: 15px; background: var(--soft); padding: 1px 5px; border-radius: 4px; }
.charts.wide { grid-template-columns: minmax(0, 1fr); }
.charts.timing { grid-template-columns: minmax(0, 3fr) minmax(0, 1.2fr); margin-top: 10px; }
@media (max-width: 900px) { .charts.timing, .charts[style] { grid-template-columns: minmax(0, 1fr) !important; } }
.timing-head { display: flex; flex-wrap: wrap; gap: 8px 18px; align-items: baseline; margin-top: 18px; }
h3.sub { margin: 0; font-size: 18.5px; font-weight: 650; }
.pick { font-size: 16px; color: var(--muted); display: flex; gap: 6px; align-items: center; }
.pick { flex-wrap: wrap; min-width: 0; }
.pick select { font: inherit; color: var(--ink); background: var(--surface); border: 1px solid var(--line);
  border-radius: 5px; padding: 3px 6px; max-width: 100%; }
.stackv { display: grid; gap: 22px; grid-template-columns: minmax(0, 1fr); }
p.how { margin: 2px 0 10px; font-size: 16.5px; color: var(--muted); }
p.how b { color: var(--ink); font-weight: 600; margin-right: 4px; }
td.wrap { white-space: normal; min-width: 260px; max-width: 520px; }
details.kinds { font-size: 16px; color: var(--muted); margin: 0 0 10px; }
details.kinds summary { cursor: pointer; color: var(--accent); }
.hm svg text { font-size: 16px; }
.chart svg.wide-text text { font-size: 17px; }
.panel { display: grid; gap: 28px; grid-template-columns: minmax(0, 1fr); padding-top: 14px; }
td.sharecell { padding-top: 4px; padding-bottom: 4px; }
td.sharecell svg { display: block; width: 210px; max-width: none; height: 18px; }
td.strip svg { display: block; }
table[id^="rng"] td:first-child, table[id^="fl"] td:nth-child(2) { min-width: 240px; }
.mapcard .fimap { max-width: 300px; margin: 6px auto 0; }
.fimap svg path[data-href] { cursor: pointer; }
.chart svg.pie { display: block; width: 100%; max-width: 380px; margin: auto; padding: 12px 0; }
.pie path, .pie circle { stroke: var(--surface); stroke-width: 2; }
.pie a:hover path, .pie a:hover circle, .pie a:focus path { opacity: .78; }
a.spc { font-weight: 600; text-decoration: underline; text-decoration-thickness: 1px; text-underline-offset: 2px; }
a.spc:hover { text-decoration-thickness: 2px; }
.fimap svg path { fill: var(--none); stroke: var(--surface); stroke-width: 0.8; }
.fimap svg path.on { fill: var(--accent); }
.fimap svg path:hover { stroke: var(--ink); stroke-width: 1.2; }
.swatch { display: inline-block; width: 12px; height: 12px; border-radius: 3px; margin-right: 6px;
  vertical-align: -1px; border: 1px solid var(--line); }
.choro svg path { stroke: var(--surface); stroke-width: 0.8; }
.covbar { display: inline-block; width: 160px; height: 12px; background: var(--soft); border-radius: 3px; overflow: hidden;
  vertical-align: middle; margin-right: 8px; }
.covbar i { display: block; height: 100%; background: var(--accent); }
ol.toplist { margin: 8px 0 12px; padding-left: 26px; display: grid; gap: 6px; }
ol.toplist li { display: grid; grid-template-columns: minmax(150px, 220px) minmax(0, 1fr) 70px; gap: 10px; align-items: center; }
ol.toplist .rb { height: 12px; background: var(--soft); border-radius: 3px; overflow: hidden; }
ol.toplist .rb i { display: block; height: 100%; background: var(--accent); border-radius: 3px; }
.limgrid { display: grid; grid-template-columns: repeat(auto-fill, minmax(min(380px, 100%), 1fr)); gap: 12px; }
.limcard h3 { font-size: 15px; }
.limcard .q { min-height: 2.6em; }
.showmore { margin-top: 10px; font: inherit; color: var(--accent); background: var(--surface); border: 1px solid var(--line);
  border-radius: 6px; padding: 6px 14px; cursor: pointer; }
.pager { display: flex; gap: 12px; align-items: center; justify-content: flex-end; margin-top: 8px; font-size: 16px;
  color: var(--muted); }
.pager button { font: inherit; color: var(--accent); background: var(--surface); border: 1px solid var(--line);
  border-radius: 5px; padding: 3px 10px; cursor: pointer; }
.pager button:disabled { color: var(--muted); opacity: 0.5; cursor: default; }
.tabs { display: flex; flex-wrap: wrap; gap: 8px; border-bottom: 2px solid var(--line); margin-bottom: 14px; }
.tabs button { font: inherit; font-weight: 650; font-size: 25.5px; color: var(--muted); background: var(--soft);
  border: 1px solid var(--line); border-bottom: 0; border-radius: 8px 8px 0 0; padding: 12px 26px; margin-bottom: -2px;
  cursor: pointer; }
.tabs button:hover { color: var(--ink); }
.tabs button[aria-selected="true"] { color: var(--ink); background: var(--surface); box-shadow: inset 0 3px 0 var(--accent); }
.ratio-hi { color: var(--crit-ink); font-weight: 600; }
footer { font-size: 16px; color: var(--muted); }
dl.defs { display: grid; grid-template-columns: max-content 1fr; gap: 4px 14px; margin: 8px 0 0; }
dl.defs dt { font-weight: 600; color: var(--ink); }
dl.defs dd { margin: 0; }
@media (max-width: 560px) { dl.defs { grid-template-columns: 1fr; } dl.defs dd { margin-bottom: 6px; } }

/* the naming figure (Service Providers tab) */
.namecases { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(300px, 100%), 1fr)); gap: 16px; }
.namecase { border: 1px solid var(--line); border-top: 4px solid var(--accent); border-radius: 8px; padding: 10px 12px 6px; background: var(--surface); }
.namecase h4 { margin: 0 0 6px; font-size: 14px; white-space: nowrap; }
@media (max-width: 760px) { .namecase h4 { white-space: normal; } }
.namefig { overflow-x: auto; }
.chart .namefig svg { min-width: 0; max-width: none; width: 100%; margin: 0 auto; }
.namefig .nd { fill: var(--surface); stroke: var(--line); stroke-width: 1.2; }
.namefig .nd.use { stroke: var(--accent); stroke-width: 1.8; }
.namefig .ed.use { stroke: var(--accent); stroke-opacity: .7; }
.namefig .nd.out { fill: var(--soft); stroke: var(--accent); stroke-width: 2; }
.namefig .hd { fill: var(--muted); font: 600 10.5px var(--font); letter-spacing: .05em; paint-order: stroke; stroke: var(--surface); stroke-width: 5px; stroke-linejoin: round; }
.namefig .t { fill: var(--ink); font: 12.5px var(--font); }
.namefig .tb { fill: var(--ink); font: 600 14px var(--font); }
.namefig .ts { fill: var(--muted); font: 12px var(--font); }
.namefig .tc { fill: var(--muted); font: 11px var(--mono); }
.namefig .yt { font-weight: 700; }
.namefig .tsp { fill: var(--ink); font: 600 16px var(--font); }
.namefig .ed { fill: none; stroke: var(--line); stroke-width: 1.5; }
"""

JS = """// Sort any table.sortable by clicking a header; filter its rows with the search box and the checkboxes.
(function () {
  "use strict";
  function val(tr, i) {
    var td = tr.cells[i]; if (!td) return null;
    var s = td.getAttribute("data-sort");
    if (s !== null) { var n = parseFloat(s); return isNaN(n) ? s.toLowerCase() : n; }
    var t = td.textContent.trim().toLowerCase(); return t === "" || t === "—" ? null : t;
  }
  document.querySelectorAll("table.sortable").forEach(function (t) {
    var tb = t.tBodies[0], ths = Array.prototype.slice.call(t.tHead.rows[0].cells);
    ths.forEach(function (th, i) {
      th.tabIndex = 0;
      function go() {
        var asc = th.getAttribute("aria-sort") !== "ascending";
        ths.forEach(function (h) { h.removeAttribute("aria-sort"); });
        th.setAttribute("aria-sort", asc ? "ascending" : "descending");
        var rows = Array.prototype.slice.call(tb.rows);
        var pinned = rows.filter(function (r) { return r.classList.contains("pinned"); });
        rows.filter(function (r) { return !r.classList.contains("pinned"); }).sort(function (a, b) {
          var x = val(a, i), y = val(b, i);
          if (x === null && y === null) return 0; if (x === null) return 1; if (y === null) return -1;
          return (x < y ? -1 : x > y ? 1 : 0) * (asc ? 1 : -1);
        }).concat(pinned).forEach(function (r) { tb.appendChild(r); });
        run();  // a row limit shows the top rows of the new order
      }
      th.addEventListener("click", go);
      th.addEventListener("keydown", function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(); } });
    });
    var q = document.querySelector('[data-search="' + t.id + '"]');
    var count = document.querySelector('[data-count="' + t.id + '"]');
    var checks = Array.prototype.slice.call(document.querySelectorAll('[data-require][data-table="' + t.id + '"]'));
    // pages: data-limit rows per page (default 25), with Previous / Next under the table; the search and the filters
    // look through every row, and the pages run over the matches in the current order
    var size = parseInt(t.getAttribute("data-limit") || "25", 10), page = 0;
    var pager = document.createElement("div");
    pager.className = "pager";
    pager.innerHTML = '<button type="button" data-p="-1">← Previous</button><span></span><button type="button" data-p="1">Next →</button>';
    (t.closest(".tablebox") || t).insertAdjacentElement("afterend", pager);
    pager.addEventListener("click", function (e) {
      var d = e.target.getAttribute && e.target.getAttribute("data-p");
      if (d) { page += parseInt(d, 10); run(true); }
    });
    function run(keepPage) {
      if (keepPage !== true) page = 0;
      var terms = (q ? q.value : "").toLowerCase().split(/\\s+/).filter(Boolean), all = 0, hit = [];
      Array.prototype.forEach.call(tb.rows, function (tr) {
        if (tr.classList.contains("pinned")) return; all++;
        var txt = tr.getAttribute("data-text") || tr.textContent.toLowerCase();
        var ok = terms.every(function (w) { return txt.indexOf(w) >= 0; }) &&
          checks.every(function (c) { return !c.checked || tr.getAttribute("data-" + c.getAttribute("data-require")) === "1"; });
        tr.hidden = true;
        if (ok) hit.push(tr);
      });
      var pages = Math.max(1, Math.ceil(hit.length / size));
      page = Math.max(0, Math.min(page, pages - 1));
      hit.slice(page * size, (page + 1) * size).forEach(function (tr) { tr.hidden = false; });
      var a = hit.length ? page * size + 1 : 0, b = Math.min(hit.length, (page + 1) * size);
      if (count) count.textContent = (hit.length <= size ? hit.length + " shown" : a + "–" + b + " of " + hit.length) +
        (hit.length < all ? " (of " + all + ")" : "");
      pager.hidden = pages < 2;
      pager.querySelector("span").textContent = "Page " + (page + 1) + " of " + pages;
      pager.querySelector('[data-p="-1"]').disabled = page === 0;
      pager.querySelector('[data-p="1"]').disabled = page >= pages - 1;
    }
    if (q) q.addEventListener("input", run);
    checks.forEach(function (c) { c.addEventListener("change", run); });
    run();
  });
  // lab x year heatmaps: drawn from the embedded data; the controls also pick the value boxes
  function rgb(v) { v = v.trim(); if (v[0] === "#") { var n = parseInt(v.slice(1), 16); return [n >> 16 & 255, n >> 8 & 255, n & 255]; } return [200, 200, 200]; }
  function mix(a, b, t) { return "rgb(" + a.map(function (x, i) { return Math.round(x + (b[i] - x) * t); }).join(",") + ")"; }
  function esc(s) { return String(s).replace(/[&<>"]/g, function (c) { return {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]; }); }
  document.querySelectorAll("[data-heatmap]").forEach(function (box) {
    var data = JSON.parse(document.getElementById(box.getAttribute("data-heatmap")).textContent);
    var css = getComputedStyle(document.documentElement);
    var neg = rgb(css.getPropertyValue("--s1")), pos = rgb(css.getPropertyValue("--s2")), mid = rgb(css.getPropertyValue("--soft")),
        acc = rgb(css.getPropertyValue("--accent"));
    var selO = box.querySelector('select[name="order"]'), selM = box.querySelector('select[name="metric"]');
    function color(m, t) {
      if (m.kind === "ratio" || m.kind === "limit") { var u = Math.max(-1, Math.min(1, t / m.vmax)); return mix(mid, u >= 0 ? pos : neg, Math.abs(u)); }
      if (m.kind === "pct") return mix(mid, acc, Math.max(0, Math.min(1, t / m.vmax)));
      return mix(mid, pos, Math.max(0, Math.min(1, t)));  // observed / expected: log10, 0 (x1) to 1 (x10)
    }
    function draw() {
      var m = data.metrics.filter(function (x) { return x.key === selM.value; })[0];
      var labs = data.labs[selO.value] || [], years = data.years, cells = data.cells[m.key] || {};
      var lw = 290, cw = Math.max(60, Math.min(110, 820 / Math.max(years.length, 1))), rh = 32, top = 26;
      var W = lw + cw * years.length + 10, H = top + rh * labs.length + 6;
      var g = ['<defs><pattern id="hb' + box.id + '" width="5" height="5" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="5" height="5" fill="var(--soft)"/><line x1="0" y1="0" x2="0" y2="5" stroke="var(--line)" stroke-width="2"/></pattern></defs>'];
      years.forEach(function (y, j) { g.push('<text x="' + (lw + cw * j + cw / 2) + '" y="18" text-anchor="middle">' + y + '</text>'); });
      labs.forEach(function (l, i) {
        var y = top + rh * i, name = l[0].length > 32 ? l[0].slice(0, 31) + "…" : l[0];
        var lab = l[2] ? '<a href="../lab/' + esc(l[1]) + '.html"><text x="' + (lw - 8) + '" y="' + (y + rh / 2 + 4) + '" text-anchor="end" fill="var(--accent)">' + esc(name) + '</text></a>'
                       : '<text x="' + (lw - 8) + '" y="' + (y + rh / 2 + 4) + '" text-anchor="end">' + esc(name) + '</text>';
        g.push(lab);
        var row = cells[l[0]] || {};
        years.forEach(function (yr, j) {
          var c = row[String(yr)], x = lw + cw * j;
          if (c === undefined) return;
          if (c === "B") { g.push('<rect x="' + (x + 1) + '" y="' + (y + 1) + '" width="' + (cw - 2) + '" height="' + (rh - 2) + '" fill="url(#hb' + box.id + ')"><title>' + esc(l[0]) + ' ' + yr + ': below threshold</title></rect>'); return; }
          var f = color(m, c[0]);
          g.push('<rect x="' + (x + 1) + '" y="' + (y + 1) + '" width="' + (cw - 2) + '" height="' + (rh - 2) + '" rx="2" fill="' + f + '"><title>' + esc(l[0]) + ' ' + yr + ': ' + esc(c[1]) + '</title></rect>' +
                 '<text x="' + (x + cw / 2) + '" y="' + (y + rh / 2 + 4) + '" text-anchor="middle" style="fill:var(--ink)">' + esc(c[1]) + '</text>');
        });
      });
      box.querySelector(".hm").innerHTML = '<svg viewBox="0 0 ' + W + ' ' + H + '" role="img" aria-label="Service Providers by year">' + g.join("") + '</svg>';
      box.querySelector(".hm-how").textContent = m.how;
      var lg = m.kind === "pct" ? "light: 0% · dark: " + m.vmax.toFixed(0) + "% or more" :
               m.kind === "oe" ? "light: as expected (×1) · orange: ×10 or more" :
               "blue: lower · orange: higher · full colour at ×" + Math.pow(10, m.vmax).toFixed(2) + (m.kind === "limit" ? " (vs the national limit " + m.nat + ")" : " (vs national)");
      box.querySelector(".hm-lg").textContent = lg;
      var run = m.run, key = selO.value + "|" + run;
      document.querySelectorAll('[data-boxes="' + box.id + '"]').forEach(function (e) { e.hidden = e.getAttribute("data-key") !== key; });
    }
    selO.addEventListener("change", draw); selM.addEventListener("change", draw);
    draw();
  });
  // a region's page name, as qc_portal.region_slug writes it
  function slug(k) { return k.toLowerCase().replace(/[äå]/g, "a").replace(/ö/g, "o").replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, ""); }
  // region maps: the maakunnat from assets/finland.js, the named one highlighted
  var FM = window.FINLAND_MAP || {};
  document.querySelectorAll("[data-map-region]").forEach(function (el) {
    if (!FM.regions) { el.textContent = ""; return; }
    var on = el.getAttribute("data-map-region");
    el.innerHTML = '<svg viewBox="' + FM.viewBox + '" role="img" aria-label="Map of Finland, ' + on + ' highlighted">' +
      Object.keys(FM.regions).map(function (k) {
        return '<path d="' + FM.regions[k] + '"' + (k === on ? ' class="on"' : '') + '><title>' + k + '</title></path>';
      }).join("") + '</svg>';
  });
  // the landing page's choropleth: each maakunta coloured by the picked measure (light to dark, one hue)
  document.querySelectorAll("[data-choro]").forEach(function (el) {
    if (!FM.regions) { el.textContent = ""; return; }
    var v = JSON.parse(el.getAttribute("data-choro")), card = el.closest(".chart");
    var sel = card ? card.querySelector("[data-choro-mode]") : null, kind = el.getAttribute("data-choro-kind") || "cov";
    var link = el.getAttribute("data-choro-link");
    el.addEventListener("click", function (e) { var h = e.target.getAttribute && e.target.getAttribute("data-href"); if (h) location.href = h; });
    var up = rgb(getComputedStyle(document.documentElement).getPropertyValue("--up")),
        down = rgb(getComputedStyle(document.documentElement).getPropertyValue("--down"));
    var css = getComputedStyle(document.documentElement), lo = rgb(css.getPropertyValue("--soft")), hi = rgb(css.getPropertyValue("--accent"));
    function draw() {
      var mode = sel ? sel.value : kind;
      var vals = Object.keys(v).map(function (k) { return v[k][mode]; }).filter(function (x) { return x !== null && x !== undefined; });
      var max = Math.max.apply(null, vals.concat([1e-9]));
      el.innerHTML = '<svg viewBox="' + FM.viewBox + '" role="img" aria-label="Map of Finland by region">' +
        Object.keys(FM.regions).map(function (k) {
          var x = v[k] ? v[k][mode] : null, f;
          if (x === null || x === undefined) f = "var(--none)";
          else if (mode === "ratio") { var u = Math.max(-1, Math.min(1, Math.log(x) / Math.log(2))); f = mix(lo, u >= 0 ? up : down, 0.15 + 0.85 * Math.abs(u)); }
          else f = mix(lo, hi, 0.12 + 0.88 * Math.sqrt(x / max));
          var t = (x === null || x === undefined) ? "no service provider with this region" :
            (mode === "cov" ? x.toFixed(1) + "% of the population have results here" :
             mode === "ratio" ? (x >= 1 ? "×" + x.toFixed(2) : "/" + (1 / x).toFixed(2)) + (el.closest("#panel-sp section") ? " its share of all results" : " its share of the population, in tests") : x.toFixed(1) + "% of the tests");
          var href = (link && v[k]) ? ' data-href="' + link + slug(k) + '.html"' : '';
          return '<path d="' + FM.regions[k] + '"' + href + ' style="fill:' + f + '"><title>' + k + ': ' + t + (href ? ' (click for its service providers)' : '') + '</title></path>';
        }).join("") + '</svg>';
      var lg = card ? card.querySelector(".choro-lg") : null;
      if (lg && mode === "ratio") lg.innerHTML = '<span><i style="background:' + mix(lo, down, 1) + ';height:10px;width:16px"></i>/2 or less</span>' +
        '<span><i style="background:' + mix(lo, up, 1) + ';height:10px;width:16px"></i>×2 or more</span>' +
        '<span><i style="background:var(--none);height:10px;width:16px"></i>no service provider</span>';
      else if (lg) lg.innerHTML = '<span><i style="background:' + mix(lo, hi, 0.12) + ';height:10px;width:16px"></i>lower</span>' +
        '<span><i style="background:' + mix(lo, hi, 1) + ';height:10px;width:16px"></i>higher</span>' +
        '<span><i style="background:var(--none);height:10px;width:16px"></i>no service provider</span>';
    }
    if (sel) sel.addEventListener("change", draw);
    draw();
  });
  // "show more": reveal the hidden cards of a grid
  document.querySelectorAll("[data-more]").forEach(function (b) {
    b.addEventListener("click", function () {
      document.querySelectorAll("#" + b.getAttribute("data-more") + " .more").forEach(function (e) { e.hidden = false; });
      b.hidden = true;
    });
  });
  // overlay pickers: show the chosen lab's dots in every chart of that overlay
  document.querySelectorAll("select[data-overlay]").forEach(function (sel) {
    var key = sel.getAttribute("data-overlay");
    sel.addEventListener("change", function () {
      document.querySelectorAll('[data-ov="' + key + '"]').forEach(function (g) {
        g.hidden = g.getAttribute("data-key") !== sel.value;
      });
    });
  });
  // tabs: one panel at a time; the first by default, #<name> opens another (e.g. #sp)
  var tabs = Array.prototype.slice.call(document.querySelectorAll("[data-tab]"));
  var names = tabs.map(function (b) { return b.getAttribute("data-tab"); });
  function show(name) {
    tabs.forEach(function (b) {
      var on = b.getAttribute("data-tab") === name;
      b.setAttribute("aria-selected", on ? "true" : "false");
      document.getElementById("panel-" + b.getAttribute("data-tab")).hidden = !on;
    });
    var cols = document.getElementById("concept-columns");
    if (cols) cols.hidden = name !== names[0];
  }
  if (tabs.length) {
    tabs.forEach(function (b) { b.addEventListener("click", function () { location.hash = b.getAttribute("data-tab"); }); });
    var fromHash = function () { var h = location.hash.slice(1); show(names.indexOf(h) >= 0 ? h : names[0]); };
    window.addEventListener("hashchange", fromHash);
    fromHash();
  }
})();
"""


def page(title, body, depth, release, built):
    up = "../" * depth
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{E(title)}</title>
<link rel="stylesheet" href="{up}assets/portal.css">
</head>
<body>
<header class="top"><div class="wrap">
  <a class="brand" href="{up}index.html">Kanta lab QC <span>· OMOP concepts</span></a>
  <span class="release">release <b>{E(release)}</b> · built {E(built)} · every number covers ≥ {MIN_N} people</span>
</div></header>
<main class="wrap">
{body}
</main>
<script src="{up}assets/finland.js"></script>
<script src="{up}assets/portal.js"></script>
</body>
</html>
"""


def landing(sums, labs=None, lab_conc=None, paged=frozenset(), regions=None):
    cols = [("OMOP ID", ""), ("Concept", ""), ("Unit", ""), ("People", "num"), ("Rows", "num"),
            ("Records / person", "num"), ("Female", "num"), ("Test names", "num"), ("Units", "num"), ("Service Providers", "num"),
            ("Years", "num"), ("Audit: rows flagged", "num"), ("Service Provider flags", "num"), ("Reports", "")]
    head = "".join(f'<th class="{c}" scope="col">{h}</th>' for h, c in cols)
    body = []
    for s in sums:
        o = s["omop"]
        tests = " ".join(s["_tests"])
        yrs = f'{int(s["year_first"])}–{int(s["year_last"])}' if num(s["year_first"]) else ""
        a = bool(s["audit"]) and str(s["audit"]) != "False"
        sp = bool(s["sp"]) and str(s["sp"]) != "False"
        nf = int(s["sp_flags"]) if sp and num(s["sp_flags"]) else None
        body.append(
            f'<tr data-text="{E((o + " " + s["name"] + " " + s["quantity"] + " " + s["h_unit"] + " " + tests).lower())}"'
            f' data-audit="{1 if a else 0}" data-sp="{1 if sp else 0}">'
            f'<td class="mono" data-sort="{o}"><a href="omop/{o}.html">{o}</a></td>'
            f'<td class="name"><a href="omop/{o}.html">{E(s["name"] or "(name not in LABfi)")}</a>'
            f'<span class="sub">{E(s["quantity"])}</span></td>'
            f'<td class="mono">{E(s["h_unit"])}</td>{count_td(s["people"])}{count_td(s["rows"])}'
            f'<td class="num" data-sort="{s["records_per_person"] if num(s["records_per_person"]) else ""}">'
            f'{int(s["records_per_person"]) if num(s["records_per_person"]) else pct_s(None)}</td>'
            f'<td class="num" data-sort="{s["female_pct"] if num(s["female_pct"]) else ""}">{pct_s(s["female_pct"], 0)}</td>'
            f'<td class="num">{s["test_names"]}</td><td class="num">{s["units"]}</td><td class="num">{s["labs"]}</td>'
            f'<td class="num" data-sort="{s["year_first"] if num(s["year_first"]) else ""}">{yrs}</td>'
            f'<td class="num" data-sort="{s["audit_flagged_pct"] if num(s["audit_flagged_pct"]) else ""}">'
            f'{sev_chip(s["audit_flagged_pct"]) if a else "<span class=muted>—</span>"}</td>'
            f'<td class="num" data-sort="{nf if nf is not None else ""}" title="{E(s["sp_flag_kinds"] or "")}">'
            f'{nf if nf is not None else "<span class=muted>—</span>"}</td>'
            f'<td class="links"><a class="pill{"" if a else " off"}" href="omop/{o}.html#audit">audit</a>'
            f'<a class="pill{"" if sp else " off"}" href="omop/{o}.html#sp">Service Providers</a></td></tr>')
    has_labs = labs is not None and len(labs) > 0
    labs_tab = (f'<button type="button" role="tab" data-tab="sp" aria-selected="false">Service Providers ({len(labs)})</button>'
                if has_labs else "")
    labs_html = labs_panel(labs, lab_conc or {}, paged, regions) if has_labs else ""
    na = sum(1 for s in sums if str(s["audit"]) == "True")
    ns = sum(1 for s in sums if str(s["sp"]) == "True")
    return f"""
<section>
  <h1>OMOP lab concepts</h1>
  <p class="lead">One row per concept that was run through the unit and reference-range audit or the Service Provider report. Search by
  name, OMOP ID or test name; click a column to sort, a concept to see its test names and reports. Every number covers at
  least {MIN_N} people; smaller groups are pooled, and statistics that would cover fewer people read
  <span class="below">below threshold</span>.</p>
</section>
<div>
<div class="tabs" role="tablist"><button type="button" role="tab" data-tab="concepts" aria-selected="true">Concepts ({len(sums)})</button>{labs_tab}</div>
<section id="panel-concepts">
  <div class="controls">
    <input class="search" id="q" type="search" placeholder="Search concepts, OMOP IDs, test names…" aria-label="Search concepts" data-search="omops">
    <label><input type="checkbox" id="onlyAudit" data-require="audit" data-table="omops"> audited</label>
    <label><input type="checkbox" id="onlySp" data-require="sp" data-table="omops"> with Service Provider report</label>
    <span class="count" data-count="omops"></span>
  </div>
  <div class="tablebox"><table class="sortable" id="omops"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>
  <p class="muted" style="font-size:16px;margin:8px 2px 0">{len(sums)} concepts · {na} audited · {ns} with a Service Provider report · the table as <a href="omops.tsv">omops.tsv</a></p>
</section>
{labs_html}
</div>
<footer id="concept-columns">
  <h2>Columns</h2>
  <dl class="defs">
    <dt>People, rows</dt><dd>distinct people and results with a value for the concept in this release</dd>
    <dt>Records / person</dt><dd>median number of results per person</dd>
    <dt>Test names, units, Service Providers</dt><dd>how many test names, units and service providers each cover at least {MIN_N} people</dd>
    <dt>Years</dt><dd>first and last year with results</dd>
    <dt>Audit: rows flagged</dt><dd>share of rows whose unit, value or reference range disagree (omop_ref_audit); green under 1%, amber 1–5%, red 5% and more</dd>
    <dt>Service Provider flags</dt><dd>service-provider- and time-specific flags from sp_qc (steps, shifts, outliers, range changes, …), all runs; hover for the kinds</dd>
  </dl>
</footer>"""


HOUR_CATS = ["none", "00:00"] + [f"{h:02d}" for h in range(24)]
DAY_CATS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", "none"]
DAY_NAME = {d: d for d in DAY_CATS} | {"none": "no date"}


def parse_x(v):
    """sp_qc's factor text (x1.08, /1.10) as a log10 ratio, or None."""
    v = str(v).strip()
    try:
        return np.log10(float(v[1:])) * (1 if v[0] == "x" else -1) if v[:1] in ("x", "/") else None
    except ValueError:
        return None


def timing_svg(tim):
    """When the samples are taken, nationally (all labs), from sp_qc's published _timing.tsv: hours and weekdays side
    by side, one dot per hour / weekday, sized by its share of the results, at the height of its median value vs all
    hours / days (same sex and age; log scale; orange higher, blue lower)."""
    blocks = []  # (kind, categories, x0, step, rows of all labs by category)
    ml, gap = 70, 70
    x = ml
    for kind, cats, step in (("hour", HOUR_CATS, 50), ("weekday", DAY_CATS, 56)):
        t = tim[(tim["kind"] == kind) & (tim["sp"] == "all labs")].set_index("cat")
        cats = [c for c in cats if c in t.index]
        if cats:
            blocks.append((kind, cats, x, step, t))
            x += step * len(cats) + gap
    if not blocks:
        return ""
    W = x - gap + 16
    top, mh = 40, 260
    y0 = top + mh / 2
    H = top + mh + 30
    name = lambda kind, c: ({"none": "no time"} if kind == "hour" else {"none": "no date"}).get(c, c)  # noqa: E731
    shares = [x for *_, t in blocks for x in pd.to_numeric(t["pct_of_rows"], errors="coerce") if pd.notna(x) and x > 0]
    rmax = 0.48 * min(step for _, _, _, step, _ in blocks)  # the largest dot nearly fills its column
    k = rmax / np.sqrt(max(shares + [1.0]))
    R = lambda pct: k * np.sqrt(max(pct, 0))  # noqa: E731  (dot area proportional to the share)
    devs = [parse_x(v) for *_, t in blocks for v in t["x_vs_all"]]
    dmax = max([abs(d) for d in devs if d is not None] + [np.log10(1.05)])
    Y = lambda d: y0 - d / dmax * (mh / 2 - rmax)  # noqa: E731
    g = []
    for d in (dmax, 0, -dmax):
        lab = "×1" if d == 0 else (f"×{10 ** d:.2f}" if d > 0 else f"/{10 ** -d:.2f}")
        g.append(f'<text x="{ml - 10}" y="{Y(d) + 4:.1f}" text-anchor="end">{lab}</text>')
    for kind, cats, x0, step, t in blocks:
        X = lambda i: x0 + step * i + step / 2  # noqa: E731
        x1 = x0 + step * len(cats)
        g.append(f'<text x="{x0}" y="14" font-weight="600">{"Hour of the day" if kind == "hour" else "Weekday"}</text>'
                 f'<line x1="{x0}" x2="{x1}" y1="{y0:.1f}" y2="{y0:.1f}" stroke="var(--muted)"/>')
        for d in (dmax, -dmax):
            g.append(f'<line x1="{x0}" x2="{x1}" y1="{Y(d):.1f}" y2="{Y(d):.1f}" stroke="var(--line)" '
                     f'stroke-dasharray="3 3"/>')
        for j, c in enumerate(cats):
            if kind != "hour" or c in ("none", "00:00") or int(c) % 3 == 0:
                fs = ' style="font-size:15px"' if c in ("none", "00:00") else ""
                g.append(f'<text x="{X(j):.1f}" y="{H - 8}" text-anchor="middle"{fs}>{E(name(kind, c))}</text>')
            v, dev = pd.to_numeric(t.loc[c, "pct_of_rows"], errors="coerce"), parse_x(t.loc[c, "x_vs_all"])
            if dev is None or pd.isna(v):
                continue
            txt = f"×{10 ** dev:.2f}" if dev >= 0 else f"/{10 ** -dev:.2f}"
            g.append(f'<circle cx="{X(j):.1f}" cy="{Y(dev):.1f}" r="{R(v):.1f}" fill="var(--{"s2" if dev >= 0 else "s1"})" '
                     f'fill-opacity="0.85" stroke="var(--surface)" stroke-width="1.5"><title>{E(name(kind, c))}: median '
                     f'{txt} vs all, {v:.1f}% of the results</title></circle>')
    key = "".join(f'<span><i class="dotkey" style="width:{2 * R(p):.0f}px;height:{2 * R(p):.0f}px"></i>{p}% of the '
                  f'results</span>' for p in (2, 5, 10))
    return (f'<div class="chart"><h3>When the samples are taken</h3><p class="q">All service providers: per hour of the day and per '
            f'weekday, the median value vs all hours / days (height; same sex and age; orange higher, blue lower) and '
            f'the share of the results (dot size). From the Service Provider report.</p>'
            f'<svg class="wide-text" viewBox="0 0 {W:.0f} {H:.0f}" role="img" aria-label="When the samples are taken">'
            f'{"".join(g)}</svg>'
            f'<div class="lg">{key}</div></div>')


def timing_html(tim):
    if tim is None or tim.empty:
        return ""
    return f'<div class="charts wide" style="margin-top:14px">{timing_svg(tim)}</div>'


def concept_region_map(clabs, lab_region):
    """The concept's regions: per maakunta its share of this concept's results (summed over the service providers whose
    region it is, from the concept's Service Providers table) against its share of all results (the same service
    providers' shares of the whole data), as a ratio on the map: green where the region does relatively more of this
    test, red where less. Service providers without a region are left out of both."""
    if clabs is None or clabs.empty or not lab_region:
        return ""
    d = clabs.assign(region=clabs["lab_id"].astype(str).map(lab_region))
    d = d[d["region"].notna()]
    if d.empty:
        return ""
    g = d.groupby("region")[["pct_concept", "pct_dataset"]].sum()
    g["sc"], g["sa"] = 100 * g["pct_concept"] / g["pct_concept"].sum(), 100 * g["pct_dataset"] / g["pct_dataset"].sum()
    data = {k: {"ratio": float(r.sc / r.sa)} for k, r in g.iterrows() if r.sa > 0 and r.sc > 0}
    if not data:
        return ""
    g = g[(g["sc"] > 0) & (g["sa"] > 0)].assign(ratio=lambda x: x["sc"] / x["sa"]).sort_values("sc", ascending=False)
    xmax = max(g["sc"].max(), g["sa"].max()) * 1.05
    trs = "".join(
        f'<tr><td>{E(k)}</td>{share_cell(r.sc, r.sa, xmax)}<td class="num" data-sort="{r.sc}">{r.sc:.1f}%</td>'
        f'<td class="num" data-sort="{r.sa}">{r.sa:.1f}%</td><td class="num" data-sort="{r.ratio}">{ratio_s(r.ratio)}</td></tr>'
        for k, r in g.iterrows())
    return (f'<section><h2>Regions <small>this concept against the whole data</small></h2>'
            f'<div style="display:flex; flex-wrap:wrap; gap:14px; align-items:flex-start">'
            f'<div class="chart mapcard" style="flex:0 1 400px; min-width:0">'
            f'<h3>Regions doing more or less of this test</h3><p class="q">Each region&#39;s share of this concept&#39;s '
            f'results against its share of all results: green more, red less. Hover a region for the ratio.</p>'
            f'<div class="fimap choro" data-choro-kind="ratio" data-choro="{E(json.dumps(data, ensure_ascii=False))}"></div>'
            f'<div class="lg choro-lg"></div><p class="q" style="font-size:12px">Map: Statistics Finland, CC BY 4.0</p></div>'
            f'<div class="chart" style="flex:1 1 560px; min-width:0"><h3>Regions: this concept against all results</h3>'
            + how("Per region, summed over the service providers whose region it is (as in the Service Providers table): "
                  "the bar is the region&#39;s share of this concept&#39;s results, the dot its share of all results, "
                  "both over the regions listed; green when the region does more of this test than of all tests, red "
                  "when less. Ratio: the first over the second. Service providers without a region are left out.")
            + f'<div class="tablebox"><table class="sortable" id="cregions"><thead><tr><th>Region</th><th class="num">Service Providers</th><th>Share</th>'
            f'<th class="num">% of this concept</th><th class="num">% of all results</th><th class="num">Ratio</th>'
            f'</tr></thead><tbody>{trs}</tbody></table></div></div></div></section>')


def concept_page(s, comp, years, files, verdicts, clabs=None, paged=frozenset(), tim=None, d=None, ids_by_label=None,
                 lab_region=None):
    o = s["omop"]
    tile = lambda k, v, d="": (f'<div class="tile"><div class="k">{k}</div><div class="v">{v}</div>'  # noqa: E731
                               + (f'<div class="d">{d}</div>' if d else "") + "</div>")
    yrs = f'{int(s["year_first"])}–{int(s["year_last"])}' if num(s["year_first"]) else "—"
    tiles = "".join([
        tile("People", f'<span data-n="{int(s["people"])}">{int(s["people"]):,}</span>'),
        tile("Results", f'<span data-n="{int(s["rows"])}">{int(s["rows"]):,}</span>', "with a value"),
        tile("Female", pct_s(s["female_pct"], 0)), tile("Years", yrs),
        tile("Test names", s["test_names"], f'{s["units"]} units'), tile("Service Providers", s["labs"], "with at least 5 people"),
        tile("Records per person", int(s["records_per_person"]) if num(s["records_per_person"]) else pct_s(None), "median"),
        tile("People with ≥ 2 records", pct_s(s["ge2_records_pct"], 0)),
        tile("Age at test", int(s["age_median"]) if num(s["age_median"]) else pct_s(None), "median, years")])
    link = lambda p: f'<a href="{o}/{E(p.name)}">{E(p.name)}</a>'  # noqa: E731
    au = [p for k, p in files if k == "audit"]
    sp = [p for k, p in files if k == "sp"]
    if au:
        bar = ""
        if verdicts is not None and len(verdicts):
            v = verdicts.assign(pct=pd.to_numeric(verdicts["rows"]) / pd.to_numeric(verdicts["rows"]).sum() * 100)
            bar = ('<div class="stack" role="img" aria-label="Verdict shares">' + "".join(
                f'<span style="width:{r.pct:.2f}%;background:{A.VCOLOR.get(r.verdict, "#a8a69c")}" '
                f'title="{E(r.verdict)}: {r.pct:.1f}%"></span>' for r in v.itertuples() if r.pct > 0.05) + "</div>"
                '<div class="keyrow">' + "".join(
                f'<span><i style="background:{A.VCOLOR.get(r.verdict, "#a8a69c")}"></i>{E(r.verdict)} '
                f'{r.pct:.{2 if r.pct < 1 else 1}f}%</span>' for r in v.itertuples() if r.pct > 0.05) + "</div>")
        audit = (f'<div class="card" id="audit"><h3>Units and reference ranges {sev_chip(s["audit_flagged_pct"])}</h3>'
                 f'<p>Does each row\'s value agree with its unit and its own reference range? Rows flagged by the audit, '
                 f'and the unit they probably should have had (the charts are under Units and reference ranges).</p>{bar}'
                 + f'<div class="files">{"".join(link(p) for p in au)}</div></div>')
    else:
        audit = (f'<div class="card" id="audit"><h3>Units and reference ranges</h3><p>The audit has not been run for this '
                 f'concept. Run <code>omop_ref_audit.py --omop {o}</code> and rebuild the portal.</p></div>')
    if sp:
        kinds = [x.rsplit(" ", 1) for x in str(s["sp_flag_kinds"] or "").split(", ") if x]
        nf = int(s["sp_flags"]) if num(s["sp_flags"]) else 0
        labs = (f'<div class="card" id="labs"><h3>Service Providers over time <span class="chip {"warn" if nf else "ok"}">{nf} flags'
                f'</span></h3><p>Which service providers, and when, deviate from the national picture: level shifts, method changes, '
                f'outliers, reference-range changes, sampling times (the PDF; the flags in <code>_flags.tsv</code>).</p>'
                f'<div class="flaglist">' + ("".join(f'<span class="chip">{E(k.replace("_", " "))} {E(v)}</span>'
                                                   for k, v in kinds) or '<span class="muted">no flags</span>')
                + f'</div><div class="files">{"".join(link(p) for p in sp)}</div></div>')
    else:
        labs = (f'<div class="card" id="labs"><h3>Service Providers over time</h3><p>The Service Provider report has not been run for this concept. '
                f'Run <code>sp_qc.py --omop {o}</code> and rebuild the portal.</p></div>')
    hu = E(s["h_unit"])
    cols = [("Test name", ""), ("Unit", ""), ("Harmonized unit", ""), ("People", "num"), ("Rows", "num"),
            ("% of rows", "num"), (f"Median ({hu})", "num"), ("With reference range", "num"), ("Typical range", "num"),
            ("Outcome H/L", "num"), ("Audit: flagged", "num"), ("Differs (KS D)", "num")]
    head = "".join(f'<th class="{c}" scope="col">{h}</th>' for h, c in cols)
    trs = []
    for r in comp.itertuples():
        if not num(r.people):  # the pooled row
            trs.append(f'<tr class="pinned"><td colspan="3">{E(r.test)}: each covers too few people to show alone</td>'
                       f'<td class="num"></td>{count_td(r.rows)}<td class="num">{r.pct_rows:.1f}%</td>'
                       f'<td colspan="6"></td></tr>')
            continue
        ks = "" if not num(r.ks_d) else (f'<span class="chip crit">yes · {float(r.ks_d):.2f}</span>'
                                          if r.differs == "yes" else f"{float(r.ks_d):.2f}")
        trs.append(
            f'<tr data-text="{E((r.test + " " + r.unit).lower())}"><td class="mono">{E(r.test)}</td>'
            f'<td class="mono">{E(r.unit)}</td><td class="mono">{E(r.h_unit)}</td>{count_td(r.people)}{count_td(r.rows)}'
            f'<td class="num" data-sort="{r.pct_rows}">{r.pct_rows:.1f}%</td>'
            f'<td class="num" data-sort="{r.median if num(r.median) else ""}">{fnum(float(r.median)) if num(r.median) else pct_s(None)}</td>'
            f'<td class="num" data-sort="{r.with_ref_pct if num(r.with_ref_pct) else ""}">{pct_s(r.with_ref_pct, 0)}</td>'
            f'<td class="num">{E(r.typical_range) if isinstance(r.typical_range, str) else ""}</td>'
            f'<td class="num" data-sort="{r.outcome_hl_pct if num(r.outcome_hl_pct) else ""}">'
            f'{pct_s(r.outcome_hl_pct, 0) if num(r.outcome_hl_pct) else "<span class=muted>—</span>"}</td>'
            f'<td class="num" data-sort="{r.audit_flagged_pct if num(r.audit_flagged_pct) else ""}">{sev_chip(r.audit_flagged_pct)}</td>'
            f'<td class="num" data-sort="{r.ks_d if num(r.ks_d) else ""}">{ks or "<span class=muted>—</span>"}</td></tr>')
    athena = f'<a href="https://athena.ohdsi.org/search-terms/terms/{o}">Athena ↗</a>'
    labs_html = concept_labs_html(clabs, o, paged)
    # the audit and the Service Provider report, drawn in the page from the gated tables (no images)
    audit_html = audit_section(o, d, files)
    sp_html = lab_report_section(o, d, files, paged, ids_by_label or {}, s["h_unit"])
    return f"""
<div class="crumb"><a href="../index.html">← All concepts</a></div>
<section>
  <h1>{E(s["name"] or "OMOP " + o)}</h1>
  <div class="idline"><span>OMOP <span class="mono">{o}</span></span><span>{E(s["quantity"])}</span>
  <span>harmonized unit <span class="mono">{hu}</span></span>{athena}</div>
</section>
<section class="tiles">{tiles}</section>
<div><div class="tabs" role="tablist"><button type="button" role="tab" data-tab="omop" aria-selected="true">OMOP concept</button><button type="button" role="tab" data-tab="sp" aria-selected="false">Service Providers</button></div>
<div id="panel-omop" class="panel">
<section><h2>Report</h2><div class="cards">{audit}</div></section>
<section><h2>Composition <small>the test names and units merged into this concept</small></h2>
  <div class="charts wide">{year_svg(years)}</div>
  {timing_html(tim)}</section>
<section>
  <div class="controls"><input class="search" id="q" type="search" placeholder="Filter test names, units…" aria-label="Filter test names" data-search="comp"><span class="count" data-count="comp"></span></div>
  <div class="tablebox"><table class="sortable" id="comp"><thead><tr>{head}</tr></thead><tbody>{"".join(trs)}</tbody></table></div>
  <p class="muted" style="font-size:16px;margin:8px 2px 0">The tables as <a href="{o}/composition.tsv">composition.tsv</a>,
  <a href="{o}/years.tsv">years.tsv</a>. People don't add up over rows (one person
  can have several test names); rows do, pooled row included. Median, typical range and shares are shown only where they
  cover at least {MIN_N} people. KS D (from the audit) is the largest gap between a test name's value distribution and
  the rest of the concept's (0: the same, 1: no overlap); a high D with few rows flagged points to another analyte or
  specimen mapped here, which the scale checks miss.</p>
</section>
{audit_html}
</div>
<div id="panel-sp" class="panel" hidden>
{concept_region_map(clabs, lab_region)}
{labs_html}
<section><h2>Report</h2><div class="cards">{labs}</div></section>
{sp_html}
</div></div>
"""


def ratio_s(r):
    if not num(r):
        return ""
    r = float(r)
    txt = f"×{r:.1f}" if r >= 1 else f"/{1 / r:.1f}"
    return f'<span class="ratio-hi">{txt}</span>' if r >= 2 or r <= 0.5 else txt


def labs_svg(labs):
    """The top labs of a concept: a bar for their share of this concept's results, a dot for their share of all
    results (both over the same labs), the ratio at the right."""
    d = labs.head(15)
    W, lw, rw, row, top = 760, 230, 60, 22, 8
    H = top + row * len(d) + 30
    xmax = max(d["pct_concept"].max(), d["pct_dataset"].max()) * 1.08
    X = lambda v: lw + v / xmax * (W - lw - rw)  # noqa: E731
    g = []
    step = 10 ** np.floor(np.log10(xmax / 2))
    t = 0.0
    while t <= xmax:
        g.append(f'<line x1="{X(t):.1f}" x2="{X(t):.1f}" y1="{top - 4}" y2="{H - 24}" stroke="var(--line)"/>'
                 f'<text x="{X(t):.1f}" y="{H - 10}" text-anchor="middle">{fnum(t)}%</text>')
        t += step * (2 if xmax / step > 10 else 1)
    for i, r in enumerate(d.itertuples()):
        y = top + i * row + row / 2
        lab = r.label if len(r.label) <= 34 else r.label[:33] + "…"
        g.append(f'<text x="{lw - 8}" y="{y + 4:.1f}" text-anchor="end">{E(lab)}<title>{E(r.label)}</title></text>'
                 f'<rect x="{lw}" y="{y - 6:.1f}" width="{max(X(r.pct_concept) - lw, 1):.1f}" height="12" rx="2" '
                 f'fill="var(--accent)"><title>{E(r.label)}: {r.pct_concept:.1f}% of this concept\'s results</title></rect>'
                 f'<circle cx="{X(r.pct_dataset):.1f}" cy="{y:.1f}" r="4.5" fill="var(--ink)" stroke="var(--surface)" '
                 f'stroke-width="2"><title>{E(r.label)}: {r.pct_dataset:.1f}% of all results</title></circle>'
                 f'<text x="{W - rw + 8}" y="{y + 4:.1f}">{"" if not num(r.ratio) else (f"×{r.ratio:.1f}" if r.ratio >= 1 else f"/{1 / r.ratio:.1f}")}</text>')
    return (f'<div class="chart"><h3>The concept\'s Service Providers vs all results</h3><p class="q">Which service providers report this test more '
            f'or less than their share of all results would suggest? (top {len(d)}; shares over the service providers shown)</p>'
            f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Service Providers: share of this concept vs all results">{"".join(g)}</svg>'
            f'<div class="lg"><span><i style="background:var(--accent);height:9px"></i>share of this concept\'s results</span>'
            f'<span><i style="background:var(--ink);height:9px;width:9px;border-radius:50%"></i>share of all results</span>'
            f'<span>×, /: ratio of the two</span></div></div>')


def lab_link(lab_id, label, paged, up=""):
    """A lab's name, linked when it has a page (the --top-labs largest)."""
    return f'<a href="{up}lab/{E(lab_id)}.html">{E(label)}</a>' if lab_id in paged else E(label)


def share_cell(pc, pd_, xmax):
    """A table cell with the row's two shares drawn in place: bar = share of this concept's results, dot = share of all
    results; one scale (xmax) for every row of the table, so the bars compare down the table."""
    W, H = 260, 20
    X = lambda v: 6 + min(v, xmax) / xmax * (W - 14)  # noqa: E731  (room for the dot at both ends)
    return (f'<td class="sharecell" data-sort="{pc}"><svg viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" '
            f'aria-label="{pc:.1f}% of this concept, {pd_:.1f}% of all results">'
            f'<rect x="6" y="5" width="{max(X(pc) - 6, 1):.1f}" height="10" rx="2" '
            f'fill="var(--{"up" if pc >= pd_ else "down"})"/>'
            f'<circle cx="{X(pd_):.1f}" cy="10" r="4.5" fill="var(--ink)" stroke="var(--surface)" stroke-width="2"/></svg></td>')


def concept_labs_html(labs, o, paged=frozenset()):
    if labs is None or labs.empty:
        return ""
    xmax = max(labs["pct_concept"].max(), labs["pct_dataset"].max()) * 1.05
    head = "".join(f'<th class="{c}" scope="col">{h}</th>' for h, c in (
        ("Service Provider", ""), ("Share", ""), ("% of this concept", "num"), ("% of all results", "num"), ("Ratio", "num"),
        ("Years", "num")))
    trs = "".join(
        f'<tr data-text="{E(r.label.lower())}"><td>{lab_link(r.lab_id, r.label, paged, "../")}</td>'
        f'{share_cell(r.pct_concept, r.pct_dataset, xmax)}'
        f'<td class="num" data-sort="{r.pct_concept}">{r.pct_concept:.1f}%</td>'
        f'<td class="num" data-sort="{r.pct_dataset}">{r.pct_dataset:.1f}%</td>'
        f'<td class="num" data-sort="{r.ratio}">{ratio_s(r.ratio)}</td>'
        f'<td class="num">{int(r.year_first)}–{int(r.year_last)}</td></tr>' for r in labs.itertuples())
    return f"""<section id="labs-of-concept"><h2>Service Providers <small>from the Service Provider report's shown years; shares over these service providers</small></h2>
  {how("Which service providers report this test more or less than their share of all results would suggest? In the Share "
       "column the bar is the service provider&#39;s share of this concept&#39;s results and the dot its share of all results "
       "(one scale for the whole table): green when the bar reaches past its dot (relatively much of this test), red when it "
       "stops short (relatively little). "
       "Ratio: the first over the second, red at ×2 and more or /2 and less. Sort by any column.")}
  <div class="lg" style="margin:0 0 8px"><span><i style="background:var(--up);height:9px;width:14px;display:inline-block;border-radius:2px;margin-right:5px"></i>share of this concept&#39;s results, higher than</span>
  <span style="margin-left:10px"><i style="background:var(--down);height:9px;width:14px;display:inline-block;border-radius:2px;margin-right:5px"></i>lower than</span>
  <span style="margin-left:14px"><i style="background:var(--ink);height:9px;width:9px;display:inline-block;border-radius:50%;margin-right:5px"></i>share of all results</span></div>
  <div class="controls"><input class="search" type="search" placeholder="Filter service providers…" aria-label="Filter service providers" data-search="clabs"><span class="count" data-count="clabs"></span></div>
  <div class="tablebox"><table class="sortable" id="clabs" data-limit="20"><thead><tr>{head}</tr></thead><tbody>{trs}</tbody></table></div>
  <p class="muted" style="font-size:16px;margin:8px 2px 0">As <a href="{o}/service_providers.tsv">service_providers.tsv</a>. Both shares are
  over the service providers listed here.</p>
</section>"""


def lab_colors(ly):
    """The organisations of a service provider's over-time chart and their colours: its top 4 (by results over the
    years) in the series colours, then 'other organisations' and 'no organisation'. Shared by the chart and the
    organisation table (whose names wear these colours: the chart has no legend)."""
    weight = {}
    for org, sh, pl in zip(ly["org"], ly["share"], ly["pct_of_lab"]):
        weight[org] = weight.get(org, 0.0) + float(sh) * float(pl)
    named = [o for o in sorted(weight, key=lambda o: -weight[o]) if o not in (OTHER_ORG, NONE_ORG)][:4]
    order = named + [o for o in (OTHER_ORG, NONE_ORG) if o in weight]
    return order, {o: f"var(--s{i + 1})" for i, o in enumerate(named)} | {OTHER_ORG: "var(--other)", NONE_ORG: "var(--none)"}


def lab_years_svg(ly):
    """A lab's organisations over time: one stacked bar per period (years pooled to >= MIN_N people), as shares.
    Plain Python over the lab's rows (thousands of lab pages: no per-page pandas work)."""
    rows = list(zip(ly["year_from"], ly["year_to"], ly["org"], ly["share"], ly["pct_of_lab"]))
    if not rows:
        return ""
    segs, pct = {}, {}
    for y0, y1, org, sh, pl in rows:
        k = (int(y0), int(y1))
        segs.setdefault(k, {})[org] = float(sh)
        pct[k] = float(pl)
    order, col = lab_colors(ly)
    W, H, ml, mr, mt, mb = 1300, 300, 60, 10, 12, 30
    x0, x1 = min(k[0] for k in segs), max(k[1] for k in segs) + 1
    X = lambda y: ml + (y - x0) / (x1 - x0) * (W - ml - mr)  # noqa: E731
    Y = lambda v: H - mb - v * (H - mt - mb)  # noqa: E731
    g = [f'<line x1="{ml}" x2="{W - mr}" y1="{Y(v):.1f}" y2="{Y(v):.1f}" stroke="var(--line)"/>'
         f'<text x="{ml - 6}" y="{Y(v) + 4:.1f}" text-anchor="end">{int(v * 100)}%</text>' for v in (0.5, 1.0)]
    for (y0, y1), seg in sorted(segs.items()):
        xa, xb, acc = X(y0) + 1, X(y1 + 1) - 1, 0.0
        span = f"{y0}" + (f"–{y1} (pooled)" if y1 > y0 else "")
        for o in order:
            v = seg.get(o, 0.0)
            if v <= 0:
                continue
            g.append(f'<rect x="{xa:.1f}" y="{Y(acc + v):.1f}" width="{xb - xa:.1f}" height="{Y(acc) - Y(acc + v):.1f}" '
                     f'fill="{col[o]}" stroke="var(--surface)" stroke-width="1"><title>{E(span)}: {E(o)} {100 * v:.1f}% '
                     f'({pct[(y0, y1)]:.1f}% of the service provider\'s results)</title></rect>')
            acc += v
    every = 4 if x1 - x0 > 12 else 2 if x1 - x0 > 6 else 1
    for y in range(int(np.ceil(x0 / every) * every), x1, every):
        g.append(f'<text x="{(X(y) + X(y + 1)) / 2:.1f}" y="{H - 8}" text-anchor="middle">{y}</text>')
    return (f'<div class="chart"><h3>Coding-system organisations over time</h3><p class="q">What is this service provider made of, '
            f'year by year? (share of its results; years pooled to cover at least {MIN_N} people; the colours are those '
            f'of the organisation names above)</p>'
            f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Organisations over time">{"".join(g)}</svg></div>')


def named_chip(v):
    if not num(v):
        return pct_s(None)
    v = float(v)
    return f'<span class="chip {"ok" if v >= 90 else "warn" if v >= 60 else "crit"}">{v:.0f}%</span>'


def region_s(r):
    reg = getattr(r, "region", "")
    if not isinstance(reg, str) or not reg:
        return ""
    pct = getattr(r, "region_pct", None)
    return f"{reg} ({float(pct):.0f}%)" if num(pct) and float(pct) < 99.5 else reg



def region_choropleth(labs, regions=None, tid="regions"):
    """The landing page's regions, in percentages only: each region's share of the tests (results; summed over the
    service providers whose region it is) and its share of the population (Statistics Finland), both over the regions
    listed. Two maps side by side, the share of the tests and its ratio to the share of the population, and a table
    with both shares (bar and dot, as the Share column of the concept pages) and the ratio. No counts of people."""
    if regions is None or regions.empty:
        return ""
    r = regions.copy()
    r["results_pct"], r["population"] = pd.to_numeric(r["results_pct"], errors="coerce"), pd.to_numeric(r["population"], errors="coerce")
    r = r[(r["results_pct"] > 0) & (r["population"] > 0)]
    if r.empty:
        return ""
    r["sh_tests"] = 100 * r["results_pct"] / r["results_pct"].sum()
    r["sh_pop"] = 100 * r["population"] / r["population"].sum()
    r["ratio"] = r["sh_tests"] / r["sh_pop"]
    r = r.sort_values("sh_pop", ascending=False)
    # service providers per region: each one is listed by name in the Service Providers table (and on the region's page)
    nsp = (labs[labs["region"].astype(str).str.len() > 0].groupby("region")["lab_id"].nunique()
           if labs is not None and len(labs) and "region" in labs.columns else pd.Series(dtype=int))
    year = int(r["pop_year"].dropna().iloc[0]) if r["pop_year"].notna().any() else ""
    rest = max(0.0, 100.0 - float(r["results_pct"].sum()))
    xmax = max(r["sh_tests"].max(), r["sh_pop"].max()) * 1.05
    trs = "".join(
        f'<tr><td>{region_link(x.region)}</td><td class="num" data-sort="{int(nsp.get(x.region, 0))}">{int(nsp.get(x.region, 0))}</td>'
        f'{share_cell(x.sh_tests, x.sh_pop, xmax)}'
        f'<td class="num" data-sort="{x.sh_tests}">{x.sh_tests:.1f}%</td><td class="num" data-sort="{x.sh_pop}">{x.sh_pop:.1f}%</td>'
        f'<td class="num" data-sort="{x.ratio}">{ratio_s(x.ratio)}</td></tr>' for x in r.itertuples())

    def mapcard(t, q, kind, d):
        return (f'<div class="chart mapcard"><h3>{E(t)}</h3><p class="q">{E(q)}</p>'
                f'<div class="fimap choro" data-choro-kind="{kind}" data-choro-link="region/" data-choro="{E(json.dumps(d, ensure_ascii=False))}"></div>'
                f'<div class="lg choro-lg"></div></div>')
    maps = (mapcard("Tests by region", "Each region's share of the tests, summed over its service providers", "share",
                    {x.region: {"share": float(x.sh_tests)} for x in r.itertuples()})
            + mapcard("Tests against the population", "Each region's share of the tests against its share of the "
                      "population: green more, red less", "ratio",
                      {x.region: {"ratio": float(x.ratio)} for x in r.itertuples()}))
    note = (f"Share of the tests: the results of the service providers whose region it is; share of the population: "
            f"Statistics Finland, 31 December {year}; both over the regions listed. In the Share column the bar is the "
            f"region&#39;s share of the tests and the dot its share of the population: green when the region has more of "
            f"the tests than of the population, red when less. Ratio: the first over the second. {rest:.1f}% of all "
            f"results are from service providers without a region (private chains, labs serving several regions) and "
            f"are on no region. Service Providers: how many have this region (click a region for them).")
    legend = ('<div class="lg" style="margin:0 0 8px"><span><i style="background:var(--up);height:9px;width:14px;display:inline-block;border-radius:2px;margin-right:5px"></i>share of the tests, higher than</span>'
              '<span style="margin-left:10px"><i style="background:var(--down);height:9px;width:14px;display:inline-block;border-radius:2px;margin-right:5px"></i>lower than</span>'
              '<span style="margin-left:14px"><i style="background:var(--ink);height:9px;width:9px;display:inline-block;border-radius:50%;margin-right:5px"></i>share of the population</span></div>')
    table = (f'<div class="chart"><h3>Regions: tests against the population</h3>{how(note)}{legend}'
             f'<div class="tablebox"><table class="sortable" id="{tid}"><thead><tr><th>Region</th><th class="num">Service Providers</th><th>Share</th>'
             f'<th class="num">% of the tests</th><th class="num">% of the population</th><th class="num">Ratio</th>'
             f'</tr></thead><tbody>{trs}</tbody></table></div></div>')
    return (f'<div style="display:grid; gap:14px; margin: 10px 0 16px">'
            f'<div class="charts" style="grid-template-columns: repeat(auto-fit, minmax(min(380px, 100%), 1fr))">{maps}</div>'
            f'{table}<p class="q" style="font-size:12px;margin:0">Maps and population: Statistics Finland, CC BY 4.0</p></div>')

def region_slug(name):
    """A region's page name: region/<slug>.html (also built by the page script, slug())."""
    t = str(name).lower().replace("ä", "a").replace("å", "a").replace("ö", "o")
    return re.sub(r"[^a-z0-9]+", "-", t).strip("-")


def region_link(name, up=""):
    return f'<a href="{up}region/{region_slug(name)}.html">{E(name)}</a>'


PIE_SLOTS = 7  # the largest 7 service providers in the categorical colours; the rest share the grey "other" slice


def pie_svg(parts, size=240):
    """A pie of (label, share, colour, href or None) parts, largest first, clockwise from 12 o'clock; a slice links to
    its page when it has one, with a tooltip. 2px surface gaps between the slices."""
    import math
    tot = sum(p[1] for p in parts) or 1.0
    c, r = size / 2, size / 2 - 4
    out, a0 = [], -math.pi / 2
    for label, sh, col, href in parts:
        frac = sh / tot
        tip = f"<title>{E(label)}: {100 * frac:.1f}% of the region&#39;s results</title>"
        if frac >= 0.9999:
            shape = f'<circle cx="{c}" cy="{c}" r="{r}" style="fill:{col}">{tip}</circle>'
        else:
            a1 = a0 + 2 * math.pi * frac
            x0, y0, x1, y1 = c + r * math.cos(a0), c + r * math.sin(a0), c + r * math.cos(a1), c + r * math.sin(a1)
            shape = (f'<path d="M{c:.1f},{c:.1f} L{x0:.2f},{y0:.2f} A{r:.1f},{r:.1f} 0 {1 if frac > 0.5 else 0} 1 '
                     f'{x1:.2f},{y1:.2f} Z" style="fill:{col}">{tip}</path>')
            a0 = a1
        out.append(f'<a href="{href}">{shape}</a>' if href else shape)
    return (f'<svg class="pie" viewBox="0 0 {size} {size}" role="img" aria-label="Pie of the region&#39;s service '
            f'providers by share of its results">{"".join(out)}</svg>')


def region_page(name, rlabs, reg, paged):
    """A region's page: its service providers (those whose region it is), largest first, as a pie and a table in the
    same colours; shares of the region's results only (from the published % of all results)."""
    tile = lambda k, v, d="": (f'<div class="tile"><div class="k">{k}</div><div class="v">{v}</div>'  # noqa: E731
                               + (f'<div class="d">{d}</div>' if d else "") + "</div>")
    rl = rlabs.sort_values("pct_results", ascending=False).reset_index(drop=True)
    tot = float(rl["pct_results"].sum()) or 1.0
    rl["sh"] = 100 * rl["pct_results"] / tot
    col = {lid: f"var(--s{i + 1})" for i, lid in enumerate(rl["lab_id"].head(PIE_SLOTS))}
    href = lambda lid: f"../lab/{E(lid)}.html" if lid in paged else None  # noqa: E731
    parts = [(x.label, x.sh, col[x.lab_id], href(x.lab_id)) for x in rl.itertuples() if x.lab_id in col]
    rest = rl[~rl["lab_id"].isin(col)]
    if len(rest):
        parts.append((f"{len(rest)} other service providers", float(rest["sh"].sum()), "var(--other)", None))

    def sp_cell(x):
        c = col.get(x.lab_id, "var(--other)")
        sw = f'<span class="swatch" style="background:{c}"></span>'
        if x.lab_id in paged:
            style = f' style="color:{c}"' if x.lab_id in col else ""
            return f'{sw}<a class="spc" href="../lab/{E(x.lab_id)}.html"{style}>{E(x.label)}</a>'
        return f'{sw}<b style="color:{c}">{E(x.label)}</b>' if x.lab_id in col else f"{sw}{E(x.label)}"
    trs = "".join(
        f'<tr data-text="{E((x.label + " " + x.named_after).lower())}"><td style="white-space:nowrap">{sp_cell(x)}</td>'
        f'<td class="num" data-sort="{x.sh}">{x.sh:.1f}%</td>'
        f'<td class="num" data-sort="{x.pct_results}">{x.pct_results:.2f}%</td>{count_td(x.people)}'
        f'<td>{E(x.named_after) or "<span class=muted>—</span>"}</td>'
        f'<td class="num" data-sort="{x.regional_pct if num(x.regional_pct) else ""}">{pct_s(x.regional_pct, 0)}</td></tr>'
        for x in rl.itertuples())
    tiles = [tile("Service Providers", f"{len(rl)}", "whose region it is")]
    if reg is not None:
        tiles += [tile("Share of the tests", f"{reg['sh_tests']:.1f}%", "over the regions listed"),
                  tile("Share of the population", f"{reg['sh_pop']:.1f}%" if num(reg.get("sh_pop")) else "—",
                       "Statistics Finland"),
                  tile("Tests against the population", ratio_s(reg["ratio"]) if num(reg.get("ratio")) else "—")]
    return f"""
<div class="crumb"><a href="../index.html#sp">← All Service Providers</a></div>
<section><h1>{E(name)}</h1>
  <div class="idline"><span>region (maakunta): the service providers whose region it is</span></div></section>
<section class="tiles">{"".join(tiles)}</section>
<section><h2>Service Providers <small>largest first, by share of the region&#39;s results</small></h2>
  <div class="charts" style="grid-template-columns: repeat(auto-fit, minmax(min(340px, 100%), 1fr))">
  <div class="chart" style="display:flex; flex-direction:column"><h3>Share of the region&#39;s results</h3>
    <p class="q">The largest {PIE_SLOTS} in colour, the same as their names in the table below; the rest together in
    grey. Click a slice to open its page.</p>{pie_svg(parts)}</div>
  <div class="chart mapcard"><h3>Where</h3><p class="q">{E(name)} on the map of Finland</p>
    <div class="fimap" data-map-region="{E(name)}" style="max-width:340px"></div><p class="q" style="font-size:12px">Map: Statistics Finland, CC BY 4.0</p></div></div>
  <div class="chart" style="margin-top:14px"><h3>The service providers</h3>
    {how("A service provider belongs to a region when the maakunta of its coding-system organisations that have one is "
         "this region, and those rows are at least 5% of its results with an organisation (Regional rows). Share of the "
         "region: its share of the results of the service providers listed here; % of all results: of the whole data.")}
    <div class="controls"><input class="search" type="search" placeholder="Filter service providers…" aria-label="Filter service providers" data-search="rlabs"><span class="count" data-count="rlabs"></span></div>
    <div class="tablebox"><table class="sortable" id="rlabs" data-limit="20"><thead><tr><th>Service Provider</th>
    <th class="num">Share of the region</th><th class="num">% of all results</th><th class="num">People</th>
    <th>Top organisation</th><th class="num">Regional rows</th></tr></thead>
    <tbody>{trs}</tbody></table></div></div></section>"""


def naming_case(title, cs, orgs, sp, name, region, note, colour):
    """One example of the naming rule as a small vertical figure in its own box: coding systems -> their organisations
    (line width = share of the service provider's rows) -> the service provider -> its name and region. What the name
    comes from (codes, organisations, lines, result) in the service provider's colour; organisations of the same region
    (they back it up) in a light shade of it; the rest grey; the region's name in the colour wherever it appears;
    private organisations dashed. cs: [((Y-tunnus, OID tail), org index[, light])], orgs: [(name, maakunta or None,
    % of rows, role: "name" (the name comes from it), "region" (same region) or None[, extra lines])]: a box with extra
    lines groups several organisations of one region (e.g. "+ other organisations 5%"); region: the service
    provider's, or None."""
    W, n, gap = 480, len(orgs), 10
    ows = [(W - 16 - (n - 1) * gap) / n] * n
    ox = [8 + i * (ows[0] + gap) for i in range(n)]
    extra = [list(o[4]) if len(o) > 4 else [] for o in orgs]
    oh = 40 + 15 * max(len(e) for e in extra)  # organisation box height
    col = [colour if o[3] else "var(--other)" for o in orgs]
    fade = lambda i: {"name": "", "region": ' opacity=".45"'}.get(orgs[i][3], ' opacity=".6"')  # noqa: E731
    reg_t = lambda mk: (f'<tspan style="fill:{colour};font-weight:600">{E(mk)}</tspan>'  # noqa: E731
                        if region and mk == region else E(mk))
    by_org = {i: [(c[0], len(c) > 2 and c[2]) for c in cs if c[1] == i] for i in range(n)}
    k_max = 1
    cy = 26
    oy = cy + k_max * 24 + 22
    sy, ny = oy + oh + 38, oy + oh + 112
    el = []
    for i in range(n):  # codes (OIDs), stacked over their organisation: the Y-tunnus in it names the organisation
        for j, ((yt, tail), light) in enumerate(by_org[i][:1]):  # one box per organisation: its Y-tunnus, all its codes
            y = cy + j * 24
            ow = ows[i]
            code = f'1.2.246.10.<tspan class="yt" style="fill:{col[i]}">{yt}</tspan>.*'
            el.append(f'<g{" opacity=\".45\"" if light else fade(i)}><rect class="nd" x="{ox[i]:.1f}" y="{y}" width="{ow:.1f}" height="20" rx="4" '
                      f'style="stroke:{col[i]};stroke-width:1.4"/><text class="tc" x="{ox[i] + ow / 2:.1f}" y="{y + 14}" '
                      f'text-anchor="middle">{code}</text></g>')
        ow = ows[i]
        el.append(f'<path d="M{ox[i] + ow / 2:.1f},{cy + 20} L{ox[i] + ow / 2:.1f},{oy}" '
                  f'stroke="{col[i]}" stroke-width="1.4"{fade(i)}/>')
    for i, (org, mk, pc, use, *_) in enumerate(orgs):  # organisation -> service provider, straight down, width = share
        x, lw = ox[i] + ows[i] / 2, max(1.5, pc / 8)
        right = x > W * 0.75  # near the right edge: the label on the line's left
        el.append(f'<path d="M{x:.1f},{oy + oh} L{x:.1f},{sy}" fill="none" stroke="{col[i]}" stroke-width="{lw:.1f}"'
                  f'{fade(i)}><title>{E(org)}: {pc}% of its rows</title></path>')
        el.append(f'<text class="ts" x="{x - lw / 2 - 6 if right else x + lw / 2 + 6:.1f}" y="{(oy + oh + sy) / 2 + 4:.1f}" '
                  f'text-anchor="{"end" if right else "start"}"{fade(i)}>{pc}%</text>')
    for i, (org, mk, pc, use, *_) in enumerate(orgs):
        ow = ows[i]
        el.append(f'<g{fade(i)}><rect class="nd" x="{ox[i]:.1f}" y="{oy}" width="{ow:.1f}" height="{oh}" rx="5" '
                  f'style="stroke:{col[i]};stroke-width:{2 if use == "name" else 1.6 if use else 1.4}"{"" if mk else " stroke-dasharray=\"4 3\""}/>'
                  f'<text class="t" x="{ox[i] + ow / 2:.1f}" y="{oy + 17}" text-anchor="middle">{E(org)}</text>'
                  + "".join(f'<text class="t" x="{ox[i] + ow / 2:.1f}" y="{oy + 32 + 15 * k}" text-anchor="middle" '
                            f'opacity=".55">{E(line)}</text>' for k, line in enumerate(extra[i]))
                  + f'<text class="ts" x="{ox[i] + ow / 2:.1f}" y="{oy + 32 + 15 * len(extra[i])}" text-anchor="middle">'
                  f'{reg_t(mk) if mk else "private"}</text></g>')
    # the service provider: one box under all its organisations (all their rows are its rows)
    el.append(f'<rect class="nd" x="8" y="{sy}" width="{W - 16}" height="38" rx="6" style="stroke:{colour};stroke-width:2"/>'
              f'<text class="tsp" x="{W / 2}" y="{sy + 25}" text-anchor="middle">SP: {sp}</text>')
    el.append(f'<path d="M{W / 2},{sy + 38} L{W / 2},{ny - 6}" stroke="var(--muted)" stroke-width="1.4"/>'
              f'<path d="M{W / 2 - 5},{ny - 9} L{W / 2},{ny - 1} L{W / 2 + 5},{ny - 9} z" fill="var(--muted)"/>')
    el.append(f'<rect class="nd out" x="{W / 2 - 105}" y="{ny}" width="210" height="62" rx="6" style="stroke:{colour}"/>'
              f'<text class="tb" x="{W / 2}" y="{ny + 21}" text-anchor="middle">{E(name)}</text>'
              f'<text class="ts" x="{W / 2}" y="{ny + 39}" text-anchor="middle">'
              f'{"region: " + reg_t(region) if region else "no region"}</text>'
              f'<text class="ts" x="{W / 2}" y="{ny + 54}" text-anchor="middle">{E(note)}</text>')
    el.append(f'<text class="hd" x="8" y="{cy - 8}">CODING SYSTEMS (OIDS; Y-TUNNUS IN BOLD, * = ALL ITS CODES)</text>'
              f'<text class="hd" x="8" y="{oy - 8}">ORGANISATIONS (BY Y-TUNNUS)</text>')
    return (f'<div class="namecase" style="border-top-color:{colour}"><h4>{title}</h4><div class="namefig">'
            f'<svg viewBox="0 0 {W} {ny + 66}" role="img" aria-label="{E(title)}: named {E(name)}, region {E(region or 'none')}">'
            f'{"".join(el)}</svg></div></div>')


def naming_figure():
    """How a service provider gets its name and region: three made-up examples side by side (regional organisations
    only; a private top organisation with regional ones; private only). The rule of scripts/sote/region/sp_names.py."""
    cases = [
        naming_case("Regional → named after its organisation",
                    [(("11111118", ""), 0), (("22222226", ""), 1)],
                    [("HUS-yhtymä", "Uusimaa", 90, "name"), ("Helsingin kaupunki", "Uusimaa", 10, "region")],
                    "8f3a91…1a2b3c", "HUS-yhtymä", "Uusimaa", "regional rows 100%", "var(--s1)"),
        naming_case("Private + regional → named after its region",
                    [(("33333334", ""), 0), (("44444442", ""), 1), (("55555550", ""), 2)],
                    [("Fimlab Oy", None, 62, None), ("Pirkanmaan hva", "Pirkanmaa", 33, "name"),
                     ("other organisations", "Pirkanmaa", 5, "region")],
                    "c27e04…76628f", "Pirkanmaa", "Pirkanmaa", "regional rows 38%", "var(--s2)"),
        naming_case("Private only → named after its organisation",
                    [(("66666668", ""), 0), (("22222226", ""), 1)],
                    [("Terveystalo", None, 97, "name"), ("Helsingin kaupunki", "Uusimaa", 3, None)],
                    "5d0b6e…9d8e7f", "Terveystalo", None, "regional 3% (< 5%)", "var(--s3)")]
    return (f'<div class="chart" style="margin: 4px 0 16px"><h3>How a service provider gets its name and region</h3>'
            f'<div class="namecases">{"".join(cases)}</div>'
            f'<p class="q" style="margin-top:8px">Named after its top organisation; when that one has no maakunta (a '
            f'private company, a lab serving several regions), after its region instead, if it has one. Its region: the '
            f'maakunta of its rows whose organisation has one, when those are at least 5% of its rows with an organisation. '
            f'Each coding system is an OID whose Y-tunnus (bold) is its organisation&#39;s business ID. In colour (one per service provider): what its name comes from, from the codes on; light: '
            f'organisations of the same region, which back it up; the region&#39;s name in the colour; grey: the rest; dashed: private (no maakunta); line width: share of the service provider&#39;s rows. Made-up examples.</p></div>')


def labs_panel(labs, lab_conc, paged=frozenset(), regions=None):
    head = "".join(f'<th class="{c}" scope="col">{h}</th>' for h, c in (
        ("Service Provider", ""), ("People", "num"), ("% of all results", "num"), ("Years", "num"), ("Region", ""),
        ("Regional rows", "num"), ("Top organisation", ""), ("Share of that organisation", "num"), ("No organisation", "num"),
        ("Concepts here", "num")))
    trs = "".join(
        f'<tr data-text="{E((r.label + " " + r.named_after).lower())}"><td>{lab_link(r.lab_id, r.label, paged)}</td>'
        f'{count_td(r.people)}<td class="num" data-sort="{r.pct_results}">{r.pct_results:.2f}%</td>'
        f'<td class="num">{"" if not num(r.year_first) else f"{int(r.year_first)}–{int(r.year_last)}"}</td>'
        f'<td>{region_link(r.region) + E(region_s(r)[len(r.region):]) if region_s(r) else ""}</td><td class="num" data-sort="{r.regional_pct if num(r.regional_pct) else ""}">'
        f'{pct_s(r.regional_pct, 0) if region_s(r) else "<span class=muted>—</span>"}</td>'
        f'<td>{E(r.named_after) or "<span class=muted>—</span>"}</td>'
        f'<td class="num" data-sort="{r.named_pct if num(r.named_pct) else ""}">{named_chip(r.named_pct)}</td>'
        f'<td class="num" data-sort="{r.no_org_pct if num(r.no_org_pct) else ""}">{pct_s(r.no_org_pct, 0)}</td>'
        f'<td class="num">{len(lab_conc.get(r.lab_id, []))}</td></tr>' for r in labs.itertuples())
    return f"""<section id="panel-sp" hidden>
  {naming_figure()}
  {region_choropleth(labs, regions)}
  <p class="lead" style="margin-top:0">The service providers in this release with at least {MIN_N} people. A service provider's
  name is its top coding-system organisation, as in the Service Provider reports; when that organisation has no region of
  its own (a private company such as Fimlab, a lab serving several regions such as NordLab), its region instead, when it
  has one. Region, as in scripts/sote: the maakunta of its organisations that have one, with that maakunta's share of
  them in brackets when under 100%, shown when those regional rows are at least 5% of its results with an organisation
  (Regional rows).</p>
  <div class="controls" style="margin-top:12px"><input class="search" type="search" placeholder="Search service providers, organisations…" aria-label="Search service providers" data-search="labs"><span class="count" data-count="labs"></span></div>
  <div class="tablebox"><table class="sortable" id="labs" data-limit="20"><thead><tr>{head}</tr></thead><tbody>{trs}</tbody></table></div>
  <p class="muted" style="font-size:16px;margin:8px 2px 0">As <a href="lab/service_providers.tsv">service_providers.tsv</a>,
  <a href="lab/sp_organisations.tsv">sp_organisations.tsv</a>, <a href="lab/sp_years.tsv">sp_years.tsv</a>. Share of that organisation:
  its share of the service provider's results that carry an organisation (green 90% and more, amber 60–90%, red under 60%).</p>
</section>"""


def region_map_card(r):
    """The service provider's region on the map of Finland (drawn by the page script from assets/finland.js)."""
    reg = region_s(r)
    if not reg:
        return ""
    return (f'<div class="chart mapcard"><h3>Region: {E(reg)}</h3><p class="q">The maakunta of its coding-system '
            f'organisations that have one ({pct_s(getattr(r, "regional_pct", None), 0)} of its results with an '
            f'organisation)</p><div class="fimap" data-map-region="{E(r.region)}"></div>'
            f'<p class="q" style="font-size:12px">Map: Statistics Finland, CC BY 4.0</p></div>')


def lab_page(r, lo, ly, concepts):
    tile = lambda k, v, d="": (f'<div class="tile"><div class="k">{k}</div><div class="v">{v}</div>'  # noqa: E731
                               + (f'<div class="d">{d}</div>' if d else "") + "</div>")
    yrs = f"{int(r.year_first)}–{int(r.year_last)}" if num(r.year_first) else "—"
    tiles = "".join([
        tile("People", f'<span data-n="{int(r.people)}">{int(r.people):,}</span>'),
        tile("Share of all results", f"{r.pct_results:.2f}%"), tile("Years", yrs),
        tile("Region", E(region_s(r)) or "—", "maakunta of its regional organisations (and their share in it)"),
        tile("Regional rows", pct_s(r.regional_pct, 0) if region_s(r) else "—",
             "of its results with an organisation (the rest: private or serving several regions)"),
        tile("Top organisation", E(r.named_after) or "—", "its most common coding-system organisation"),
        tile("Share of that organisation", named_chip(r.named_pct), "of its results with an organisation"),
        tile("No organisation", pct_s(r.no_org_pct, 0), "results without a coding-system organisation")])
    _, ocol = lab_colors(ly)
    # the organisations in the over-time chart wear its colours (it has no legend); 'no organisation' is a pale grey
    # in the chart, so its name gets a swatch instead
    oname = lambda org: (f'<span class="swatch" style="background:{ocol[org]}"></span>{E(org)}' if org == NONE_ORG
                         else f'<b style="color:{ocol[org]}">{E(org)}</b>' if org in ocol else E(org))  # noqa: E731
    orows = "".join(f'<tr><td>{oname(org)}</td><td class="num" data-sort="{pc}">{pc:.1f}%</td></tr>'
                    for org, pc in sorted(zip(lo["org"], lo["pct"]), key=lambda t: -t[1]))
    crows = "".join(f'<tr><td><a href="../omop/{o}.html">{E(name or "OMOP " + o)}</a></td>{count_td(n)}'
                    f'<td class="num" data-sort="{pc}">{pc:.1f}%</td><td class="num" data-sort="{rt}">{ratio_s(rt)}</td></tr>'
                    for o, name, pc, rt, n in sorted(concepts, key=lambda c: -c[2]))
    conc = (f"""<section><h2>Concepts on this portal <small>its results in each concept (the years shown in the Service Provider report) and their share of the concept's results (over the service providers listed there)</small></h2>
  <div class="tablebox"><table class="sortable" id="lconc"><thead><tr><th>Concept</th><th class="num">N rows</th><th class="num">% of the concept</th>
  <th class="num">Ratio to all results</th></tr></thead><tbody>{crows}</tbody></table></div></section>""" if crows else "")
    return f"""
<div class="crumb"><a href="../index.html#sp">← All Service Providers</a></div>
<section>
  <h1>{E(r.label)}</h1>
  <div class="idline"><span>service provider, pseudonym ending <span class="mono">{E(r.lab_id.split("-")[0])}</span></span></div>
</section>
<section class="tiles">{tiles}</section>
<section><h2>Composition <small>the coding-system organisations behind this service provider</small></h2>
  <div class="charts">{region_map_card(r)}
  <div class="chart"><h3>Organisations, all years</h3><p class="q">Share of the service provider's results; organisations covering too few
  people pooled</p><div class="tablebox"><table class="sortable" id="lorgs"><thead><tr><th>Organisation</th>
  <th class="num">Share</th></tr></thead><tbody>{orows}</tbody></table></div></div></div>
  <div class="charts wide" style="margin-top:14px">{lab_years_svg(ly)}</div></section>
{conc}"""


# ---------------------------------------------------------------------------
# Native charts: the audit and the lab report drawn in the page (from the gated tables), with how to read them
# ---------------------------------------------------------------------------

CHECKS = [("d_ru", "1 · Range vs canonical", "Does the row's unit match its reference range? The range, converted "
           "with the row's own harmonization factor, against the concept's canonical range: a unit error moves it by a "
           "unit factor (×10, ×1000, …)."),
          ("d_vr", "2 · Value vs its own range", "Does the row agree with itself? The value against its own reference "
           "range (no unit needed), relative to the concept's typical."),
          ("d_vu", "3 · Harmonized value vs concept", "Is the harmonized value on scale? Against the concept's median.")]
FACTORS = [(-3, "/1000"), (-2, "/100"), (-1, "/10"), (0, "×1"), (1, "×10"), (2, "×100"), (3, "×1000")]


def how(text):
    return f'<p class="how"><b>How to read</b> {text}</p>'


def svg_offsets(off, meta, ks):
    """The three checks as small multiples: one row per test name (the largest, then the rest), one column per check;
    each a histogram of the offsets (pooled bins, share of the test's rows per 0.05 log10, log scale), the band where
    the row counts as on scale shaded."""
    tests = [t for t in meta.get("keep", []) if t in set(off["test"])] + (["other"] if (off["test"] == "other").any() else [])
    if not tests:
        return ""
    cw, chh, lw, gap, top = 330, 62, 170, 22, 22
    W = lw + 3 * cw + 2 * gap + 10
    H = top + len(tests) * (chh + 10) + 28
    tol = {"d_ru": meta.get("rtol") or 0, "d_vr": meta.get("tol_vr") or 0, "d_vu": meta.get("tol_v") or 0}
    xl = 3.5
    g = []
    dmap = dict(zip(ks["test"], ks["D (KS)"])) if ks is not None and len(ks) else {}
    differs = set(ks.loc[ks["differs"] == "yes", "test"]) if ks is not None and len(ks) else set()
    for j, (col, title, _) in enumerate(CHECKS):
        x0 = lw + j * (cw + gap)
        X = lambda v, x0=x0: x0 + (min(max(v, -xl), xl) + xl) / (2 * xl) * cw  # noqa: E731
        g.append(f'<text x="{x0}" y="12" font-weight="600">{E(title)}</text>')
        for i, t in enumerate(tests):
            y0 = top + i * (chh + 10)
            yb = y0 + chh
            d = off[(off["check"] == col) & (off["test"] == t)].sort_values("lo")
            g.append(f'<rect x="{X(-tol[col]):.1f}" y="{y0}" width="{X(tol[col]) - X(-tol[col]):.1f}" height="{chh}" '
                     f'fill="var(--soft)"/>')
            for f, _ in FACTORS:
                g.append(f'<line x1="{X(f):.1f}" x2="{X(f):.1f}" y1="{y0}" y2="{yb}" stroke="var(--line)" '
                         f'stroke-width="{1 if f == 0 else 0.5}"/>')
            g.append(f'<line x1="{x0}" x2="{x0 + cw}" y1="{yb}" y2="{yb}" stroke="var(--muted)" stroke-width="0.8"/>')
            if len(d):
                dens = d["share"] * 0.05 / (d["hi"] - d["lo"])
                lo_l, hi_l = -4.0, max(0.0, float(np.log10(dens.max())))  # log share from 1e-4 up
                Yv = lambda v: yb - (min(max(np.log10(max(v, 1e-9)), lo_l), hi_l) - lo_l) / (hi_l - lo_l) * (chh - 4)  # noqa: E731
                path, prev = "", None
                for (a, b), dv in zip(zip(d["lo"], d["hi"]), dens):
                    if prev is None or a > prev + 1e-9:
                        path += f"M{X(a):.1f},{yb} "
                    path += f"V{Yv(dv):.1f} H{X(b):.1f} "
                    prev = b
                    path += ""
                path += f"V{yb}"
                g.append(f'<path d="{path}" fill="var(--accent)" fill-opacity="0.18" stroke="var(--accent)" '
                         f'stroke-width="1.4"><title>{E(t)}: {E(title)}</title></path>')
            if j == 0:
                dd = dmap.get(t)
                bold = t in differs
                g.append(f'<text x="{lw - 10}" y="{y0 + chh / 2:.1f}" text-anchor="end" '
                         f'{"font-weight=\"600\" fill=\"var(--crit-ink)\"" if bold else ""}>{E(t)}</text>')
                if dd:
                    g.append(f'<text x="{lw - 10}" y="{y0 + chh / 2 + 14:.1f}" text-anchor="end" font-size="10">'
                             f'KS D {E(dd)}{" · differs" if bold else ""}</text>')
        for f, lab in FACTORS:
            g.append(f'<text x="{X(f):.1f}" y="{H - 8}" text-anchor="middle" font-size="10">{lab}</text>')
    return f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="The three checks per test name">{"".join(g)}</svg>'


def svg_map(m, meta):
    """The decision map: harmonized rows with a range, value offset (x) against range offset (y), coloured by verdict,
    dot area by share of these rows; cells of fewer than 5 people left out."""
    if m is None or m.empty:
        return ""
    S_, pad, xl = 440, 62, 3.5
    X = lambda v: pad + (v + xl) / (2 * xl) * (S_ - pad - 10)  # noqa: E731
    Y = lambda v: 10 + (xl - v) / (2 * xl) * (S_ - pad - 10)  # noqa: E731
    rt, tv = meta.get("rtol") or 0, meta.get("tol_v") or 0
    g = [f'<rect x="{X(-xl)}" y="{Y(rt):.1f}" width="{X(xl) - X(-xl):.1f}" height="{Y(-rt) - Y(rt):.1f}" fill="var(--soft)"/>',
         f'<rect x="{X(-tv):.1f}" y="{Y(xl)}" width="{X(tv) - X(-tv):.1f}" height="{Y(-xl) - Y(xl):.1f}" fill="var(--soft)"/>',
         f'<line x1="{X(-xl)}" y1="{Y(-xl)}" x2="{X(xl)}" y2="{Y(xl)}" stroke="var(--line)" stroke-dasharray="4 3"/>']
    for f, lab in FACTORS:
        g.append(f'<line x1="{X(f):.1f}" x2="{X(f):.1f}" y1="{Y(xl)}" y2="{Y(-xl)}" stroke="var(--line)" stroke-width="0.5"/>'
                 f'<line x1="{X(-xl)}" x2="{X(xl)}" y1="{Y(f):.1f}" y2="{Y(f):.1f}" stroke="var(--line)" stroke-width="0.5"/>'
                 f'<text x="{X(f):.1f}" y="{S_ - pad + 16}" text-anchor="middle" font-size="10">{lab}</text>'
                 f'<text x="{pad - 6}" y="{Y(f) + 4:.1f}" text-anchor="end" font-size="10">{lab}</text>')
    smax = m["share"].max()
    for r in m.sort_values("share", ascending=False).itertuples():
        rad = 2 + 16 * np.sqrt(r.share / smax)
        g.append(f'<circle cx="{X(r.bx):.1f}" cy="{Y(r.by):.1f}" r="{rad:.1f}" fill="{A.VCOLOR.get(r.verdict, "#a8a69c")}" '
                 f'fill-opacity="0.85" stroke="var(--surface)" stroke-width="1"><title>{E(r.verdict)}: value '
                 f'{10 ** r.bx:.3g}× the concept median, range {10 ** r.by:.3g}× the canonical; {100 * r.share:.1f}% of '
                 f'these rows</title></circle>')
    for x, y, txt in [(0.25, 0.2, "ok"), (-3.3, -1.4, "wrong unit"), (0.3, 2.7, "range off scale"),
                      (-3.3, 0.2, "value mis-scaled")]:
        g.append(f'<text x="{X(x):.1f}" y="{Y(y):.1f}" font-size="10" font-style="italic">{txt}</text>')
    g.append(f'<text x="{(X(-xl) + X(xl)) / 2:.1f}" y="{S_ - 6}" text-anchor="middle" font-size="10.5">value / concept median</text>'
             f'<text transform="translate(12,{(Y(xl) + Y(-xl)) / 2:.1f}) rotate(-90)" text-anchor="middle" font-size="10.5">'
             f'range / canonical range</text>')
    seen = [v for v, _, _ in A.VERDICTS if v in set(m["verdict"])]
    lg = "".join(f'<span><i style="background:{A.VCOLOR[v]};height:9px;width:9px;border-radius:50%"></i>{E(v)}</span>' for v in seen)
    return (f'<svg viewBox="0 0 {S_} {S_}" role="img" aria-label="Decision map" style="max-width:560px">{"".join(g)}</svg>'
            f'<div class="lg">{lg}</div>')


def svg_stacked(rows, order_keys, colors, label_w=130, row_h=26, share=True):
    """Horizontal stacked bars: rows = [(label, {key: value}), ...]; share=True: each bar 0-100%, else counts (the
    count written at the bar's end, and in data-n for the last scan)."""
    if not rows:
        return ""
    W = 520
    H = 10 + row_h * len(rows) + 24
    vmax = 1.0 if share else max(sum(v.values()) for _, v in rows) * 1.15
    X = lambda v: label_w + v / vmax * (W - label_w - 70)  # noqa: E731
    g = []
    for i, (lab, vals) in enumerate(rows):
        y = 10 + i * row_h
        tot = sum(vals.values())
        acc = 0.0
        g.append(f'<text x="{label_w - 8}" y="{y + row_h / 2 + 3:.1f}" text-anchor="end">'
                 f'{E(lab if len(lab) <= 18 else lab[:17] + "…")}<title>{E(lab)}</title></text>')
        for k in order_keys:
            v = vals.get(k, 0)
            if v <= 0:
                continue
            w = v / tot if share else v
            g.append(f'<rect x="{X(acc):.1f}" y="{y + 4}" width="{X(acc + w) - X(acc):.1f}" height="{row_h - 8}" '
                     f'fill="{colors.get(k, "#a8a69c")}" stroke="var(--surface)" stroke-width="1"'
                     f'{"" if share else f" data-n=\"{int(v)}\""}><title>{E(lab)}: {E(k)} '
                     f'{f"{100 * w:.1f}%" if share else f"{int(v):,} rows"}</title></rect>')
            acc += w
        if not share:
            g.append(f'<text x="{X(acc) + 6:.1f}" y="{y + row_h / 2 + 3:.1f}" data-n="{int(tot)}">{int(tot):,}</text>')
    ticks = [0, 0.25, 0.5, 0.75, 1.0] if share else []
    for t in ticks:
        g.append(f'<text x="{X(t):.1f}" y="{H - 6}" text-anchor="middle" font-size="10">{int(t * 100)}%</text>')
    keys = [k for k in order_keys if any(v.get(k, 0) > 0 for _, v in rows)]
    lg = "".join(f'<span><i style="background:{colors.get(k, "#a8a69c")};height:9px;width:9px"></i>{E(k)}</span>' for k in keys)
    return f'<svg viewBox="0 0 {W} {H}" role="img">{"".join(g)}</svg><div class="lg">{lg}</div>'


def audit_section(o, d, files):
    """Units and reference ranges, from the audit's tables (the tables dir) and its published TSVs (the site copies)."""
    meta_p = d / "audit_meta.json"
    if not meta_p.exists():
        return ""
    meta = json.loads(meta_p.read_text())
    rd = lambda n: read_tsv(d / f"{n}.tsv") if (d / f"{n}.tsv").exists() else pd.DataFrame()  # noqa: E731
    off, mp, vb, st = (rd(n) for n in ("audit_offsets", "audit_map", "audit_vbars", "audit_stages"))
    fl = (pd.read_csv(d / "audit_flagged.tsv", sep="\t", dtype=str, keep_default_na=False)
          if (d / "audit_flagged.tsv").exists() else pd.DataFrame())
    tp = next((p for k, p in files if k == "audit" and p.name.endswith("_ref_audit_tests.tsv")), None)
    ks = pd.read_csv(tp, sep="\t", dtype=str, keep_default_na=False) if tp is not None else None
    cp = next((p for k, p in files if k == "audit" and p.name.endswith("_ref_audit_canonical.tsv")), None)
    canon = ""
    if cp is not None:
        c = pd.read_csv(cp, sep="\t", dtype=str, keep_default_na=False)
        c = c[(c["use"] == "True") & (c["rows"] != BELOW)]
        canon = " · ".join(f'{"all" if r.sex == "all" else r.sex + " " + r.age}: '
                           f'{fmt_lim(pd.to_numeric(r.lower, errors="coerce"), pd.to_numeric(r.upper, errors="coerce"))}'
                           for r in c.itertuples())
    unit = E(meta.get("target") or "")
    med = meta.get("median")
    head = (f'<p class="lead" style="margin:0">Every row has three witnesses of its scale: its value, its reference range and '
            f'its unit. The audit checks them against each other and gives each row a verdict. Concept median '
            f'<b>{fnum(med) if med else "—"} {unit}</b>; canonical range ({unit}): {canon or "—"}.</p>')
    parts = [head]
    if len(off):
        parts.append(f'<div class="chart"><h3>The three checks, per test name</h3>'
                     f'{how("Each small histogram shows where a test name&#39;s rows fall on one check, on a log scale of "
                            "factors: ×1 is on scale, the grey band is the tolerance. A bump at ×1000 or /1000 is a unit "
                            "error (mg vs µg); a bump at another factor, e.g. ×10, often a decimal or unit mix-up. Bins "
                            "are pooled until each covers at least 5 people.")}{svg_offsets(off, meta, ks)}</div>')
    row = []
    if len(mp):
        row.append(f'<div class="chart"><h3>Decision map</h3>'
                   f'{how("Harmonized rows with a range: x is the value against the concept median, y the range against "
                          "the canonical range. Rows on the diagonal moved value and range together: the unit is wrong. "
                          "Off on x only: the value is mis-scaled. Off on y only: the range is on another scale. Dot size: "
                          "share of these rows.")}{svg_map(mp, meta)}</div>')
    if len(vb):
        keep = [t for t in meta.get("keep", []) if t in set(vb["test"])] + (["other"] if (vb["test"] == "other").any() else [])
        rows = [(t, dict(zip(vb.loc[vb["test"] == t, "verdict"], pd.to_numeric(vb.loc[vb["test"] == t, "rows"])))) for t in keep]
        vcols = [v for v, _, _ in A.VERDICTS]
        row.append(f'<div class="chart"><h3>Verdicts per test name</h3>'
                   f'{how("The share of each test name&#39;s rows per verdict (green: fine; red, orange, yellow, purple: "
                          "flagged). A test that is mostly flagged has a systematic problem; compare with its KS D above.")}'
                   f'{svg_stacked(rows, vcols, A.VCOLOR)}</div>')
    if len(st):
        rows = [(v, dict(zip(st.loc[st["verdict"] == v, "stage"], pd.to_numeric(st.loc[st["verdict"] == v, "rows"]))))
                for v in A.FLAGGED if (st["verdict"] == v).any()]
        row.append(f'<div class="chart"><h3>Flagged rows: where the unit came from</h3>'
                   f'{how("For the flagged rows, the stage their unit comes from: the service provider (kept as sent), injected by "
                          "the engine (missing before), rewritten by injection, or no unit at all. Rows, shown groups only.")}'
                   f'{svg_stacked(rows, [k for k, _ in A.STAGES], dict(A.STAGES), share=False)}</div>')
    if row:
        parts.append(f'<div class="charts">{"".join(row)}</div>')
    if len(fl):
        cols = list(fl.columns)
        th = "".join(f'<th{" class=num" if c in ("rows", "% rows") else ""}>{E(c)}</th>' for c in cols)
        trs = "".join("<tr>" + "".join(
            (count_td(pd.to_numeric(r[c], errors="coerce")) if c == "rows" else
             f'<td><span class="chip" style="background:{A.VCOLOR.get(r[c], "#ccc")}33">{E(r[c])}</span></td>' if c == "verdict"
             else f'<td class="{"num" if c == "% rows" else ""}">{E(str(r[c]))}</td>') for c in cols) + "</tr>"
            for _, r in fl.iterrows())
        parts.append(f'<div><h3 class="sub">The largest flagged groups</h3>'
                     f'{how("Each row is a test name × unit path (source unit → unit after injection → harmonized unit) "
                            "with one verdict: its rows, the median range and values, how far they are off (range x, "
                            "value x) and the unit the audit suggests.")}'
                     f'<div class="tablebox"><table class="sortable" id="aflag"><thead><tr>{th}</tr></thead>'
                     f'<tbody>{trs}</tbody></table></div></div>')
    return (f'<section id="units"><h2>Units and reference ranges <small>omop_ref_audit</small></h2>'
            f'<div class="stackv">{"".join(parts)}</div></section>')


HM_METRICS = [  # (key, menu title, run, column, kind, how to read)
    ("shift_raw", "Median vs national: raw values", "raw", "x_shift", "ratio",
     "Each cell is one service provider in one year: its median value against the national median of the same sex and "
     "age (orange higher, blue lower). A whole row off is the service provider's patients or method; a change from one "
     "year to the next is a method "
     "change or a data problem."),
    ("shift_uln", "Median vs national: × upper limit", "uln", "x_shift", "ratio",
     "The same with every value divided by its own row's upper reference limit. A service provider off on raw values but not here "
     "uses another assay whose limits follow it; off here too, its values moved and its limits did not."),
    ("shift_lln", "Median vs national: × lower limit", "lln", "x_shift", "ratio",
     "The same with every value divided by its own row's lower reference limit: the low side (e.g. HDL, sodium)."),
    ("above_uln", "% above the service provider's own upper limit", "raw", "above_uln_pct", "pct",
     "Share of the service provider's results that year above their own upper reference limit."),
    ("below_lln", "% below the service provider's own lower limit", "raw", "below_lln_pct", "pct",
     "Share of the service provider's results that year below their own lower reference limit."),
    ("outliers", "Outliers: observed / expected", "raw", "outliers_oe", "oe",
     "Values outside the national fences (same sex and age), against what the national rate would give: 1 is "
     "normal, 3 or more is flagged."),
    ("jump", "Person jumps: observed / expected", "raw", "person_jump_oe", "oe",
     "Values far from the same person's other values of this test, against the national rate: wrong scale, wrong "
     "test or wrong person."),
    ("qc", "% failing the engine's QC", "raw", "qc_fail_pct", "pct",
     "Share of the service provider's results that year with QC_PASS = 0 (the reason is in QC_NOTES)."),
    ("outcome", "% outcome (H/L/N) against own range", "raw", "outcome_mismatch_pct", "pct",
     "Share of results whose TEST_OUTCOME disagrees with their own reference limits."),
]


def heatmap_data(spy_by_run, order, paged, ids_by_label):
    """The lab x year heatmap's data for the page's script: per measure, per lab, per year [colour value, text] or
    'B' (below threshold). Only the heatmap's labs (filtered first: a concept can have thousands of labs). No counts."""
    want = set(order["label"])
    runs = {}
    for run, d in spy_by_run.items():
        d = d[d["sp"].isin(want)].copy()
        d["y"] = pd.to_numeric(d["year"], errors="coerce")
        runs[run] = d[d["y"].notna()]
    years = sorted({int(y) for d in runs.values() for y in d["y"]})
    metrics, cells = [], {}
    for key, title, run, col, kind, how_ in HM_METRICS:
        d = runs.get(run)
        if d is None or col not in d.columns or d.empty:
            continue
        nat = None
        num_v = pd.to_numeric(d[col], errors="coerce").to_numpy()
        vals, nums = {}, []
        for sp, y, v, x, rows in zip(d["sp"], d["y"], d[col], num_v, d["rows"]):
            cell = None
            if v == BELOW or rows == BELOW:
                cell = "B"
            elif kind == "ratio":
                t = parse_x(v)
                if t is not None:
                    cell = [round(t, 5), ("×" if t >= 0 else "/") + str(v)[1:]]
            elif not np.isnan(x):
                if kind == "pct":
                    cell = [float(x), f"{x:.1f}%" if x < 10 else f"{x:.0f}%"]
                elif kind == "oe":
                    cell = [float(np.log10(max(x, 0.01))), f"{x:.1f}"]
            if cell is not None:
                vals.setdefault(sp, {})[str(int(y))] = cell
                if cell != "B":
                    nums.append(cell[0])
        if not vals:
            continue
        vmax = (max([abs(x) for x in nums] + [np.log10(1.1)]) if kind in ("ratio", "limit") else
                max(nums + [5.0]) if kind == "pct" else 1.0)
        vmax = min(vmax, np.log10(2)) if kind in ("ratio", "limit") else vmax
        metrics.append({"key": key, "title": title, "run": run, "kind": kind, "how": how_, "vmax": vmax,
                        "nat": fnum(nat) if nat else ""})
        cells[key] = vals
    labs = {g: [[lab, ids_by_label.get(lab, ""), ids_by_label.get(lab, "") in paged]
                for lab in order.loc[order["group"] == g, "label"]] for g in ("largest", "deviating")}
    return {"years": years, "labs": labs, "metrics": metrics, "cells": cells}


def svg_boxes(lv, labels, run, h_unit):
    """Each lab's values relative to the national median (log scale): box p25-p75, whiskers p5-p95, line the median."""
    d = lv[lv["run"] == run].set_index("label")
    if d.empty or "national" not in d.index:
        return ""
    nat = float(d.loc["national", "median"])
    rows = [lab for lab in labels if lab in d.index]
    lw, rh, W = 260, 24, 1500
    H = 10 + rh * max(len(labels), 1) + 30
    qs = d.loc[rows, ["q05", "q95"]].astype(float) if rows else pd.DataFrame()
    lo = min([0.5] + list(qs["q05"])) if rows else 0.5
    hi = max([2.0] + list(qs["q95"])) if rows else 2.0
    lo, hi = np.log10(max(lo, 1e-3)) - 0.05, np.log10(min(hi, 1e3)) + 0.05
    X = lambda v: lw + (np.log10(v) - lo) / (hi - lo) * (W - lw - 190)  # noqa: E731
    g = [f'<line x1="{X(1):.1f}" x2="{X(1):.1f}" y1="6" y2="{H - 24}" stroke="var(--muted)"/>']
    for e in range(int(np.floor(lo)), int(np.ceil(hi)) + 1):
        for f in (1, 2, 5):
            v = e + np.log10(f)
            if lo <= v <= hi:
                g.append(f'<line x1="{X(10 ** v):.1f}" x2="{X(10 ** v):.1f}" y1="6" y2="{H - 24}" stroke="var(--line)" '
                         f'stroke-width="0.5"/><text x="{X(10 ** v):.1f}" y="{H - 8}" text-anchor="middle" font-size="10">'
                         f'{"×" if v >= 0 else "/"}{fnum(10 ** abs(v))}</text>')
    unit = h_unit if run == "raw" else ("× ULN" if run == "uln" else "× LLN")
    for i, lab in enumerate(labels):
        y = 10 + i * rh + rh / 2
        g.append(f'<text x="{lw - 8}" y="{y + 4:.1f}" text-anchor="end">{E(lab if len(lab) <= 34 else lab[:33] + "…")}'
                 f'<title>{E(lab)}</title></text>')
        if lab not in d.index:
            g.append(f'<text x="{lw + 4}" y="{y + 4:.1f}" font-size="10">too few rows or people to show</text>')
            continue
        q = d.loc[lab, ["q05", "q25", "q50", "q75", "q95"]].astype(float).to_numpy()
        g.append(f'<line x1="{X(q[0]):.1f}" x2="{X(q[4]):.1f}" y1="{y:.1f}" y2="{y:.1f}" stroke="var(--accent)" stroke-width="1.2"/>'
                 f'<rect x="{X(q[1]):.1f}" y="{y - 7:.1f}" width="{max(X(q[3]) - X(q[1]), 1):.1f}" height="14" rx="2" '
                 f'fill="var(--accent)" fill-opacity="0.25" stroke="var(--accent)"/>'
                 f'<line x1="{X(q[2]):.1f}" x2="{X(q[2]):.1f}" y1="{y - 8:.1f}" y2="{y + 8:.1f}" stroke="var(--ink)" '
                 f'stroke-width="2.2"><title>{E(lab)}: median {fnum(q[2] * nat)} {E(unit)} '
                 f'({"×" if q[2] >= 1 else "/"}{fnum(q[2] if q[2] >= 1 else 1 / q[2])} national)</title></line>'
                 f'<text x="{W - 175}" y="{y + 4:.1f}" font-size="10.5">median {fnum(q[2] * nat)} {E(unit)}</text>')
    return f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Values per lab">{"".join(g)}</svg>'


def sp_words(text):
    """sp_qc's texts say lab for a service provider: the portal says service provider."""
    text = re.sub(r"\blabs\b", "service providers", str(text))
    text = re.sub(r"\blab's\b", "service provider's", text)
    return re.sub(r"\blab\b", "service provider", text)


def limit_series(lim):
    """From lab_limits.tsv: the years, {(service provider, limit, sex): {year: (value, share)}} and the national limit per
    (limit, sex)."""
    if lim is None or lim.empty:
        return [], {}, {}
    nat = lim[lim["label"] == "national"]
    natl = {(r.limit, r.sex): float(r.value) for r in nat.itertuples()}
    d = lim[(lim["label"] != "national") & lim["year"].notna()]
    by = {}
    for r in d.itertuples():
        by.setdefault((r.label, r.limit, r.sex), {})[int(r.year)] = (float(r.value), float(r.share))
    yrs = sorted({int(y) for y in d["year"]})
    return yrs, by, natl


def limit_changes(series):
    """A service provider's limit moves in words, from its yearly series: "105 → 97 in 2017"."""
    out, prev = [], None
    for y in sorted(series):
        v = series[y][0]
        if prev is not None and abs(v - prev) > 1e-9:
            out.append(f"{fnum(prev)} → {fnum(v)} in {y}")
        prev = v
    return out


SEX_COL = {"F": "var(--fem)", "M": "var(--s1)"}
SEX_NAME = {"F": "women", "M": "men"}


def limit_card(lab, by, natl, yrs, vmin, vmax, link):
    """One service provider's adult reference limits over the years: per sex (women pink, men blue) its upper and lower
    limit as lines with a dot per year (a gap where no limit is carried by >= MIN_N people that year), the national
    limits dashed in the same colour; one y scale for all cards."""
    W, H, ml, mr, mt, mb = 440, 200, 52, 12, 12, 26
    n = max(len(yrs), 1)
    X = lambda y: ml + (yrs.index(y) + 0.5) / n * (W - ml - mr)  # noqa: E731
    Y = lambda v: mt + (vmax - v) / (vmax - vmin) * (H - mt - mb)  # noqa: E731
    g = []
    span = vmax - vmin
    step = 10 ** np.floor(np.log10(span / 2))
    t = np.ceil(vmin / step) * step
    while t <= vmax:
        g.append(f'<line x1="{ml}" x2="{W - mr}" y1="{Y(t):.1f}" y2="{Y(t):.1f}" stroke="var(--line)" stroke-width="0.6"/>'
                 f'<text x="{ml - 6}" y="{Y(t) + 4:.1f}" text-anchor="end">{fnum(float(t))}</text>')
        t += step * (2 if span / step > 6 else 1)
    every = 4 if n > 12 else 2 if n > 6 else 1
    for i, y in enumerate(yrs):
        if i % every == 0:
            g.append(f'<text x="{X(y):.1f}" y="{H - 6}" text-anchor="middle">{y}</text>')
    changes = []
    for sx in ("F", "M"):
        col = SEX_COL[sx]
        for lim in ("upper", "lower"):
            nat = natl.get((lim, sx))
            if nat is not None and vmin <= nat <= vmax:
                g.append(f'<line x1="{ml}" x2="{W - mr}" y1="{Y(nat):.1f}" y2="{Y(nat):.1f}" stroke="{col}" '
                         f'stroke-opacity="0.55" stroke-width="1.4" stroke-dasharray="5 4"><title>national {lim} limit, '
                         f'{SEX_NAME[sx]}: {fnum(nat)}</title></line>')
            series = by.get((lab, lim, sx), {})
            if not series:
                continue
            path, prev = "", None
            for y in yrs:
                if y in series:
                    v = series[y][0]
                    path += (" L" if prev is not None and yrs.index(y) == yrs.index(prev) + 1 else " M") + f"{X(y):.1f},{Y(v):.1f}"
                    prev = y
            g.append(f'<path d="{path.strip()}" fill="none" stroke="{col}" stroke-width="2.4" stroke-linejoin="round"/>')
            for y, (v, sh) in sorted(series.items()):
                g.append(f'<circle cx="{X(y):.1f}" cy="{Y(v):.1f}" r="3.6" fill="{col}" stroke="var(--surface)" '
                         f'stroke-width="1.2"><title>{y}, {SEX_NAME[sx]}, {lim}: {fnum(v)}'
                         f'{"" if sh >= 0.9 else f" ({100 * sh:.0f}% of that year&#39;s rows)"}'
                         f'{f"; national {fnum(nat)}" if nat else ""}</title></circle>')
            changes += [f"{SEX_NAME[sx]} {lim} {c}" for c in limit_changes(series)]
    what = E("; ".join(changes)) if changes else '<span class="muted">no change</span>'
    return (f'<div class="chart limcard"><h3>{link}</h3><p class="q">{what}</p>'
            f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Reference limits over the years">{"".join(g)}</svg></div>')


def limits_grid(lim, rg, paged, ids_by_label, h_unit, o, show=12, most=48):
    """The reference limits over time as small multiples: the service providers whose limits changed first (most
    changes first), then the rest by rows; the first `show` visible, up to `most` behind a button."""
    yrs, by, natl = limit_series(lim)
    if not by:
        return ""
    labs = sorted({k[0] for k in by})
    size = (rg.assign(r=pd.to_numeric(rg["rows"], errors="coerce")).groupby("sp")["r"].sum()
            if rg is not None and len(rg) else pd.Series(dtype=float))
    nch = {lab: sum(len(limit_changes(by.get((lab, lim_, sx), {}))) for lim_ in ("upper", "lower") for sx in ("F", "M"))
           for lab in labs}
    order = sorted(labs, key=lambda x: (-nch[x], -float(size.get(x, 0) or 0)))[:most]
    vals = [v for k, ser in by.items() if k[0] in order for v, _ in ser.values()] + list(natl.values())
    lo, hi = min(vals), max(vals)
    pad = (hi - lo) * 0.08 or max(hi * 0.05, 1)
    vmin, vmax = max(0.0, lo - pad), hi + pad
    cards = []
    for i, lab in enumerate(order):
        link = lab_link(ids_by_label.get(lab, ""), lab, paged, "../")
        card = limit_card(lab, by, natl, yrs, vmin, vmax, link)
        cards.append(card if i < show else card.replace('<div class="chart limcard">', '<div class="chart limcard more" hidden>', 1))
    more = (f'<button type="button" class="showmore" data-more="lim{o}">Show {len(order) - show} more</button>'
            if len(order) > show else "")
    n_ch = sum(1 for x in labs if nch[x])
    return (f'<div><h3 class="sub">Reference limits over the years (adults)</h3>'
            f'{how(f"One small chart per service provider: its most common adult reference limits each year ({E(h_unit)}), "
                   "women pink and men blue, the upper limit the higher line and the lower limit the lower one; the "
                   "national limits dashed in the same colours. A step is a change of limits (a new method or new reference "
                   "values); a gap is a year where no limit was carried by at least 5 people. Service providers whose limits "
                   f"changed come first ({n_ch} of {len(labs)}); all charts share one scale.")}'
            f'<div class="limgrid" id="lim{o}">{"".join(cards)}</div>{more}</div>')


def lab_report_section(o, d, files, paged, ids_by_label, h_unit):
    """Labs over time, from sp_qc's published TSVs (and the lab value boxes from the tables): controls, the lab x year
    heatmap (drawn by the page script from embedded data), the value boxes, the reference ranges and the flags."""
    sp = {p.name: p for k, p in files if k == "sp"}
    ids = pd.DataFrame({"label": list(ids_by_label)})
    rd = lambda name: relabel(pd.read_csv(sp[name], sep="\t", dtype=str, keep_default_na=False), ids) \
        if name in sp else None  # noqa: E731
    spy = {run: rd(f"sp_qc_{o}{sfx}_sp_year.tsv") for run, sfx in (("raw", ""), ("uln", "_uln"), ("lln", "_lln"))}
    spy = {k: v for k, v in spy.items() if v is not None}
    if "raw" not in spy or not (d / "lab_order.tsv").exists():
        return ""
    order = read_tsv(d / "lab_order.tsv")
    lv = read_tsv(d / "lab_values.tsv") if (d / "lab_values.tsv").exists() else pd.DataFrame()
    lim = read_tsv(d / "lab_limits.tsv") if (d / "lab_limits.tsv").exists() else None
    data = heatmap_data(spy, order, paged, ids_by_label)
    opts = "".join(f'<option value="{m["key"]}">{E(m["title"])}</option>' for m in data["metrics"])
    boxes = ""
    if len(lv):
        for grp in ("largest", "deviating"):
            labels = list(order.loc[order["group"] == grp, "label"])
            for run in ("raw", "uln", "lln"):
                svg = svg_boxes(lv, labels, run, h_unit)
                if svg:
                    boxes += (f'<div data-boxes="hm{o}" data-key="{grp}|{run}"'
                              f'{"" if (grp, run) == ("largest", "raw") else " hidden"}>{svg}</div>')
    js = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")
    n_dev = int((order["group"] == "deviating").sum())
    dev_opt = f'<option value="deviating">the {n_dev} most deviating others</option>' if n_dev else ""
    n_top = int((order["group"] == "largest").sum())
    parts = [f'''<div class="chart" id="hm{o}" data-heatmap="hmd{o}"><h3>Service Providers by year</h3>
  <div class="controls" style="margin:6px 0"><label class="pick">Service Providers <select name="order"><option value="largest">the {n_top} with the most results</option>{dev_opt}</select></label>
  <label class="pick">Measure <select name="metric">{opts}</select></label></div>
  {how('<span class="hm-how"></span> Grey hatching: that year covers too few people to show; empty: no results. Click a name to open its page (the largest service providers).')}
  <div class="hm"></div><div class="lg hm-lg"></div>
  <script type="application/json" id="hmd{o}">{js}</script></div>''']
    if boxes:
        parts.append(f'<div class="chart"><h3>Values per Service Provider</h3>'
                     f'{how("Each service provider&#39;s results relative to the national median (log scale; the measure above picks raw, "
                            "× upper or × lower limit): box from the 25th to the 75th percentile, whiskers 5th to 95th, "
                            "the dark line is the median. Not adjusted for sex and age, so a service provider with older patients sits "
                            "higher on age-dependent tests. Quantiles stay where at least 5 people lie beyond them.")}'
                     f'{boxes}</div>')
    rg = rd(f"sp_qc_{o}_ranges.tsv")
    if rg is not None and len(rg):
        rg = rg[rg["age"] == "adult"].copy()
        rg["dev"] = [abs(parse_x(x)) if parse_x(x) is not None else 0 for x in rg["x_vs_national"]]
        rg = rg.sort_values("dev", ascending=False)
        yrs, by, natl = limit_series(lim)
        parts.append(limits_grid(lim, rg, paged, ids_by_label, h_unit, o))
        trs = "".join(
            f'<tr data-text="{E((r.sp + " " + r.limit + " " + r.sex).lower())}"><td>{lab_link(ids_by_label.get(r.sp, ""), r.sp, paged, "../")}</td>'
            f'<td>{E(r.limit)}</td><td>{E(r.sex)}</td>{count_td(pd.to_numeric(r.rows, errors="coerce"))}'
            f'<td class="num">{E(r.sp_typical)}</td><td class="num">{E(r.sp_share_at_typical)}%</td>'
            f'<td class="num">{E(r.national_typical)}</td><td class="num" data-sort="{r.dev}">'
            f'{"<span class=\"chip warn\">" + E(r.x_vs_national) + "</span>" if r.dev >= np.log10(1.1) else E(r.x_vs_national)}</td>'
            f'<td class="wrap">{E("; ".join(limit_changes(by.get((r.sp, r.limit, r.sex), {})))) or "<span class=muted>unchanged</span>"}</td></tr>'
            for r in rg.itertuples())
        parts.append(f'<div><h3 class="sub">Reference ranges by Service Provider (adults)</h3>'
                     f'{how("Each service provider&#39;s typical adult limit per sex against the national typical one (harmonized unit), "
                            "most different first; amber at 10% or more off. Share: how many of its rows carry that limit (low: it changed "
                            "limits, see the charts above); Changes says when the limit moved.")}'
                     f'<div class="controls"><input class="search" type="search" placeholder="Filter service providers…" '
                     f'aria-label="Filter reference ranges" data-search="rng{o}"><span class="count" data-count="rng{o}"></span></div>'
                     f'<div class="tablebox"><table class="sortable" id="rng{o}" data-limit="20"><thead><tr><th>Service Provider</th>'
                     f'<th>Limit</th><th>Sex</th><th class="num">Rows</th><th class="num">Its typical</th>'
                     f'<th class="num">Share at typical</th><th class="num">National</th><th class="num">vs national</th>'
                     f'<th>Changes</th>'
                     f'</tr></thead><tbody>{trs}</tbody></table></div></div>')
    fl = []
    for run, sfx in (("raw values", ""), ("× upper limit", "_uln"), ("× lower limit", "_lln")):
        f = rd(f"sp_qc_{o}{sfx}_flags.tsv")
        if f is not None and len(f):
            fl.append(f.assign(values=run))
    if fl:
        f = pd.concat(fl, ignore_index=True)
        f["sc"] = pd.to_numeric(f["score"], errors="coerce").fillna(0)
        f = f.sort_values("sc", ascending=False)
        trs = "".join(
            f'<tr data-text="{E((r.kind + " " + r.sp + " " + r.detail + " " + r.cause).lower())}">'
            f'<td><span class="chip" style="background:{S.KIND_COLOR.get(r.kind, "#ccc")}33">{E(r.kind.replace("_", " "))}</span></td>'
            f'<td>{lab_link(ids_by_label.get(r.sp, ""), r.sp, paged, "../")}</td><td class="num">{E(r.period)}</td>'
            f'<td>{E(r.values)}</td>{count_td(pd.to_numeric(r.rows, errors="coerce"))}<td class="num" data-sort="{r.sc}">{E(r.effect_x)}</td>'
            f'<td class="wrap">{E(sp_words(r.detail))}</td><td class="wrap muted">{E(sp_words(r.cause))}</td></tr>'
            for r in f.itertuples())
        kinds = "".join(f"<li><b>{E(k.replace('_', ' '))}</b>: {E(sp_words(t))}</li>" for k, t in S.KIND_TEXT.items())
        parts.append(f'<div><h3 class="sub">Flags</h3>'
                     f'{how("Each flag is a service provider and a period that deviates from the national picture, most important "
                            "first, with what is seen in plain words and the likely cause (the test, unit, decimals, QC "
                            "note, range source or time of day whose share changed most then).")}'
                     f'<details class="kinds"><summary>The kinds of flag</summary><ul>{kinds}</ul></details>'
                     f'<div class="controls"><input class="search" type="search" placeholder="Filter flags: a service provider, a kind, a word…" '
                     f'aria-label="Filter flags" data-search="fl{o}"><span class="count" data-count="fl{o}"></span></div>'
                     f'<div class="tablebox"><table class="sortable" id="fl{o}" data-limit="25"><thead><tr><th>Kind</th>'
                     f'<th>Service Provider</th><th class="num">Period</th><th>Values</th><th class="num">Rows</th><th class="num">Effect</th>'
                     f'<th>What we see</th><th>Likely cause</th></tr></thead><tbody>{trs}</tbody></table></div></div>')
    return (f'<section id="labs-over-time"><h2>Service Providers over time <small>sp_qc: which service providers, and when, deviate from the '
            f'national picture</small></h2><div class="stackv">{"".join(parts)}</div></section>')


def read_tsv(p):
    return pd.read_csv(p, sep="\t", keep_default_na=False, na_values=[""])


def scan_site(root):
    """The last check: no count of 1..MIN_N-1 in any TSV (count columns and 'k of n' texts) or HTML data-n."""
    bad = []
    for p in root.rglob("*.tsv"):
        d = pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False)
        bad += [f"{p.relative_to(root)}: {b}" for b in S.small_counts(d, COUNTS)]
    pat = re.compile(r'data-n="(\d+)"')
    for p in root.rglob("*.html"):
        bad += [f"{p.relative_to(root)}: data-n={n}" for n in pat.findall(p.read_text()) if 1 <= int(n) < MIN_N]
    return bad


def stage_site(args, omops):
    tdirs = sorted((d for d in args.tables.glob("omop_*") if (d / "summary.tsv").exists()),
                   key=lambda d: int(d.name.split("_")[1]))
    if omops is not None:
        tdirs = [d for d in tdirs if d.name.split("_")[1] in set(omops)]
    elif args.omop:
        want = {x for o in args.omop for x in o.split(",") if x.strip()}
        tdirs = [d for d in tdirs if d.name.split("_")[1] in want]
    out, part = args.out, args.out.with_name(args.out.name + ".part")
    shutil.rmtree(part, ignore_errors=True)
    (part / "assets").mkdir(parents=True)
    (part / "omop").mkdir()
    (part / "assets" / "portal.css").write_text(CSS)
    (part / "assets" / "portal.js").write_text(JS)
    # the maakunta map, once for every page (the pages name a region; the page script draws it)
    fmap = json.loads(args.map.read_text()) if args.map and args.map.exists() else {}
    (part / "assets" / "finland.js").write_text("window.FINLAND_MAP = " + json.dumps(fmap, ensure_ascii=False) + ";\n")
    if not fmap:
        log(f"Site: no maakunta map at {args.map} (scripts/sote/data/fetch_maakunta_map.py makes it): regions without a map")
    sums, release, built = [], "", time.strftime("%Y-%m-%d")
    ldir = args.tables / "labs"
    labs = read_tsv(ldir / "labs.tsv") if (ldir / "labs.tsv").exists() else pd.DataFrame()
    lab_orgs = read_tsv(ldir / "lab_orgs.tsv") if (ldir / "lab_orgs.tsv").exists() else pd.DataFrame()
    lab_years = read_tsv(ldir / "lab_years.tsv") if (ldir / "lab_years.tsv").exists() else pd.DataFrame()
    for t in (labs, lab_orgs, lab_years):
        if "lab_id" in t.columns:
            t["lab_id"] = t["lab_id"].astype(str)
    if len(labs):
        labs["named_after"] = labs["named_after"].fillna("").astype(str)
        labs["label"] = labs["label"].astype(str)
    lab_conc = {}  # lab_id -> [(omop, name, % of the concept, ratio, rows)]
    regions = read_tsv(ldir / "regions.tsv") if (ldir / "regions.tsv").exists() else None
    ids_priv = pd.read_parquet(ldir / "ids.parquet") if (ldir / "ids.parquet").exists() else None  # private: joins only
    if ids_priv is not None:
        ids_priv["lab_id"] = ids_priv["lab_id"].astype(str)
    ids_by_label = dict(zip(labs["label"], labs["lab_id"])) if len(labs) else {}
    lab_region = ({lid: r for lid, r in zip(labs["lab_id"], labs["region"]) if isinstance(r, str) and r}
                  if len(labs) and "region" in labs.columns else {})
    # pages (and charts) for the --top-labs largest labs only; every lab stays in the searchable table
    n_pages = len(labs) if args.top_labs <= 0 else args.top_labs  # 0: every service provider gets a page
    paged = frozenset(labs.sort_values("pct_results", ascending=False).head(n_pages)["lab_id"]) if len(labs) \
        else frozenset()
    log(f"Site: {len(tdirs)} concept page(s) ...")
    bar = Progress("Site: concept pages", len(tdirs))
    for ci, d in enumerate(tdirs, 1):
        bar.update(ci, d.name.replace("omop_", "OMOP "))
        meta = json.loads((d / "meta.json").read_text())
        s = read_tsv(d / "summary.tsv").iloc[0].to_dict()
        s["omop"] = str(int(s["omop"]))
        for k in ("name", "quantity", "h_unit", "sp_flag_kinds"):
            s[k] = "" if not isinstance(s[k], str) else s[k]
        if int(s["rows"]) < args.min_rows:
            continue
        o = s["omop"]
        comp, years = (read_tsv(d / f"{n}.tsv") for n in ("composition", "years"))
        comp["test"] = comp["test"].astype(str)
        comp["unit"] = comp["unit"].fillna("").astype(str)
        s["_tests"] = list(comp.loc[comp["people"].notna(), "test"])
        files = [(k, Path(p)) for k, p in meta["files"] if Path(p).exists()]
        fdir = part / "omop" / o
        fdir.mkdir()
        for k, p in files:  # the reports: TSVs checked again on the way, figures copied (or linked)
            if p.suffix == ".tsv":
                S.publish_tsv(pd.read_csv(p, sep="\t", dtype=str, keep_default_na=False), fdir / p.name,
                              count_cols=COUNTS)
            elif args.symlink:
                os.symlink(p.resolve(), fdir / p.name)
            else:
                shutil.copy2(p, fdir / p.name)
        clabs = read_tsv(d / "labs.tsv") if (d / "labs.tsv").exists() else pd.DataFrame()
        if len(clabs) and "rows" not in clabs.columns and ids_priv is not None:  # tables from before the rows column
            clabs = concept_labs(o, files, ids_priv)
        if len(clabs):
            clabs["lab_id"], clabs["label"] = clabs["lab_id"].astype(str), clabs["label"].astype(str)
            for r in clabs.itertuples():
                lab_conc.setdefault(r.lab_id, []).append((o, s["name"], float(r.pct_concept), float(r.ratio),
                                                          getattr(r, "rows", None)))
        for n, pub in (("composition", "composition"), ("years", "years"), ("labs", "service_providers")):
            if (d / f"{n}.tsv").exists():
                shutil.copy2(d / f"{n}.tsv", fdir / f"{pub}.tsv")
        verdicts = read_tsv(fdir / f"omop_{o}_ref_audit_verdicts.tsv") if (fdir / f"omop_{o}_ref_audit_verdicts.tsv").exists() else None
        release = meta.get("release", release)
        tp = fdir / f"sp_qc_{o}_timing.tsv"
        tim = pd.read_csv(tp, sep="\t", dtype=str, keep_default_na=False) if tp.exists() else None
        body = concept_page(s, comp, years, files, verdicts, clabs, paged, tim, d, ids_by_label, lab_region)
        (part / "omop" / f"{o}.html").write_text(page(f"{s['name'] or 'OMOP ' + o} · Kanta lab QC", body, 1, release, built))
        sums.append(s)
    bar.close()
    sums.sort(key=lambda s: -int(s["rows"]))
    if len(labs):
        (part / "lab").mkdir()
        for n, pub in (("labs", "service_providers"), ("lab_orgs", "sp_organisations"), ("lab_years", "sp_years")):
            shutil.copy2(ldir / f"{n}.tsv", part / "lab" / f"{pub}.tsv")
        lo_by, ly_by = dict(tuple(lab_orgs.groupby("lab_id"))), dict(tuple(lab_years.groupby("lab_id")))
        empty_o, empty_y = lab_orgs.iloc[:0], lab_years.iloc[:0]
        log(f"Site: {len(paged)} service provider page(s) (of {len(labs)} service providers) ...")
        lbar = Progress("Site: service provider pages", len(paged))
        for i, r in enumerate(labs[labs["lab_id"].isin(paged)].itertuples(), 1):
            if i == 1 or i % 25 == 0:  # redrawing for every one of thousands of small pages costs more than the page
                lbar.update(i, r.label)
            body = lab_page(r, lo_by.get(r.lab_id, empty_o), ly_by.get(r.lab_id, empty_y), lab_conc.get(r.lab_id, []))
            (part / "lab" / f"{r.lab_id}.html").write_text(page(f"{r.label} · Kanta lab QC", body, 1, release, built))
        lbar.close()
    if len(labs) and "region" in labs.columns:
        # one page per region: its service providers, as a pie and a table (shares of the region's results only)
        rstats = {}
        if regions is not None and len(regions):
            g = regions.copy()
            g["results_pct"], g["population"] = pd.to_numeric(g["results_pct"], errors="coerce"), pd.to_numeric(g["population"], errors="coerce")
            g = g[(g["results_pct"] > 0) & (g["population"] > 0)]
            if len(g):
                g["sh_tests"] = 100 * g["results_pct"] / g["results_pct"].sum()
                g["sh_pop"] = 100 * g["population"] / g["population"].sum()
                g["ratio"] = g["sh_tests"] / g["sh_pop"]
                rstats = {r["region"]: r for _, r in g.iterrows()}
        reg_labs = labs[labs["region"].astype(str).str.len() > 0]
        if len(reg_labs):
            (part / "region").mkdir()
            for name, rl in reg_labs.groupby("region"):
                body = region_page(name, rl, rstats.get(name), paged)
                (part / "region" / f"{region_slug(name)}.html").write_text(page(f"{name} · Kanta lab QC", body, 1, release, built))
            log(f"Site: {reg_labs['region'].nunique()} region page(s)")
    (part / "index.html").write_text(page("Kanta Lab QC Portal", landing(sums, labs, lab_conc, paged, regions), 0, release, built))
    S.publish_tsv(pd.DataFrame([{k: v for k, v in s.items() if not k.startswith("_")} for s in sums]),
                  part / "omops.tsv", count_cols=COUNTS)
    log("Site: checking every TSV and count in the HTML ...")
    bad = scan_site(part)
    if bad:
        raise SystemExit(f"Site not published: {len(bad)} count(s) under {MIN_N}: {'; '.join(bad[:5])} (left in {part})")
    if out.exists():  # swap in: the old site out, the new one in
        old = out.with_name(out.name + ".old")
        shutil.rmtree(old, ignore_errors=True)
        os.replace(out, old)
        os.replace(part, out)
        shutil.rmtree(old)
    else:
        os.replace(part, out)
    log(f"Site: {len(sums)} concept(s), {len(labs)} lab(s) -> {out}/index.html")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(description=" ".join(__doc__.strip().splitlines()[1:3]),
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--stage", choices=["tables", "site", "all"], default="all", help="Build the tables, the site or both")
    p.add_argument("--omop", nargs="+", default=[], help="Only these OMOP ids (space- or comma-separated)")
    p.add_argument("--min-rows", type=int, default=0, help="Concepts with at least this many rows")
    p.add_argument("--input", type=Path, default=S.latest_engine_output(),
                   help="Engine output the row cache was built from (names the cache folder; now %(default)s)")
    p.add_argument("--cache", type=Path, default=S.CACHE, help="sp_qc.py's cache (the rows of each OMOP)")
    p.add_argument("--sp-dir", type=Path, default=S.OUT, help="sp_qc.py's reports")
    p.add_argument("--audit-dir", type=Path, default=Path("/mnt/disks/data/kanta/qc/omop"),
                   help="omop_ref_audit.py's reports")
    p.add_argument("--tables", type=Path, default=TABLES, help="The gated tables, per concept")
    p.add_argument("--out", type=Path, default=SITE, help="The site folder")
    p.add_argument("--sp-names", type=Path, default=None,
                   help="kanta_sp_names.tsv from scripts/sote/region/sp_names.py for the SP names (as given to sp_qc.py); "
                        "default: counted over --input")
    p.add_argument("--symlink", action="store_true", help="Link the PDF reports instead of copying them")
    p.add_argument("--population", type=Path,
                   default=Path(__file__).resolve().parents[1] / "sote" / "data" / "maakunta_population.tsv",
                   help="Population by maakunta (scripts/sote/data/fetch_maakunta_population.py), for the coverage")
    p.add_argument("--map", type=Path, default=Path(__file__).resolve().parents[1] / "sote" / "data" / "maakunta_map.json",
                   help="Simplified maakunta boundaries for the region maps (scripts/sote/data/fetch_maakunta_map.py)")
    p.add_argument("--top-labs", type=int, default=20,
                   help="Service providers (largest by share of results) that get a page; 0: all. All are in the table")
    p.add_argument("--force", action="store_true", help="Rebuild every concept's tables")
    p.add_argument("--names", type=Path, default=A.DATA_DIR / "LABfi.tsv", help="Usagi mapping (concept names)")
    p.add_argument("--harmonization-counts", type=Path, default=A.DATA_DIR / "harmonization_counts.tsv",
                   help="OMOP_ID -> quantity and harmonized unit")
    p.add_argument("--memory-limit", default=f"{int(S.RAM_GB * 0.45)}GB", help="duckdb memory limit (spills beyond it)")
    p.add_argument("--threads", type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads")
    return p


def main():
    args = build_parser().parse_args()
    if args.input is None and args.stage != "site":
        raise SystemExit(f"No engine output found in {S.ENGINE_DIR}: pass --input <engine_output>.parquet")
    omops = None
    if args.stage in ("tables", "all"):
        omops = stage_tables(args)
    if args.stage in ("site", "all"):
        stage_site(args, omops)


if __name__ == "__main__":
    main()
