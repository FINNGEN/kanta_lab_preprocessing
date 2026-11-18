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
  site    from those tables only, into --out (built in <out>.part, then swapped in):
            index.html       one row per concept: search (names, OMOP ids, test names), sort, filters
            omop/<id>.html   the summary, the audit and lab-report cards, results per year, the composition table,
                             the figures (the audit's and sp_qc's pages), the concept's labs; omop/<id>/ holds the
                             copied reports and the concept's TSVs
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
S.MIN_N = MIN_N
TABLES = Path("/mnt/disks/data/kanta/qc/portal_tables")
SITE = Path("/mnt/disks/data/kanta/qc/portal")
VERSION = "p4"
CELL = ["test", "unit", "h_unit"]
ABNORMAL = ("H", "L", "HH", "LL", "A", "AA")
COUNTS = ("rows", "people", "n")
LABS_VERSION = "l3"
NONE_ORG, OTHER_ORG = "no organisation", "other organisations"
SP_PAGES = [("labs_top", "Labs over time: the largest labs", "rows, median vs national per year and values, per lab"),
            ("labs_deviating", "Labs over time: the most deviating labs", "the other labs furthest from national"),
            ("ranges", "Reference ranges by lab", "adult limits per lab and year against the national range"),
            ("timing", "When the samples are taken", "share of each lab's results per hour and weekday")]
t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


def npeople(sketches):
    return len(S.pset(sketches))


# ---------------------------------------------------------------------------
# Which concepts: whatever sp_qc.py / omop_ref_audit.py were run for
# ---------------------------------------------------------------------------

def report_files(omop, args):
    """The concept's report files: (kind, path) for the audit and sp_qc outputs (png / pdf / tsv)."""
    sp = [p for p in sorted(args.sp_dir.glob(f"sp_qc_{omop}_*")) + sorted(args.sp_dir.glob(f"sp_qc_{omop}.pdf"))
          if p.suffix in (".pdf", ".tsv", ".png")] if args.sp_dir.is_dir() else []
    au = [p for p in sorted(args.audit_dir.glob(f"omop_{omop}_ref_audit*"))
          if p.suffix in (".pdf", ".png", ".tsv")] if args.audit_dir.is_dir() else []
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
    return {"summary": summary, "composition": comp, "years": years,
            "labs": concept_labs(omop, files, ids)}


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
    srcs = [root / "sp_org_year.parquet", root / "sp_people.parquet", root / "sp_orgs.parquet"]
    want = {"version": LABS_VERSION, "src": {str(p): p.stat().st_mtime for p in srcs if p.exists()}}
    meta_p = ldir / "meta.json"
    if meta_p.exists() and (ldir / "ids.parquet").exists() and not args.force and \
            json.loads(meta_p.read_text()).get("want") == want:
        log(f"Labs: cached ({ldir})")
        return pd.read_parquet(ldir / "ids.parquet")
    labels = S.Labels(S.ORG_MAP)
    if (root / "sp_orgs.parquet").exists():
        labels.add_orgs(pd.read_parquet(root / "sp_orgs.parquet"))
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
                     "named_after": named or "", "named_pct": named_pct, "no_org_pct": no_org})
        ids.append({"k_sp": sp, "lab_id": lid, "label": label, "n": n})
    ldir.mkdir(parents=True, exist_ok=True)
    S.publish_tsv(pd.DataFrame(labs), ldir / "labs.tsv", count_cols=COUNTS)
    S.publish_tsv(pd.DataFrame(orgs, columns=["lab_id", "org", "pct"]), ldir / "lab_orgs.tsv", count_cols=COUNTS)
    S.publish_tsv(pd.DataFrame(years, columns=["lab_id", "year_from", "year_to", "org", "share", "pct_of_lab"]),
                  ldir / "lab_years.tsv", count_cols=COUNTS)
    ids = pd.DataFrame(ids, columns=["k_sp", "lab_id", "label", "n"])
    ids.to_parquet(ldir / "ids.parquet", index=False)
    meta_p.write_text(json.dumps({"want": want}, indent=1) + "\n")
    log(f"Labs: {len(labs)} lab(s) of >= {MIN_N} people -> {ldir}")
    return ids


def concept_labs(omop, files, ids):
    """The concept's labs from sp_qc's published lab-years (raw run): per lab the sum of its shown years, as % of the
    shown labs' rows, against the same labs' shares of all results (re-based to them) and the ratio. No counts."""
    spy = next((p for k, p in files if k == "sp" and p.name == f"sp_qc_{omop}_sp_year.tsv"), None)
    cols = ["lab_id", "label", "pct_concept", "pct_dataset", "ratio", "year_first", "year_last"]
    if spy is None or ids is None or ids.empty:
        return pd.DataFrame(columns=cols)
    d = pd.read_csv(spy, sep="\t", dtype=str, keep_default_na=False)
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
    g = g.astype({"year_first": "Int64", "year_last": "Int64"})
    return g.sort_values("pct_concept", ascending=False)[cols]


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
        for p in tdir.glob("*.tsv"):
            p.unlink()
        if t is None:
            why = f"under {args.min_rows} rows or fewer than {MIN_N} people"
            meta_p.write_text(json.dumps({"want": want, "dropped": why}, indent=1) + "\n")
            dropped[omop] = why
            continue
        for name, df in t.items():
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
  --other: #a8a69c; --none: #d9d7cf;
  --warn-bg: #fdf1d6; --warn-ink: #7a5300; --crit-bg: #fbe1df; --crit-ink: #9c2420; --ok-bg: #e3f3e8; --ok-ink: #1d6b39;
  --font: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --mono: ui-monospace, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #141413; --surface: #1c1c1a; --ink: #edece7; --muted: #a9a8a0; --line: #36352f; --soft: #252522;
  --accent: #6ea6ec; --accent-ink: #a9cbf5; --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
  --other: #77766e; --none: #45443f;
  --warn-bg: #3a2f12; --warn-ink: #f0c869; --crit-bg: #42201d; --crit-ink: #f3a19b; --ok-bg: #18301f; --ok-ink: #8fd3a5;
  color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #141413; --surface: #1c1c1a; --ink: #edece7; --muted: #a9a8a0; --line: #36352f; --soft: #252522;
  --accent: #6ea6ec; --accent-ink: #a9cbf5; --s1: #3987e5; --s2: #d95926; --s3: #199e70; --s4: #c98500;
  --other: #77766e; --none: #45443f;
  --warn-bg: #3a2f12; --warn-ink: #f0c869; --crit-bg: #42201d; --crit-ink: #f3a19b; --ok-bg: #18301f; --ok-ink: #8fd3a5;
  color-scheme: dark; }
* { box-sizing: border-box; }
body { background: var(--bg); color: var(--ink); font: 14px/1.5 var(--font); margin: 0; }
a { color: var(--accent); text-decoration-thickness: 1px; text-underline-offset: 2px; }
a:hover { color: var(--accent-ink); }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; border-radius: 3px; }
.wrap { max-width: 1760px; margin: 0 auto; padding-left: max(16px, env(safe-area-inset-left, 0px));
  padding-right: max(16px, env(safe-area-inset-right, 0px)); }
.mono { font-family: var(--mono); font-size: 0.93em; }
.num { text-align: right; font-variant-numeric: tabular-nums; white-space: nowrap; }
.muted { color: var(--muted); }
.top { border-bottom: 1px solid var(--line); background: var(--surface); }
.top .wrap { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 20px; padding-block: 12px; }
.brand { font-weight: 650; font-size: 16px; color: var(--ink); text-decoration: none; letter-spacing: -0.01em; }
.brand span { color: var(--muted); font-weight: 450; }
.release { color: var(--muted); font-size: 13px; }
.release b { font-family: var(--mono); font-weight: 500; color: var(--ink); font-size: 12.5px; }
main { padding-block: 24px 64px; display: grid; grid-template-columns: minmax(0, 1fr); gap: 28px; }
h1 { font-size: 24px; line-height: 1.25; margin: 0; letter-spacing: -0.015em; text-wrap: balance; font-weight: 650; }
h2 { font-size: 16px; margin: 0 0 10px; font-weight: 650; display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap; }
h2 small { font-weight: 400; color: var(--muted); font-size: 13px; }
.lead { margin: 6px 0 0; color: var(--muted); max-width: 72ch; }
.controls { display: flex; flex-wrap: wrap; gap: 10px 18px; align-items: center; margin-bottom: 10px; }
.search { flex: 1 1 280px; max-width: 460px; padding: 8px 11px; font: inherit; color: var(--ink); background: var(--surface);
  border: 1px solid var(--line); border-radius: 6px; }
.controls label { display: flex; align-items: center; gap: 6px; font-size: 13px; color: var(--muted); cursor: pointer; }
.count { font-size: 13px; color: var(--muted); margin-left: auto; font-variant-numeric: tabular-nums; }
.tablebox { overflow-x: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 8px; }
table { border-collapse: collapse; width: 100%; font-size: 13px; }
th, td { padding: 7px 10px; border-bottom: 1px solid var(--line); vertical-align: top; }
th { text-align: left; font-weight: 600; font-size: 12px; color: var(--muted); background: var(--soft); white-space: nowrap;
  cursor: pointer; user-select: none; }
th.num { text-align: right; }
th[aria-sort="ascending"]::after { content: " ▲"; font-size: 9px; }
th[aria-sort="descending"]::after { content: " ▼"; font-size: 9px; }
tbody tr:last-child td { border-bottom: 0; }
tbody tr:hover td { background: var(--soft); }
tr.pinned td { color: var(--muted); font-style: italic; background: var(--soft); }
td.name { min-width: 260px; }
td.name a { font-weight: 550; }
td .sub { display: block; color: var(--muted); font-size: 12px; }
.below { font-size: 11px; color: var(--muted); letter-spacing: 0.02em; white-space: nowrap; }
.chip { display: inline-block; padding: 1px 7px; border-radius: 10px; font-size: 12px; font-variant-numeric: tabular-nums;
  white-space: nowrap; background: var(--soft); color: var(--muted); }
.chip.ok { background: var(--ok-bg); color: var(--ok-ink); }
.chip.warn { background: var(--warn-bg); color: var(--warn-ink); }
.chip.crit { background: var(--crit-bg); color: var(--crit-ink); }
.pill { display: inline-block; padding: 2px 8px; border: 1px solid var(--line); border-radius: 5px; font-size: 12px;
  text-decoration: none; margin-right: 4px; white-space: nowrap; background: var(--surface); }
.pill.off { color: var(--muted); border-style: dashed; }
.crumb { font-size: 13px; }
.idline { display: flex; flex-wrap: wrap; gap: 6px 14px; margin-top: 8px; color: var(--muted); font-size: 13px; align-items: center; }
.idline .mono { color: var(--ink); }
.tiles { display: grid; grid-template-columns: repeat(auto-fill, minmax(150px, 1fr)); gap: 10px; }
.tile { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; min-width: 0; }
.tile .k { font-size: 12px; color: var(--muted); }
.tile .v { font-size: 20px; font-weight: 600; font-variant-numeric: tabular-nums; letter-spacing: -0.01em; }
.tile .d { font-size: 12px; color: var(--muted); }
.cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(340px, 100%), 1fr)); gap: 14px; align-items: start; }
.card { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 14px 16px; display: grid;
  gap: 10px; align-content: start; min-width: 0; }
.card h3 { margin: 0; font-size: 14px; font-weight: 650; display: flex; gap: 8px; align-items: baseline; flex-wrap: wrap; }
.card p { margin: 0; color: var(--muted); font-size: 13px; }
.files { display: flex; flex-wrap: wrap; gap: 6px 10px; }
.files a { font-family: var(--mono); font-size: 12px; }
.fig { display: grid; gap: 6px; margin: 0 0 22px; }
.fig figcaption { font-size: 13px; color: var(--muted); }
.fig figcaption b { color: var(--ink); font-weight: 600; }
.fig img { width: 100%; height: auto; border: 1px solid var(--line); border-radius: 6px; background: #fcfcfb; display: block; }
.stack { display: flex; height: 12px; border-radius: 3px; overflow: hidden; gap: 2px; background: var(--surface); }
.stack span { display: block; height: 100%; }
.keyrow { display: flex; flex-wrap: wrap; gap: 4px 12px; font-size: 12px; color: var(--muted); }
.keyrow i { display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 4px; vertical-align: -1px; }
.flaglist { display: flex; flex-wrap: wrap; gap: 6px; }
.charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(min(340px, 100%), 1fr)); gap: 14px; }
td.links { white-space: nowrap; }
.chart { background: var(--surface); border: 1px solid var(--line); border-radius: 8px; padding: 12px 14px; min-width: 0; }
.chart h3 { margin: 0; font-size: 13.5px; font-weight: 650; }
.chart .q { margin: 2px 0 6px; font-size: 12.5px; color: var(--muted); }
.chart svg { width: 100%; height: auto; display: block; }
.chart svg text { fill: var(--muted); font: 11px var(--font); }
.chart .lg { display: flex; flex-wrap: wrap; gap: 4px 14px; font-size: 12px; color: var(--muted); margin-top: 4px; }
.chart .lg i { display: inline-block; width: 14px; height: 3px; border-radius: 2px; margin-right: 5px; vertical-align: 3px; }
.chart .lg { align-items: center; }
.chart .lg i.dotkey { display: inline-block; border-radius: 50%; background: var(--muted); opacity: 0.6;
  vertical-align: middle; margin-right: 5px; }
.chart .lg i.hatchkey { height: 9px; width: 12px; vertical-align: -1px;
  background: repeating-linear-gradient(45deg, var(--accent) 0 2px, transparent 2px 4px); }
code { font-family: var(--mono); font-size: 12px; background: var(--soft); padding: 1px 5px; border-radius: 4px; }
.charts.wide { grid-template-columns: minmax(0, 1fr); }
.charts.timing { grid-template-columns: minmax(0, 3fr) minmax(0, 1.2fr); margin-top: 10px; }
@media (max-width: 900px) { .charts.timing { grid-template-columns: minmax(0, 1fr); } }
.timing-head { display: flex; flex-wrap: wrap; gap: 8px 18px; align-items: baseline; margin-top: 18px; }
h3.sub { margin: 0; font-size: 14px; font-weight: 650; }
.pick { font-size: 13px; color: var(--muted); display: flex; gap: 6px; align-items: center; }
.pick select { font: inherit; color: var(--ink); background: var(--surface); border: 1px solid var(--line);
  border-radius: 5px; padding: 3px 6px; max-width: 320px; }
.tabs { display: flex; gap: 4px; border-bottom: 1px solid var(--line); margin-bottom: 14px; }
.tabs button { font: inherit; font-weight: 600; font-size: 14px; color: var(--muted); background: none; border: 0;
  border-bottom: 2px solid transparent; padding: 8px 12px; margin-bottom: -1px; cursor: pointer; }
.tabs button[aria-selected="true"] { color: var(--ink); border-bottom-color: var(--accent); }
.ratio-hi { color: var(--crit-ink); font-weight: 600; }
footer { font-size: 12.5px; color: var(--muted); }
dl.defs { display: grid; grid-template-columns: max-content 1fr; gap: 4px 14px; margin: 8px 0 0; }
dl.defs dt { font-weight: 600; color: var(--ink); }
dl.defs dd { margin: 0; }
@media (max-width: 560px) { dl.defs { grid-template-columns: 1fr; } dl.defs dd { margin-bottom: 6px; } }
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
    // data-limit: without a search, only the first rows (in the current order); a search looks through all of them
    var limit = parseInt(t.getAttribute("data-limit") || "0", 10);
    function run() {
      var terms = (q ? q.value : "").toLowerCase().split(/\\s+/).filter(Boolean), n = 0, all = 0, shown = 0;
      Array.prototype.forEach.call(tb.rows, function (tr) {
        if (tr.classList.contains("pinned")) return; all++;
        var txt = tr.getAttribute("data-text") || tr.textContent.toLowerCase();
        var ok = terms.every(function (w) { return txt.indexOf(w) >= 0; }) &&
          checks.every(function (c) { return !c.checked || tr.getAttribute("data-" + c.getAttribute("data-require")) === "1"; });
        if (ok) n++;
        var vis = ok && (terms.length > 0 || !limit || shown < limit);
        tr.hidden = !vis; if (vis) shown++;
      });
      if (count) count.textContent = shown === all ? all + " shown" :
        shown + " of " + all + " shown" + (shown < n ? " · search to find the others" : "");
    }
    if (q) q.addEventListener("input", run);
    checks.forEach(function (c) { c.addEventListener("change", run); });
    run();
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
  // tabs (landing page): one panel at a time; #labs opens the labs
  var tabs = Array.prototype.slice.call(document.querySelectorAll("[data-tab]"));
  function show(name) {
    tabs.forEach(function (b) {
      var on = b.getAttribute("data-tab") === name;
      b.setAttribute("aria-selected", on ? "true" : "false");
      document.getElementById("panel-" + b.getAttribute("data-tab")).hidden = !on;
    });
    var cols = document.getElementById("concept-columns");
    if (cols) cols.hidden = name !== "concepts";
  }
  if (tabs.length) {
    tabs.forEach(function (b) { b.addEventListener("click", function () { location.hash = b.getAttribute("data-tab"); }); });
    var fromHash = function () { show(location.hash === "#labs" ? "labs" : "concepts"); };
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
<script src="{up}assets/portal.js"></script>
</body>
</html>
"""


def landing(sums, labs=None, lab_conc=None, paged=frozenset()):
    cols = [("OMOP ID", ""), ("Concept", ""), ("Unit", ""), ("People", "num"), ("Rows", "num"),
            ("Records / person", "num"), ("Female", "num"), ("Test names", "num"), ("Units", "num"), ("Labs", "num"),
            ("Years", "num"), ("Audit: rows flagged", "num"), ("Lab flags", "num"), ("Reports", "")]
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
            f'<a class="pill{"" if sp else " off"}" href="omop/{o}.html#labs">labs</a></td></tr>')
    has_labs = labs is not None and len(labs) > 0
    labs_tab = (f'<button type="button" role="tab" data-tab="labs" aria-selected="false">Labs ({len(labs)})</button>'
                if has_labs else "")
    labs_html = labs_panel(labs, lab_conc or {}, paged) if has_labs else ""
    na = sum(1 for s in sums if str(s["audit"]) == "True")
    ns = sum(1 for s in sums if str(s["sp"]) == "True")
    return f"""
<section>
  <h1>OMOP lab concepts</h1>
  <p class="lead">One row per concept that was run through the unit and reference-range audit or the lab report. Search by
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
    <label><input type="checkbox" id="onlySp" data-require="sp" data-table="omops"> with lab report</label>
    <span class="count" data-count="omops"></span>
  </div>
  <div class="tablebox"><table class="sortable" id="omops"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>
  <p class="muted" style="font-size:12.5px;margin:8px 2px 0">{len(sums)} concepts · {na} audited · {ns} with a lab report · the table as <a href="omops.tsv">omops.tsv</a></p>
</section>
{labs_html}
</div>
<footer id="concept-columns">
  <h2>Columns</h2>
  <dl class="defs">
    <dt>People, rows</dt><dd>distinct people and results with a value for the concept in this release</dd>
    <dt>Records / person</dt><dd>median number of results per person</dd>
    <dt>Test names, units, labs</dt><dd>how many test names, units and labs (service providers) each cover at least {MIN_N} people</dd>
    <dt>Years</dt><dd>first and last year with results</dd>
    <dt>Audit: rows flagged</dt><dd>share of rows whose unit, value or reference range disagree (omop_ref_audit); green under 1%, amber 1–5%, red 5% and more</dd>
    <dt>Lab flags</dt><dd>lab- and time-specific flags from sp_qc (steps, shifts, outliers, range changes, …), all runs; hover for the kinds</dd>
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


def timing_svg(tim, labs):
    """When the samples are taken, from sp_qc's published _timing.tsv, hours and weekdays side by side in one drawing
    (so the rows line up). Top: one row per lab (all labs, then the concept's largest), a dot per hour / weekday sized
    by that lab's share of its results. Bottom: for all labs, the median value per hour / weekday vs all (vertical
    position, log scale; orange higher, blue lower), dots sized by the share of the results."""
    rows = [("all labs", "all labs")] + [(lab, lab) for _, lab in labs]
    blocks = []  # (kind, categories, x0, step)
    lw, gap, rmax = 220, 56, 15.0
    x = lw
    for kind, cats, step in (("hour", HOUR_CATS, 38), ("weekday", DAY_CATS, 46)):
        t = tim[tim["kind"] == kind]
        cats = [c for c in cats if c in set(t["cat"])]
        if cats:
            blocks.append((kind, cats, x, step, t))
            x += step * len(cats) + gap
    if not blocks:
        return ""
    W = x - gap + 64  # room for the factor labels on the right
    rh, top = 30, 46
    m_top = top + rh * len(rows) + 34
    mh = 150
    y0 = m_top + mh / 2
    H = m_top + mh + 30
    name = lambda kind, c: ({"none": "no time"} if kind == "hour" else {"none": "no date"}).get(c, c)  # noqa: E731
    R = lambda pct: 1.5 + (rmax - 1.5) * np.sqrt(max(pct, 0) / 100)  # noqa: E731  (dot area ~ share)
    devs = [parse_x(v) for _, _, _, _, t in blocks for v in t.loc[t["sp"] == "all labs", "x_vs_all"]]
    dmax = max([abs(d) for d in devs if d is not None] + [np.log10(1.05)])
    Y = lambda d: y0 - d / dmax * (mh / 2 - rmax)  # noqa: E731
    g = []
    for i, (_, lab) in enumerate(rows):
        y = top + rh * i + rh / 2
        txt = lab if len(lab) <= 32 else lab[:31] + "…"
        g.append(f'<text x="{lw - 12}" y="{y + 4:.1f}" text-anchor="end"{" font-weight=\"600\"" if i == 0 else ""}>'
                 f'{E(txt)}<title>{E(lab)}</title></text>')
        g.append(f'<line x1="{lw}" x2="{W - 12}" y1="{y:.1f}" y2="{y:.1f}" stroke="var(--line)" stroke-width="0.6"/>')
    g.append(f'<text x="{lw - 12}" y="{y0 + 4:.1f}" text-anchor="end" font-weight="600">median vs all</text>'
             f'<text x="{lw - 12}" y="{y0 + 20:.1f}" text-anchor="end">(all labs)</text>')
    for kind, cats, x0, step, t in blocks:
        X = lambda i: x0 + step * i + step / 2  # noqa: E731
        g.append(f'<text x="{x0}" y="{14}" font-weight="600">{"Hour of the day" if kind == "hour" else "Weekday"}</text>')
        g.append(f'<line x1="{x0}" x2="{x0 + step * len(cats)}" y1="{y0:.1f}" y2="{y0:.1f}" stroke="var(--muted)"/>')
        for d in (dmax, -dmax):
            lab = f"×{10 ** d:.2f}" if d > 0 else f"/{10 ** -d:.2f}"
            g.append(f'<line x1="{x0}" x2="{x0 + step * len(cats)}" y1="{Y(d):.1f}" y2="{Y(d):.1f}" stroke="var(--line)" '
                     f'stroke-dasharray="3 3"/><text x="{x0 + step * len(cats) + 4}" y="{Y(d) + 4:.1f}">{lab}</text>')
        for j, c in enumerate(cats):
            show = kind != "hour" or c in ("none", "00:00") or int(c) % 3 == 0
            if show:
                fs = ' font-size="9"' if c in ("none", "00:00") else ""
                g.append(f'<text x="{X(j):.1f}" y="{top - 10}" text-anchor="middle"{fs}>{E(name(kind, c))}</text>'
                         f'<text x="{X(j):.1f}" y="{H - 8}" text-anchor="middle"{fs}>{E(name(kind, c))}</text>')
        for i, (sp, lab) in enumerate(rows):
            d = t[t["sp"] == sp].set_index("cat")
            y = top + rh * i + rh / 2
            for j, c in enumerate(cats):
                v = pd.to_numeric(d.loc[c, "pct_of_rows"], errors="coerce") if c in d.index else np.nan
                if pd.notna(v) and v > 0:
                    g.append(f'<circle cx="{X(j):.1f}" cy="{y:.1f}" r="{R(v):.1f}" fill="var(--accent)" '
                             f'fill-opacity="0.8"><title>{E(lab)}, {E(name(kind, c))}: {v:.1f}% of its results</title></circle>')
        d = t[t["sp"] == "all labs"].set_index("cat")
        for j, c in enumerate(cats):
            if c not in d.index:
                continue
            v, dev = pd.to_numeric(d.loc[c, "pct_of_rows"], errors="coerce"), parse_x(d.loc[c, "x_vs_all"])
            if dev is None or pd.isna(v):
                continue
            txt = f"×{10 ** dev:.2f}" if dev >= 0 else f"/{10 ** -dev:.2f}"
            g.append(f'<circle cx="{X(j):.1f}" cy="{Y(dev):.1f}" r="{R(v):.1f}" fill="var(--{"s2" if dev >= 0 else "s1"})" '
                     f'fill-opacity="0.85" stroke="var(--surface)" stroke-width="1.5"><title>{E(name(kind, c))}: median '
                     f'{txt} vs all, {v:.1f}% of the results (same sex and age)</title></circle>')
    key = "".join(f'<span><i class="dotkey" style="width:{2 * R(p):.0f}px;height:{2 * R(p):.0f}px"></i>{p}% of the '
                  f'results</span>' for p in (5, 20, 50))
    return (f'<div class="chart"><h3>When the samples are taken</h3><p class="q">Each lab\'s results per hour and weekday '
            f'(dot size: share of the lab\'s results), and below, the median value per hour / weekday vs all (higher: '
            f'orange, lower: blue; dot size: share of all results). From the lab report; small cells are left out.</p>'
            f'<svg viewBox="0 0 {W:.0f} {H:.0f}" role="img" aria-label="When the samples are taken">{"".join(g)}</svg>'
            f'<div class="lg">{key}</div></div>')


def timing_html(tim, clabs):
    if tim is None or tim.empty:
        return ""
    top = [] if clabs is None or clabs.empty else list(zip(clabs["lab_id"], clabs["label"]))[:10]
    top = [(lid, lab) for lid, lab in top if (tim["sp"] == lab).any()]
    return f'<div class="charts wide" style="margin-top:14px">{timing_svg(tim, top)}</div>'


def concept_page(s, comp, years, files, verdicts, clabs=None, paged=frozenset(), tim=None):
    o = s["omop"]
    tile = lambda k, v, d="": (f'<div class="tile"><div class="k">{k}</div><div class="v">{v}</div>'  # noqa: E731
                               + (f'<div class="d">{d}</div>' if d else "") + "</div>")
    yrs = f'{int(s["year_first"])}–{int(s["year_last"])}' if num(s["year_first"]) else "—"
    tiles = "".join([
        tile("People", f'<span data-n="{int(s["people"])}">{int(s["people"]):,}</span>'),
        tile("Results", f'<span data-n="{int(s["rows"])}">{int(s["rows"]):,}</span>', "with a value"),
        tile("Female", pct_s(s["female_pct"], 0)), tile("Years", yrs),
        tile("Test names", s["test_names"], f'{s["units"]} units'), tile("Labs", s["labs"], "service providers"),
        tile("Records per person", int(s["records_per_person"]) if num(s["records_per_person"]) else pct_s(None), "median"),
        tile("People with ≥ 2 records", pct_s(s["ge2_records_pct"], 0)),
        tile("Age at test", int(s["age_median"]) if num(s["age_median"]) else pct_s(None), "median, years")])
    link = lambda p: f'<a href="{o}/{E(p.name)}">{E(p.name)}</a>'  # noqa: E731
    au = [p for k, p in files if k == "audit"]
    sp = [p for k, p in files if k == "sp"]
    if au:
        png = next((p for p in au if p.suffix == ".png"), None)
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
                 f'and the unit they probably should have had (the figure is under Figures).</p>{bar}'
                 + f'<div class="files">{"".join(link(p) for p in au)}</div></div>')
    else:
        audit = (f'<div class="card" id="audit"><h3>Units and reference ranges</h3><p>The audit has not been run for this '
                 f'concept. Run <code>omop_ref_audit.py --omop {o}</code> and rebuild the portal.</p></div>')
    if sp:
        kinds = [x.rsplit(" ", 1) for x in str(s["sp_flag_kinds"] or "").split(", ") if x]
        nf = int(s["sp_flags"]) if num(s["sp_flags"]) else 0
        labs = (f'<div class="card" id="labs"><h3>Labs over time <span class="chip {"warn" if nf else "ok"}">{nf} flags'
                f'</span></h3><p>Which labs, and when, deviate from the national picture: level shifts, method changes, '
                f'outliers, reference-range changes, sampling times (the PDF; the flags in <code>_flags.tsv</code>).</p>'
                f'<div class="flaglist">' + ("".join(f'<span class="chip">{E(k.replace("_", " "))} {E(v)}</span>'
                                                   for k, v in kinds) or '<span class="muted">no flags</span>')
                + f'</div><div class="files">{"".join(link(p) for p in sp if p.suffix != ".png")}</div></div>')
    else:
        labs = (f'<div class="card" id="labs"><h3>Labs over time</h3><p>The lab report has not been run for this concept. '
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
    figs = []
    if au and png:
        figs.append(("Units and reference ranges", "the audit: the three checks per test, the decision map, verdicts, "
                     "where the units came from, the flagged groups", png))
    sp_png = {p.stem.split("_", 3)[-1]: p for p in sp if p.suffix == ".png"}
    for tag, title, what in SP_PAGES:
        for p in sorted((q for k, q in sp_png.items() if k.endswith(tag) or f"{tag}_" in k), key=lambda q: q.name):
            figs.append((title, what, p))
    pdf = next((p for p in sp if p.suffix == ".pdf"), None)
    figs_html = "".join(f'<figure class="fig"><figcaption><b>{E(t)}</b> · {E(w)}</figcaption>'
                        f'<a href="{o}/{E(p.name)}"><img src="{o}/{E(p.name)}" alt="{E(t)}" loading="lazy"></a></figure>'
                        for t, w, p in figs)
    if pdf is not None and not sp_png:  # a lab report from before the page images: say so, not just nothing
        figs_html += (f'<p class="muted">The lab report has no page images yet (it was written before sp_qc saved them): '
                      f're-run <code>sp_qc.py --omop {o}</code> and rebuild the portal, or open '
                      f'<a href="{o}/{E(pdf.name)}">the PDF</a>.</p>')
    if figs_html:
        figs_html = (f'<section id="figures"><h2>Figures <small>click a figure for full size'
                     + (f'; the lab flags as tables in <a href="{o}/{E(pdf.name)}">the PDF</a>' if pdf else "")
                     + f'</small></h2>{figs_html}</section>')
    return f"""
<div class="crumb"><a href="../index.html">← All concepts</a></div>
<section>
  <h1>{E(s["name"] or "OMOP " + o)}</h1>
  <div class="idline"><span>OMOP <span class="mono">{o}</span></span><span>{E(s["quantity"])}</span>
  <span>harmonized unit <span class="mono">{hu}</span></span>{athena}</div>
</section>
<section class="tiles">{tiles}</section>
<section><h2>Reports</h2><div class="cards">{audit}{labs}</div></section>
<section><h2>Composition <small>the test names and units merged into this concept</small></h2>
  <div class="charts wide">{year_svg(years)}</div>
  {timing_html(tim, clabs)}</section>
<section>
  <div class="controls"><input class="search" id="q" type="search" placeholder="Filter test names, units…" aria-label="Filter test names" data-search="comp"><span class="count" data-count="comp"></span></div>
  <div class="tablebox"><table class="sortable" id="comp"><thead><tr>{head}</tr></thead><tbody>{"".join(trs)}</tbody></table></div>
  <p class="muted" style="font-size:12.5px;margin:8px 2px 0">The tables as <a href="{o}/composition.tsv">composition.tsv</a>,
  <a href="{o}/years.tsv">years.tsv</a>. People don't add up over rows (one person
  can have several test names); rows do, pooled row included. Median, typical range and shares are shown only where they
  cover at least {MIN_N} people.</p>
</section>
{figs_html}
{labs_html}"""


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
    return (f'<div class="chart"><h3>The concept\'s labs vs all results</h3><p class="q">Which labs report this test more '
            f'or less than their share of all results would suggest? (top {len(d)}; shares over the labs shown)</p>'
            f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Labs: share of this concept vs all results">{"".join(g)}</svg>'
            f'<div class="lg"><span><i style="background:var(--accent);height:9px"></i>share of this concept\'s results</span>'
            f'<span><i style="background:var(--ink);height:9px;width:9px;border-radius:50%"></i>share of all results</span>'
            f'<span>×, /: ratio of the two</span></div></div>')


def lab_link(lab_id, label, paged, up=""):
    """A lab's name, linked when it has a page (the --top-labs largest)."""
    return f'<a href="{up}lab/{E(lab_id)}.html">{E(label)}</a>' if lab_id in paged else E(label)


def concept_labs_html(labs, o, paged=frozenset()):
    if labs is None or labs.empty:
        return ""
    head = "".join(f'<th class="{c}" scope="col">{h}</th>' for h, c in (
        ("Lab", ""), ("% of this concept", "num"), ("% of all results", "num"), ("Ratio", "num"), ("Years", "num")))
    trs = "".join(
        f'<tr data-text="{E(r.label.lower())}"><td>{lab_link(r.lab_id, r.label, paged, "../")}</td>'
        f'<td class="num" data-sort="{r.pct_concept}">{r.pct_concept:.1f}%</td>'
        f'<td class="num" data-sort="{r.pct_dataset}">{r.pct_dataset:.1f}%</td>'
        f'<td class="num" data-sort="{r.ratio}">{ratio_s(r.ratio)}</td>'
        f'<td class="num">{int(r.year_first)}–{int(r.year_last)}</td></tr>' for r in labs.itertuples())
    return f"""<section id="labs-of-concept"><h2>Labs <small>from the lab report's shown lab-years; shares over these labs</small></h2>
  <div class="charts" style="max-width:1100px">{labs_svg(labs)}</div>
  <div class="controls" style="margin-top:14px"><input class="search" type="search" placeholder="Filter labs…" aria-label="Filter labs" data-search="clabs"><span class="count" data-count="clabs"></span></div>
  <div class="tablebox"><table class="sortable" id="clabs" data-limit="20"><thead><tr>{head}</tr></thead><tbody>{trs}</tbody></table></div>
  <p class="muted" style="font-size:12.5px;margin:8px 2px 0">As <a href="{o}/labs.tsv">labs.tsv</a>. Ratio: the lab's share
  of this concept over its share of all results (both over the labs listed here); red at ×2 and more or /2 and less.</p>
</section>"""


def lab_years_svg(ly):
    """A lab's organisations over time: one stacked bar per period (years pooled to >= MIN_N people), as shares.
    Plain Python over the lab's rows (thousands of lab pages: no per-page pandas work)."""
    rows = list(zip(ly["year_from"], ly["year_to"], ly["org"], ly["share"], ly["pct_of_lab"]))
    if not rows:
        return ""
    segs, pct, weight = {}, {}, {}
    for y0, y1, org, sh, pl in rows:
        k = (int(y0), int(y1))
        segs.setdefault(k, {})[org] = float(sh)
        pct[k] = float(pl)
        weight[org] = weight.get(org, 0.0) + float(sh) * float(pl)
    named = [o for o in sorted(weight, key=lambda o: -weight[o]) if o not in (OTHER_ORG, NONE_ORG)][:4]
    order = named + [o for o in (OTHER_ORG, NONE_ORG) if o in weight]
    col = {o: f"var(--s{i + 1})" for i, o in enumerate(named)} | {OTHER_ORG: "var(--other)", NONE_ORG: "var(--none)"}
    W, H, ml, mr, mt, mb = 760, 230, 40, 10, 10, 26
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
                     f'({pct[(y0, y1)]:.1f}% of the lab\'s results)</title></rect>')
            acc += v
    every = 4 if x1 - x0 > 12 else 2 if x1 - x0 > 6 else 1
    for y in range(int(np.ceil(x0 / every) * every), x1, every):
        g.append(f'<text x="{(X(y) + X(y + 1)) / 2:.1f}" y="{H - 8}" text-anchor="middle">{y}</text>')
    lg = "".join(f'<span><i style="background:{col[o]};height:9px"></i>{E(o)}</span>' for o in order)
    return (f'<div class="chart"><h3>Coding-system organisations over time</h3><p class="q">What is this lab made of, '
            f'year by year? (share of its results; years pooled to cover at least {MIN_N} people)</p>'
            f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="Organisations over time">{"".join(g)}</svg>'
            f'<div class="lg">{lg}</div></div>')


def named_chip(v):
    if not num(v):
        return pct_s(None)
    v = float(v)
    return f'<span class="chip {"ok" if v >= 90 else "warn" if v >= 60 else "crit"}">{v:.0f}%</span>'


def labs_panel(labs, lab_conc, paged=frozenset()):
    head = "".join(f'<th class="{c}" scope="col">{h}</th>' for h, c in (
        ("Lab", ""), ("People", "num"), ("% of all results", "num"), ("Years", "num"), ("Named after", ""),
        ("Share of that organisation", "num"), ("No organisation", "num"), ("Concepts here", "num")))
    trs = "".join(
        f'<tr data-text="{E((r.label + " " + r.named_after).lower())}"><td>{lab_link(r.lab_id, r.label, paged)}</td>'
        f'{count_td(r.people)}<td class="num" data-sort="{r.pct_results}">{r.pct_results:.2f}%</td>'
        f'<td class="num">{"" if not num(r.year_first) else f"{int(r.year_first)}–{int(r.year_last)}"}</td>'
        f'<td>{E(r.named_after) or "<span class=muted>—</span>"}</td>'
        f'<td class="num" data-sort="{r.named_pct if num(r.named_pct) else ""}">{named_chip(r.named_pct)}</td>'
        f'<td class="num" data-sort="{r.no_org_pct if num(r.no_org_pct) else ""}">{pct_s(r.no_org_pct, 0)}</td>'
        f'<td class="num">{len(lab_conc.get(r.lab_id, []))}</td></tr>' for r in labs.itertuples())
    return f"""<section id="panel-labs" hidden>
  <p class="lead" style="margin-top:0">The labs (service providers) in this release with at least {MIN_N} people. A lab's name is
  the coding-system organisation most of its results carry (as in the lab reports). The {len(paged)} largest labs have a page
  (linked): what they are made of and how that changed over time; every lab's organisations are in lab_orgs.tsv. Shares are
  of all results of the labs listed.</p>
  <div class="controls" style="margin-top:12px"><input class="search" type="search" placeholder="Search labs, organisations…" aria-label="Search labs" data-search="labs"><span class="count" data-count="labs"></span></div>
  <div class="tablebox"><table class="sortable" id="labs" data-limit="20"><thead><tr>{head}</tr></thead><tbody>{trs}</tbody></table></div>
  <p class="muted" style="font-size:12.5px;margin:8px 2px 0">As <a href="lab/labs.tsv">labs.tsv</a>,
  <a href="lab/lab_orgs.tsv">lab_orgs.tsv</a>, <a href="lab/lab_years.tsv">lab_years.tsv</a>. Share of that organisation:
  its share of the lab's results that carry an organisation (green 90% and more, amber 60–90%, red under 60%).</p>
</section>"""


def lab_page(r, lo, ly, concepts):
    tile = lambda k, v, d="": (f'<div class="tile"><div class="k">{k}</div><div class="v">{v}</div>'  # noqa: E731
                               + (f'<div class="d">{d}</div>' if d else "") + "</div>")
    yrs = f"{int(r.year_first)}–{int(r.year_last)}" if num(r.year_first) else "—"
    tiles = "".join([
        tile("People", f'<span data-n="{int(r.people)}">{int(r.people):,}</span>'),
        tile("Share of all results", f"{r.pct_results:.2f}%"), tile("Years", yrs),
        tile("Named after", E(r.named_after) or "—", "the most common coding-system organisation"),
        tile("Share of that organisation", named_chip(r.named_pct), "of its results with an organisation"),
        tile("No organisation", pct_s(r.no_org_pct, 0), "results without a coding-system organisation")])
    orows = "".join(f'<tr><td>{E(org)}</td><td class="num" data-sort="{pc}">{pc:.1f}%</td></tr>'
                    for org, pc in sorted(zip(lo["org"], lo["pct"]), key=lambda t: -t[1]))
    crows = "".join(f'<tr><td><a href="../omop/{o}.html">{E(name or "OMOP " + o)}</a></td>'
                    f'<td class="num" data-sort="{pc}">{pc:.1f}%</td><td class="num" data-sort="{rt}">{ratio_s(rt)}</td></tr>'
                    for o, name, pc, rt in sorted(concepts, key=lambda c: -c[2]))
    conc = (f"""<section><h2>Concepts on this portal <small>its share of each concept's results (over the labs listed there)</small></h2>
  <div class="tablebox"><table class="sortable" id="lconc"><thead><tr><th>Concept</th><th class="num">% of the concept</th>
  <th class="num">Ratio to all results</th></tr></thead><tbody>{crows}</tbody></table></div></section>""" if crows else "")
    return f"""
<div class="crumb"><a href="../index.html#labs">← All labs</a></div>
<section>
  <h1>{E(r.label)}</h1>
  <div class="idline"><span>service provider, pseudonym ending <span class="mono">{E(r.lab_id.split("-")[0])}</span></span></div>
</section>
<section class="tiles">{tiles}</section>
<section><h2>Composition <small>the coding-system organisations behind this lab</small></h2>
  <div class="charts">{lab_years_svg(ly)}
  <div class="chart"><h3>Organisations, all years</h3><p class="q">Share of the lab's results; organisations covering too few
  people pooled</p><div class="tablebox"><table class="sortable" id="lorgs"><thead><tr><th>Organisation</th>
  <th class="num">Share</th></tr></thead><tbody>{orows}</tbody></table></div></div></div></section>
{conc}"""


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
    lab_conc = {}  # lab_id -> [(omop, name, % of the concept, ratio)]
    # pages (and charts) for the --top-labs largest labs only; every lab stays in the searchable table
    paged = frozenset(labs.sort_values("pct_results", ascending=False).head(args.top_labs)["lab_id"]) if len(labs) \
        else frozenset()
    log(f"Site: {len(tdirs)} concept page(s) ...")
    for d in tdirs:
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
        if len(clabs):
            clabs["lab_id"], clabs["label"] = clabs["lab_id"].astype(str), clabs["label"].astype(str)
            for r in clabs.itertuples():
                lab_conc.setdefault(r.lab_id, []).append((o, s["name"], float(r.pct_concept), float(r.ratio)))
        for n in ("composition", "years", "labs"):
            if (d / f"{n}.tsv").exists():
                shutil.copy2(d / f"{n}.tsv", fdir / f"{n}.tsv")
        verdicts = read_tsv(fdir / f"omop_{o}_ref_audit_verdicts.tsv") if (fdir / f"omop_{o}_ref_audit_verdicts.tsv").exists() else None
        release = meta.get("release", release)
        tp = fdir / f"sp_qc_{o}_timing.tsv"
        tim = pd.read_csv(tp, sep="\t", dtype=str, keep_default_na=False) if tp.exists() else None
        body = concept_page(s, comp, years, files, verdicts, clabs, paged, tim)
        (part / "omop" / f"{o}.html").write_text(page(f"{s['name'] or 'OMOP ' + o} · Kanta lab QC", body, 1, release, built))
        sums.append(s)
    sums.sort(key=lambda s: -int(s["rows"]))
    if len(labs):
        (part / "lab").mkdir()
        for n in ("labs", "lab_orgs", "lab_years"):
            shutil.copy2(ldir / f"{n}.tsv", part / "lab" / f"{n}.tsv")
        lo_by, ly_by = dict(tuple(lab_orgs.groupby("lab_id"))), dict(tuple(lab_years.groupby("lab_id")))
        empty_o, empty_y = lab_orgs.iloc[:0], lab_years.iloc[:0]
        log(f"Site: {len(paged)} lab page(s) (the largest of {len(labs)} labs) ...")
        for i, r in enumerate(labs[labs["lab_id"].isin(paged)].itertuples(), 1):
            body = lab_page(r, lo_by.get(r.lab_id, empty_o), ly_by.get(r.lab_id, empty_y), lab_conc.get(r.lab_id, []))
            (part / "lab" / f"{r.lab_id}.html").write_text(page(f"{r.label} · Kanta lab QC", body, 1, release, built))
            if i % 500 == 0:
                log(f"Site: {i} of {len(paged)} lab pages")
    (part / "index.html").write_text(page("Kanta Lab QC Portal", landing(sums, labs, lab_conc, paged), 0, release, built))
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
    p.add_argument("--symlink", action="store_true", help="Link the PDF / PNG reports instead of copying them")
    p.add_argument("--top-labs", type=int, default=20,
                   help="Labs (largest by share of results) that get a page with charts; all are in the labs table")
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
