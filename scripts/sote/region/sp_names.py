#!/usr/bin/env python3
"""Name each SERVICE_PROVIDER (SP) by the coding systems its rows carry, as the QC reports do.

SERVICE_PROVIDER is a pseudonym, so an SP is named after the organisation most of its rows' coding systems belong to
(CODING_SYSTEM_MAP in the engine output, from src/kanta/engine/data/coding_system_map.tsv; top_csm in sp_region.tsv),
e.g. HUS-yhtymä, HELSINGIN KAUPUNKI. Except: when that organisation has no maakunta of its own (a private company such
as Fimlab, a lab serving several regions such as NordLab) and the SP has a region, the SP is named after the region
instead, e.g. "Pirkanmaa" for an SP whose rows carry Fimlab's and Pirkanmaa's coding systems. A real OID
(1.2.246[.537].10.<code>...) is named from coding_system_map.tsv by its organisation code first.

The SP's region, as in sp_region.tsv (sp_region_cs.py): among its rows whose organisation has a maakunta
(CODING_SYSTEM_MAAKUNTA), the top maakunta (pct_maakunta: its share of those rows); the SP has a region when those
regional rows are at least --region-min of its rows with an organisation (pct_regional), so a nationwide private
chain with a few stray municipal rows has none.

Used by scripts/qc_checks/sp_qc.py and qc_portal.py (import sp_names, the same Labels), so the names agree everywhere;
they can also take this script's kanta_sp_names.tsv (--sp-names) instead of counting the organisations themselves.

Input: the engine output parquet. Writes --out/kanta_sp_names.tsv, one row per SP with at least one row with an
organisation (the others are named "SP …<tail>"):
  SERVICE_PROVIDER    the pseudonym
  name                the name (organisation or region), without the tail
  label               the name as the reports print it: at most 28 characters, then " …" and the pseudonym's last 6
  named_after         organisation | region | oid
  top_org             the top coding-system organisation; top_org_pct its share of the SP's rows with an organisation
  top_org_regional    whether that organisation has a maakunta of its own
  maakunta            the SP's region (empty when it has none); pct_maakunta and pct_regional as above
  top_maakunta        the top maakunta among its regional rows, also when under --region-min
  n_org_rows          the SP's rows with an organisation
The file holds pseudonyms and row counts: it stays on the data side (not for publishing).

Usage
-----
  python3 scripts/sote/region/sp_names.py [--input <engine_output>.parquet] [--out DIR]
"""
import argparse
import re
import sys
import time
from pathlib import Path

import duckdb
import pandas as pd

ENGINE_DIR = Path("/mnt/disks/data/kanta/engine")
ORG_MAP = Path(__file__).resolve().parents[3] / "src/kanta/engine/data/coding_system_map.tsv"
ORG_CODE = re.compile(r"^1\.2\.246\.(?:537\.)?10\.(\d+)\.")
# engine output column names first, then the release's
SP_COLS = ("SERVICE_PROVIDER", "SERVICE_PROVIDER_ID")
ORG_COLS = ("CODING_SYSTEM_MAP", "CODING_SYSTEM_ORG")
MK_COLS = ("CODING_SYSTEM_MAAKUNTA",)
OUT_NAME = "kanta_sp_names.tsv"
COLUMNS = ["SERVICE_PROVIDER", "name", "label", "named_after", "top_org", "top_org_pct", "top_org_regional",
           "maakunta", "pct_maakunta", "pct_regional", "top_maakunta", "n_org_rows"]


def sql_str(x):
    return "'" + str(x).replace("'", "''") + "'"


def org_counts_sql(inp, types):
    """The duckdb query for the SP x organisation x maakunta row counts (k_sp, org, mk, n) over the whole input, given
    the input's column types; None when it has no SP or organisation column."""
    csp = next((c for c in SP_COLS if c in types), None)
    corg = next((c for c in ORG_COLS if c in types), None)
    cmk = next((c for c in MK_COLS if c in types), None)
    if not (csp and corg):
        return None
    mk = f"""nullif(nullif(trim(cast("{cmk}" as varchar)), ''), 'NA')""" if cmk else "cast(null as varchar)"
    return (f"""select "{csp}" k_sp, "{corg}" org, {mk} mk, count(*) n from read_parquet({sql_str(inp)})
                where "{corg}" is not null and "{corg}" not in ('NA', '') group by all""")


def org_counts(con, inp):
    """SP x organisation x maakunta row counts over the whole input (one group-by: few groups, little memory)."""
    types = {r[0]: r[1] for r in con.sql(f"describe select * from read_parquet({sql_str(inp)})").fetchall()}
    q = org_counts_sql(inp, types)
    if q is None:
        raise SystemExit(f"{inp}: needs one of {SP_COLS} and one of {ORG_COLS}")
    return con.sql(q).df()


def _pct(x):
    return None if x is None or pd.isna(x) else round(100 * float(x), 2)


class Labels:
    """SP -> name (see the module docstring). Fed with (k_sp, org, n[, mk]) counts through add_orgs (the whole input's
    first, then a per-OMOP table only fills gaps), or loaded from kanta_sp_names.tsv with from_tsv.
    self.region: SP -> (top maakunta, pct_maakunta, pct_regional) as fractions; REGION_MIN: the regional share under
    which the SP has no region (region_people.py's --sp-min-region-pct)."""
    REGION_MIN = 0.05

    def __init__(self, org_map=ORG_MAP):
        self.code, self.org, self.region, self.top_regional, self.top_pct, self.n = {}, {}, {}, {}, {}, {}
        if org_map and Path(org_map).exists():
            m = pd.read_csv(org_map, sep="\t", dtype=str, usecols=["code", "org_name"]).dropna()
            self.code = dict(zip(m["code"], m["org_name"]))

    def add_orgs(self, orgs):
        """Names (and regions) for the SPs not named yet, from an (k_sp, org, n[, mk]) table."""
        if orgs is None or not len(orgs):
            return
        o = orgs[~orgs["org"].isin(["NA", ""])].copy()
        o["mk"] = o["mk"].where(~o["mk"].isin(["NA", ""])) if "mk" in o.columns else None
        o = o.groupby(["k_sp", "org"], as_index=False, dropna=False).agg(n=("n", "sum"), mk=("mk", "first"))
        for sp, g in o.groupby("k_sp"):
            if sp in self.org:
                continue
            g = g.sort_values(["n", "org"], ascending=[False, True])
            tot = g["n"].sum()
            self.org[sp] = g["org"].iloc[0].replace("_", " ")
            self.top_pct[sp] = float(g["n"].iloc[0] / tot)
            self.n[sp] = int(tot)
            self.top_regional[sp] = bool(pd.notna(g["mk"].iloc[0]))  # its top organisation has a maakunta
            reg = g[g["mk"].notna()]
            if len(reg):
                by = reg.groupby("mk")["n"].sum().sort_values(ascending=False)
                self.region[sp] = (by.index[0], float(by.iloc[0] / by.sum()), float(reg["n"].sum() / tot))

    @classmethod
    def from_tsv(cls, path, org_map=ORG_MAP):
        """The Labels of an kanta_sp_names.tsv (this script's output)."""
        self = cls(org_map)
        t = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
        for r in t.itertuples(index=False):
            sp = r.SERVICE_PROVIDER
            if r.top_org:
                self.org[sp] = r.top_org
                self.top_pct[sp] = float(r.top_org_pct) / 100 if r.top_org_pct else None
                self.top_regional[sp] = r.top_org_regional == "True"
            if r.n_org_rows:
                self.n[sp] = int(r.n_org_rows)
            if r.top_maakunta:
                self.region[sp] = (r.top_maakunta, float(r.pct_maakunta) / 100, float(r.pct_regional) / 100)
        return self

    @staticmethod
    def name(kind, key):
        """A plain dimension value as a label ("NA" for missing); used by sp_qc.py for the non-SP dimensions."""
        if key is None or (isinstance(key, float) and pd.isna(key)) or str(key) in ("", "None", "nan", "<NA>"):
            return "NA"
        return str(key)

    def named(self, raw):
        """(name, named_after) of the SP, or (None, None) when it has no organisation at all."""
        raw = str(raw)
        code = ORG_CODE.match(raw)
        if code and self.code.get(code.group(1)):
            return self.code[code.group(1)], "oid"
        # its top organisation, unless that one has no region (private, or serving several regions) and the SP has one
        reg = None if self.top_regional.get(raw, True) else self.region_name(raw)
        if reg:
            return reg, "region"
        return (self.org[raw], "organisation") if raw in self.org else (None, None)

    def sp(self, raw):
        """The label the reports print: the name (at most 28 characters), " …" and the pseudonym's last 6."""
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            return "SP NA"
        raw = str(raw)
        org = self.named(raw)[0]
        return f"{org[:28]} …{raw[-6:]}" if org else f"SP …{raw[-6:]}"

    def region_name(self, raw):
        """The SP's maakunta when its regional rows are >= REGION_MIN of its rows with an organisation (else None)."""
        reg = self.region.get(raw)
        return reg[0] if reg and reg[2] >= self.REGION_MIN else None

    def table(self):
        """One row per SP with an organisation: the kanta_sp_names.tsv columns, largest SPs first."""
        rows = []
        for sp in self.org:
            name, after = self.named(sp)
            reg = self.region.get(sp)
            region = self.region_name(sp)
            rows.append({"SERVICE_PROVIDER": sp, "name": name, "label": self.sp(sp), "named_after": after,
                         "top_org": self.org[sp], "top_org_pct": _pct(self.top_pct.get(sp)),
                         "top_org_regional": self.top_regional.get(sp), "maakunta": region,
                         "pct_maakunta": _pct(reg[1]) if reg else None, "pct_regional": _pct(reg[2]) if reg else None,
                         "top_maakunta": reg[0] if reg else None, "n_org_rows": self.n.get(sp)})
        t = pd.DataFrame(rows, columns=COLUMNS)
        return t.sort_values(["n_org_rows", "SERVICE_PROVIDER"], ascending=[False, True], na_position="last")


def write_tsv(df, path):
    """Plain TSV, empty for missing, written atomically."""
    tmp = Path(str(path) + ".part")
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\t".join(df.columns) + "\n")
        for row in df.itertuples(index=False):
            f.write("\t".join("" if v is None or (not isinstance(v, str) and pd.isna(v)) else str(v) for v in row) + "\n")
    tmp.replace(path)


def latest_engine_output(engine_dir=ENGINE_DIR):
    runs = [(d.name[-10:], (d / f"{d.name}.parquet").stat().st_mtime, d / f"{d.name}.parquet")
            for d in engine_dir.glob("*_[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]") if (d / f"{d.name}.parquet").is_file()]
    return max(runs)[2] if runs else None


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0],
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument("--input", type=Path, default=latest_engine_output(),
                    help="Engine output (or release) parquet with SERVICE_PROVIDER, CODING_SYSTEM_MAP and "
                         "CODING_SYSTEM_MAAKUNTA")
    ap.add_argument("--out", type=Path, default=Path("/mnt/disks/data/kanta/service-provider/region"),
                    help="Output directory (kanta_sp_names.tsv)")
    ap.add_argument("--org-map", type=Path, default=ORG_MAP,
                    help="coding_system_map.tsv, to name real OIDs by their organisation code (skipped if missing)")
    ap.add_argument("--region-min", type=float, default=Labels.REGION_MIN,
                    help="Regional rows' share of an SP's rows with an organisation for it to have a region")
    ap.add_argument("--memory-limit", default=None, help="duckdb memory limit, e.g. 12GB")
    args = ap.parse_args()
    if args.input is None:
        raise SystemExit(f"No engine output in {ENGINE_DIR}: pass --input")
    args.out.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    con = duckdb.connect()
    if args.memory_limit:
        con.sql(f"set memory_limit = '{args.memory_limit}'")
    print(f"SP names: counting organisations per service provider in {args.input} ...", file=sys.stderr, flush=True)
    labels = Labels(args.org_map)
    labels.REGION_MIN = args.region_min
    labels.add_orgs(org_counts(con, args.input))
    t = labels.table()
    write_tsv(t, args.out / OUT_NAME)
    after = t["named_after"].value_counts().to_dict()
    print(f"SP names: {len(t):,} service providers ({', '.join(f'{v:,} by {k}' for k, v in after.items())}) "
          f"-> {args.out / OUT_NAME} [{time.time() - t0:.0f}s]", file=sys.stderr)


if __name__ == "__main__":
    main()
