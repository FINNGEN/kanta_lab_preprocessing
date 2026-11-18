#!/usr/bin/env python3
"""Download the population of each maakunta (31 December of --year) from Statistics Finland's open statistics service
(StatFin, table 11ra "Key figures on population by region"; CC BY 4.0, (c) Statistics Finland) into
maakunta_population.tsv next to this script: maakunta, code, year, population. The QC portal
(scripts/qc_checks/qc_portal.py) compares each region's share of the results with its share of the population.

Usage
-----
  python3 data/fetch_maakunta_population.py [--year 2024]
"""
import argparse
import json
import urllib.request
from pathlib import Path

URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/vaerak/11ra.px"
OUT = Path(__file__).resolve().parent / "maakunta_population.tsv"


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--year", default="2024", help="Population on 31 December of this year")
    args = ap.parse_args()
    with urllib.request.urlopen(URL, timeout=120) as r:
        meta = json.load(r)
    area = next(v for v in meta["variables"] if v["text"] == "Alue")
    mk = [(c, t.split(" ", 1)[1]) for c, t in zip(area["values"], area["valueTexts"]) if c.startswith("MK")]
    query = {"query": [{"code": area["code"], "selection": {"filter": "item", "values": [c for c, _ in mk]}},
                       {"code": "contentscode", "selection": {"filter": "item", "values": ["vaerak-vaesto"]}},
                       {"code": "timeperiod_y", "selection": {"filter": "item", "values": [str(args.year)]}}],
             "response": {"format": "json"}}
    req = urllib.request.Request(URL, data=json.dumps(query).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        data = json.load(r)
    pop = {row["key"][0]: int(row["values"][0]) for row in data["data"]}
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("maakunta\tcode\tyear\tpopulation\n")
        for code, name in mk:
            f.write(f"{name}\t{code}\t{args.year}\t{pop[code]}\n")
    print(f"{len(mk)} maakunnat, {sum(pop.values()):,} people in {args.year} -> {OUT}")


if __name__ == "__main__":
    main()
