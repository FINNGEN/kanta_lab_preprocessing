"""Fetch every municipality code Statistics Finland has used since 1990, with its maakunta, next to this script.

Writes municipality_history.tsv (in scripts/sote/data/), one row per municipality code:
    MunicipalityCode  MunicipalityName  LastYear  MaakuntaCode  Maakunta
LastYear is the last classification year the code is valid in (the current year for existing municipalities), and
the maakunta is the one the municipality belonged to in that year, translated to today's maakunnat (only Itä-Uusimaa,
merged into Uusimaa in 2011, needed that). A municipality that was merged away keeps the maakunta its area was in
when it was merged (Statistics Finland's own assignment, not its successor's; these tables don't say which
municipality it was merged into).

Used for register units recorded with an old (merged) municipality code, which municipality_to_region.tsv (current
municipalities only) doesn't have. Source: the Statistics Finland classification service
(https://data.stat.fi/api/classifications/v2), the yearly municipality -> maakunta correspondence tables
(kunta_1_<year>#maakunta_1_<year>) and the municipality classifications (kunta_1_<year>) for the names. Standard
library only.
"""
import argparse, csv, json, re, sys, time, urllib.request
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent  # scripts/sote/data
API = "https://data.stat.fi/api/classifications/v2"
# Maakunta codes no longer in use -> their current maakunta.
OLD_MAAKUNTA = {"20": "01"}  # Itä-Uusimaa, merged into Uusimaa on 1.1.2011


def get(path):
    req = urllib.request.Request(f"{API}/{path}", headers={"User-Agent": "kanta-lab-preprocessing/fetch_municipality_history"})
    with urllib.request.urlopen(req, timeout=300) as r:
        return json.loads(r.read().decode("utf-8"))


def code_of(local_id):
    return local_id.split("/")[1]


def item_names(classification):
    """code -> Finnish name of a classification's items."""
    items = get(f"classifications/{classification}/classificationItems?content=data&meta=max&lang=fi")
    return {i["code"]: i["classificationItemNames"][0]["name"] for i in items}


def main(args):
    tables = [t["localId"] for t in get("correspondenceTables?content=data&meta=max&lang=fi&pageSize=2000")]
    # one municipality -> maakunta table per municipality year, preferring the maakunta classification of the same year
    by_year = {}
    for t in sorted(tables):
        m = re.fullmatch(r"kunta_1_(\d{4})0101#maakunta_1_(\d{4})\d{4}", t)
        if m and (m.group(1) not in by_year or m.group(1) == m.group(2)):
            by_year[m.group(1)] = t
    if not by_year:
        sys.exit("no municipality -> maakunta tables found")
    print(f"{len(by_year)} yearly municipality -> maakunta tables, {min(by_year)}-{max(by_year)}")

    last = {}  # municipality code -> (year, maakunta code)
    for year, table in sorted(by_year.items()):
        for x in get(f"correspondenceTables/{table.replace('#', '%23')}/maps?content=data&meta=max&lang=fi"):
            last[code_of(x["sourceLocalId"])] = (year, code_of(x["targetLocalId"]))
        time.sleep(0.1)

    # municipality names in each code's last year; current maakunta names from the newest maakunta classification
    names = {}
    for year in sorted({y for y, _ in last.values()}):
        year_names = item_names(f"kunta_1_{year}0101")
        names.update({c: year_names[c] for c, (y, _) in last.items() if y == year and c in year_names})
        time.sleep(0.1)
    newest_maakunta = by_year[max(by_year)].split("#")[1]
    maakunnat = item_names(newest_maakunta)

    rows, unknown = [], set()
    for code, (year, mk) in sorted(last.items()):
        mk = OLD_MAAKUNTA.get(mk, mk)
        if mk not in maakunnat:
            unknown.add(mk)
            continue
        rows.append({"MunicipalityCode": code, "MunicipalityName": names.get(code, ""), "LastYear": year,
                     "MaakuntaCode": mk, "Maakunta": maakunnat[mk]})
    if unknown:
        sys.exit(f"maakunta codes not in {newest_maakunta} and not in OLD_MAAKUNTA: {sorted(unknown)}")

    out = DATA_DIR / "municipality_history.tsv"
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)
    current = sum(r["LastYear"] == max(by_year) for r in rows)
    print(f"Wrote {out}: {len(rows)} municipality codes ({current} valid in {max(by_year)}, {len(rows) - current} merged away)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    main(ap.parse_args())
