"""Fetch THL/Kela National Health Index values per wellbeing services county (hyvinvointialue) from Sotkanet.

Writes data/sotkanet_health_index_<year>.tsv next to this script: one row per hyvinvointialue with the maakunta it
belongs to, its population and the age-standardised disease indices (whole country = 100), so region_people.py can
compare FinnGen endpoint prevalence per assigned maakunta against them without internet access. Uses the Sotkanet
open REST API (https://sotkanet.fi/rest/1.1/); standard library only.
"""
import argparse, csv, json, urllib.request
from pathlib import Path

API = 'https://sotkanet.fi/rest/1.1'
POPULATION = 127  # Väestö 31.12. (population at year end)
INDICES = {  # output column -> Sotkanet indicator (age-standardised, whole country = 100)
    'diabetes_index': 5661,         # Diabetesindeksi, ikävakioitu
    'coronary_index': 5645,         # Sepelvaltimotauti-indeksi, ikävakioitu
    'cerebrovascular_index': 5647,  # Aivoverisuonitauti-indeksi, ikävakioitu
    'lung_index': 5657,             # Keuhkosairausindeksi, ikävakioitu
    'memory_index': 5655,           # Muistisairausindeksi, ikävakioitu
}
# hyvinvointialue (Sotkanet title) -> maakunta; one-to-one except Uusimaa, split into Helsinki and four counties
HVA_TO_MAAKUNTA = {
    'Helsingin kaupunki': 'Uusimaa', 'Itä-Uudenmaan hyvinvointialue': 'Uusimaa', 'Keski-Uudenmaan hyvinvointialue': 'Uusimaa',
    'Länsi-Uudenmaan hyvinvointialue': 'Uusimaa', 'Vantaan ja Keravan hyvinvointialue': 'Uusimaa',
    'Varsinais-Suomen hyvinvointialue': 'Varsinais-Suomi', 'Satakunnan hyvinvointialue': 'Satakunta',
    'Kanta-Hämeen hyvinvointialue': 'Kanta-Häme', 'Pirkanmaan hyvinvointialue': 'Pirkanmaa', 'Päijät-Hämeen hyvinvointialue': 'Päijät-Häme',
    'Kymenlaakson hyvinvointialue': 'Kymenlaakso', 'Etelä-Karjalan hyvinvointialue': 'Etelä-Karjala', 'Etelä-Savon hyvinvointialue': 'Etelä-Savo',
    'Pohjois-Savon hyvinvointialue': 'Pohjois-Savo', 'Pohjois-Karjalan hyvinvointialue': 'Pohjois-Karjala', 'Keski-Suomen hyvinvointialue': 'Keski-Suomi',
    'Etelä-Pohjanmaan hyvinvointialue': 'Etelä-Pohjanmaa', 'Pohjanmaan hyvinvointialue': 'Pohjanmaa',
    'Keski-Pohjanmaan hyvinvointialue': 'Keski-Pohjanmaa', 'Pohjois-Pohjanmaan hyvinvointialue': 'Pohjois-Pohjanmaa',
    'Kainuun hyvinvointialue': 'Kainuu', 'Lapin hyvinvointialue': 'Lappi', 'Ahvenanmaa': 'Ahvenanmaa',
}


def get(path):
    # Sotkanet answers 403 to urllib's default User-Agent
    req = urllib.request.Request(f'{API}/{path}', headers={'User-Agent': 'kanta-lab-preprocessing/fetch_sotkanet'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--year', type=int, default=2024, help="Index year (each value covers the 3 years ending in it; default: 2024)")
args = ap.parse_args()

regions = {r['id']: r for r in get('regions')}
hva = {rid: r['title']['fi'] for rid, r in regions.items() if r.get('category') == 'HYVINVOINTIALUE'}
values = {name: {} for name in hva.values()}
for col, indicator in {'population': POPULATION, **INDICES}.items():
    for x in get(f'json?indicator={indicator}&years={args.year}&genders=total'):
        if x['region'] in hva and x.get('value') is not None:
            values[hva[x['region']]][col] = x['value']

unmapped = sorted(set(values) - set(HVA_TO_MAAKUNTA))
if unmapped:
    raise SystemExit(f"Sotkanet hyvinvointialue names not in HVA_TO_MAAKUNTA: {unmapped}")

out = Path(__file__).resolve().parent / 'data' / f'sotkanet_health_index_{args.year}.tsv'
out.parent.mkdir(exist_ok=True)
columns = ['hyvinvointialue', 'maakunta', 'population', *INDICES]
with open(out, 'w', encoding='utf-8', newline='') as f:
    w = csv.writer(f, delimiter='\t', lineterminator='\n')
    w.writerow(columns)
    for name in sorted(values, key=lambda n: (HVA_TO_MAAKUNTA[n], n)):
        w.writerow([name, HVA_TO_MAAKUNTA[name], *[values[name].get(c, '') for c in columns[2:]]])
print(f"Wrote {out} ({len(values)} hyvinvointialue, year {args.year}; indicators: population {POPULATION}, "
      + ", ".join(f"{c} {i}" for c, i in INDICES.items()) + ")")
