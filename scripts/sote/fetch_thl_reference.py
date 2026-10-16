"""Fetch THL's regional disease reference data per wellbeing services county (hyvinvointialue) for region_people.py.

Writes two files to data/ next to this script, so region_people.py can compare FinnGen endpoint prevalence per
assigned maakunta against them without internet access; standard library only.

- sotkanet_health_index_<year>.tsv: THL/Kela National Health Index from Sotkanet (https://sotkanet.fi/rest/1.1/),
  one row per hyvinvointialue with its maakunta, population and the age-standardised disease indices (country = 100).
- thl_ttr_incidence_<first>_<last>.tsv: THL's infectious disease register (Tartuntatautirekisteri, statistics cube
  https://sampo.thl.fi/pivot/prod/fi/ttr/cases/fact_ttr_cases), one row per hyvinvointialue with its maakunta,
  person-years and, per disease, the cases, the expected cases and the incidence index (observed / expected x 100,
  country = 100), indirectly age- and sex-standardised: expected = the region's person-years per 5-year age band x
  sex (Statistics Finland table 11re, by municipality) x the national register rate in that band. The crude index
  (no standardisation) is kept for comparison. The National Health Index has no infections, so these come from the
  register.
"""
import argparse, csv, json, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOTKANET = 'https://sotkanet.fi/rest/1.1'
POPULATION = 127  # Väestö 31.12. (population at year end)
INDICES = {  # output column -> Sotkanet indicator (age-standardised, whole country = 100)
    'diabetes_index': 5661,         # Diabetesindeksi, ikävakioitu
    'coronary_index': 5645,         # Sepelvaltimotauti-indeksi, ikävakioitu
    'cerebrovascular_index': 5647,  # Aivoverisuonitauti-indeksi, ikävakioitu
    'lung_index': 5657,             # Keuhkosairausindeksi, ikävakioitu
    'memory_index': 5655,           # Muistisairausindeksi, ikävakioitu
    'alcohol_index': 5659,          # Alkoholisairastavuusindeksi, ikävakioitu
}
TTR_CUBE = 'https://sampo.thl.fi/pivot/prod/fi/ttr/cases/fact_ttr_cases'
STATFIN_POPULATION = 'https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/vaerak/11re.px'  # population by municipality, age, sex
DISEASES = {  # output column prefix -> report group label in the register cube
    'lyme': 'Lymen borrelioosi',
    'tularemia': 'Tularemia (jänisrutto)',
}

ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--year', type=int, default=2024, help="National Health Index year (each value covers the 3 years ending in it; default: 2024)")
ap.add_argument('--first-year', type=int, default=2015, help="First year of register cases summed (default: 2015)")
ap.add_argument('--last-year', type=int, default=2024, help="Last year of register cases summed (default: 2024)")
args = ap.parse_args()

# hyvinvointialue -> maakunta, from the municipality table used everywhere in scripts/sote (one-to-one except
# Uusimaa, split into Helsinki and four counties)
with open(HERE / 'data' / 'municipality_to_region.tsv', encoding='utf-8') as f:
    HVA_TO_MAAKUNTA = {r['Hyvinvointialue']: r['Maakunta'] for r in csv.DictReader(f, delimiter='\t')}
HVA_TO_MAAKUNTA['Ahvenanmaa'] = 'Ahvenanmaa'


def get(url):
    # Sotkanet answers 403 to urllib's default User-Agent
    req = urllib.request.Request(url, headers={'User-Agent': 'kanta-lab-preprocessing/fetch_thl_reference'})
    with urllib.request.urlopen(req, timeout=300) as r:
        text = r.read().decode('utf-8')
    # the register cube's dimensions endpoint is JSONP: thl.pivot.loadDimensions(...)
    return json.loads(text if text.lstrip().startswith(('{', '[')) else text[text.index('(') + 1:text.rindex(')')])


def check_mapped(names, source):
    unmapped = sorted(set(names) - set(HVA_TO_MAAKUNTA))
    if unmapped:
        raise SystemExit(f"{source} hyvinvointialue names not in municipality_to_region.tsv: {unmapped}")


def write_tsv(path, columns, rows):
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f, delimiter='\t', lineterminator='\n')
        w.writerow(columns)
        w.writerows(rows)


# --- THL/Kela National Health Index (Sotkanet)
regions = {r['id']: r for r in get(f'{SOTKANET}/regions')}
hva = {rid: r['title']['fi'] for rid, r in regions.items() if r.get('category') == 'HYVINVOINTIALUE'}
values = {name: {} for name in hva.values()}
for col, indicator in {'population': POPULATION, **INDICES}.items():
    for x in get(f'{SOTKANET}/json?indicator={indicator}&years={args.year}&genders=total'):
        if x['region'] in hva and x.get('value') is not None:
            values[hva[x['region']]][col] = x['value']
check_mapped(values, 'Sotkanet')
out = HERE / 'data' / f'sotkanet_health_index_{args.year}.tsv'
columns = ['hyvinvointialue', 'maakunta', 'population', *INDICES]
write_tsv(out, columns, [[name, HVA_TO_MAAKUNTA[name], *[values[name].get(c, '') for c in columns[2:]]]
                         for name in sorted(values, key=lambda n: (HVA_TO_MAAKUNTA[n], n))])
print(f"Wrote {out} ({len(values)} hyvinvointialue, year {args.year}; indicators: population {POPULATION}, "
      + ", ".join(f"{c} {i}" for c, i in INDICES.items()) + ")")


# --- Infectious disease register (statistics cube)
def sid(dimensions, dim_id, label=None, stage=None):
    """sid of the node with this label (or the root, by stage) in a cube dimension."""
    def walk(nodes):
        for n in nodes:
            if (label is not None and n.get('label') == label) or (stage is not None and n.get('stage') == stage):
                yield n['sid']
            yield from walk(n.get('children', []))
    found = list(walk(next(d for d in dimensions if d['id'] == dim_id)['children']))
    if not found:
        raise SystemExit(f"{dim_id}: no node {label or stage!r} in the register cube")
    return found[0]


def jsonstat_cells(ds):
    """Cells of a JSON-stat dataset (THL cube: values as a dict; Statistics Finland's JSON-stat 2: as a list), each
    as {dimension id: category label, ..., 'value': float}. The flat value index runs over the dimensions in order,
    the last one fastest."""
    dim = ds['dimension']
    ids, size = (dim['id'], dim['size']) if 'id' in dim else (ds['id'], ds['size'])
    codes = [sorted(dim[i]['category']['index'], key=dim[i]['category']['index'].get) for i in ids]
    labels = {i: dim[i]['category']['label'] for i in ids}
    values = ds['value'].items() if isinstance(ds['value'], dict) else enumerate(ds['value'])
    for k, v in values:
        if v is None:
            continue
        k, pos = int(k), []
        for s in reversed(size):
            pos.append(k % s)
            k //= s
        yield {i: labels[i][codes[n][p]] for n, (i, p) in enumerate(zip(ids, reversed(pos)))} | {'value': float(v)}


dims = get(f'{TTR_CUBE}.dimensions.json')
region_dim = next(d['id'] for d in dims if d['id'].startswith('wscmunicipality'))
years = '.'.join(str(sid(dims, 'yearmonth', label=f'Vuosi {y}')) for y in range(args.first_year, args.last_year + 1))
measures = '.'.join(str(sid(dims, 'measure', label=m)) for m in ('Tapaukset', 'Väestö'))
groups = '.'.join(str(sid(dims, 'nidrreportgroup', label=lab)) for lab in DISEASES.values())
age_bands = [n['label'] for n in next(d for d in dims if d['id'] == 'nidragegroup')['children'][0]['children']]  # 0–4 ... 85+

# cases per hyvinvointialue (all ages), and nationally per age band x sex, summed over the years; the register has
# no age x region breakdown, so the expected cases come from the national rates and the regional population below
cases = {}  # (disease label, hyvinvointialue) -> cases
for c in jsonstat_cells(get(f"{TTR_CUBE}.json?row=nidrreportgroup-{groups}.&row={region_dim}-{sid(dims, region_dim, stage='root')}L"
                            f"&column=yearmonth-{years}.&column=measure-{measures}.")['dataset']):
    if c['measure'] == 'Tapaukset' and c[region_dim] != 'Kaikki Alueet':
        cases[c['nidrreportgroup'], c[region_dim]] = cases.get((c['nidrreportgroup'], c[region_dim]), 0) + c['value']
national = {}  # (disease label, age band, sex, measure) -> sum
for c in jsonstat_cells(get(f"{TTR_CUBE}.json?row=nidrreportgroup-{groups}.&row=nidragegroup-{sid(dims, 'nidragegroup', stage='root')}L"
                            f"&row=nidrsex-{sid(dims, 'nidrsex', stage='root')}L&column=yearmonth-{years}.&column=measure-{measures}.")['dataset']):
    if c['nidragegroup'] in age_bands and c['nidrsex'] in ('Miehet', 'Naiset'):
        key = (c['nidrreportgroup'], c['nidragegroup'], c['nidrsex'], c['measure'])
        national[key] = national.get(key, 0) + c['value']
rate = {(lab, b, s): national.get((lab, b, s, 'Tapaukset'), 0) / national[(lab, b, s, 'Väestö')]
        for lab, b, s, m in national if m == 'Väestö' and national[(lab, b, s, 'Väestö')]}

# population per hyvinvointialue x age band x sex, summed over the years (year-end population, as the register's),
# from Statistics Finland table 11re by municipality in the current municipality division; one request per year
with open(HERE / 'data' / 'municipality_to_region.tsv', encoding='utf-8') as f:
    muni_to_hva = {r['MunicipalityName']: r['Hyvinvointialue'] for r in csv.DictReader(f, delimiter='\t')}
meta = get(STATFIN_POPULATION)
var = {v['text']: v for v in meta['variables']}
munis = [c for c in var['Alue']['values'] if c.startswith('KU')]
ages = [c for c in var['Ikä']['values'] if c != 'SSS']
population = {}  # (hyvinvointialue, age band, sex) -> person-years
for year in range(args.first_year, args.last_year + 1):
    query = {'query': [{'code': var['Alue']['code'], 'selection': {'filter': 'item', 'values': munis}},
                       {'code': var['Ikä']['code'], 'selection': {'filter': 'item', 'values': ages}},
                       {'code': var['Sukupuoli']['code'], 'selection': {'filter': 'item', 'values': ['1', '2']}},
                       {'code': var['Vuosi']['code'], 'selection': {'filter': 'item', 'values': [str(year)]}}],
             'response': {'format': 'json-stat2'}}
    req = urllib.request.Request(STATFIN_POPULATION, data=json.dumps(query).encode(), headers={
        'Content-Type': 'application/json', 'User-Agent': 'kanta-lab-preprocessing/fetch_thl_reference'})
    with urllib.request.urlopen(req, timeout=300) as r:
        ds = json.load(r)
    for c in jsonstat_cells(ds):
        hva = muni_to_hva.get(c[var['Alue']['code']])
        if hva is None:
            raise SystemExit(f"Statistics Finland municipality {c[var['Alue']['code']]!r} not in municipality_to_region.tsv")
        age = int(c[var['Ikä']['code']].split()[0])  # '100 -' -> 100
        key = (hva, age_bands[min(age // 5, len(age_bands) - 1)], c[var['Sukupuoli']['code']])
        population[key] = population.get(key, 0) + c['value']

ttr_regions = sorted({h for _, h in cases})
check_mapped(ttr_regions, 'Register')
missing = sorted(set(ttr_regions) - {h for h, _, _ in population})
if missing:
    raise SystemExit(f"No Statistics Finland population for register regions {missing}")
person_years = {h: sum(v for (hh, _, _), v in population.items() if hh == h) for h in ttr_regions}
columns = ['hyvinvointialue', 'maakunta', 'person_years']
for col in DISEASES:
    columns += [f'{col}_cases', f'{col}_expected', f'{col}_incidence_index', f'{col}_crude_index']
rows = []
for h in sorted(ttr_regions, key=lambda h: (HVA_TO_MAAKUNTA[h], h)):
    row = [h, HVA_TO_MAAKUNTA[h], int(person_years[h])]
    for lab in DISEASES.values():
        observed = cases.get((lab, h), 0)
        # indirect standardisation: expected = sum over age band x sex of the region's person-years x national rate
        expected = sum(v * rate.get((lab, b, s), 0) for (hh, b, s), v in population.items() if hh == h)
        national_rate = sum(cases.get((lab, r), 0) for r in ttr_regions) / sum(person_years.values())
        row += [int(observed), round(expected, 1), round(100 * observed / expected, 1),
                round(100 * observed / person_years[h] / national_rate, 1)]
    rows.append(row)
out = HERE / 'data' / f'thl_ttr_incidence_{args.first_year}_{args.last_year}.tsv'
write_tsv(out, columns, rows)
print(f"Wrote {out} ({len(rows)} hyvinvointialue, {args.first_year}-{args.last_year}; diseases: "
      + ", ".join(f"{c} ({l})" for c, l in DISEASES.items()) + ")")
