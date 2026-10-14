"""Link each SERVICE_PROVIDER (SP) to a region via the coding systems (CODING_SYSTEM) its rows carry.

Input is the engine output parquet. Writes to --out:
- sp_x_codingsystem.tsv       SP x CODING_SYSTEM row counts
- sp_top_codingsystem_map.tsv per SP: top CODING_SYSTEM_MAP org among mapped rows, its share, next 3
- sp_best_org.tsv             per SP: dominant org code (from the OID) and its name
- cs_region.tsv               per CODING_SYSTEM: org code, maakunta/hyvinvointialue, and why (reason)
- sp_region.tsv               the SP -> region link: top maakunta/hyvinvointialue per SP with purity

Region of a coding system: OID -> org code -> sote.txt (sector, municipality) -> municipality_to_region.tsv.
Only public orgs located in one municipality vote; private orgs and multi-region public labs don't.
"""
import argparse, csv, gzip, json, re
from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
SOTE = REPO / 'scripts/THL/data/sote.txt.gz'  # THL sote organization register (';'-delimited, ISO-8859-1)
MUNI_REGION = REPO / 'scripts/THL/data/municipality_to_region.tsv'
SOTE_NAMED = REPO / 'src/kanta/engine/data/thl_sote_map_named.tsv.gz'  # engine's OID -> LAB_NAME map
MULTI_REGION_PUBLIC_LABS = {'24838684': 'NordLab', '21261066': 'ISLAB'}
LEVELS = {'maakunta': 'Maakunta', 'hva': 'Hyvinvointialue'}  # output prefix -> municipality_to_region.tsv column

ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', required=True, type=Path, help="Engine output parquet (needs SERVICE_PROVIDER, CODING_SYSTEM, CODING_SYSTEM_MAP)")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/service-provider'), help="Output directory")
args = ap.parse_args()
args.out.mkdir(parents=True, exist_ok=True)


def write_tsv(df, path):
    """Plain TSV, no quoting (pandas' to_csv would quote the JSON columns)."""
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\t'.join(df.columns) + '\n')
        for row in df.itertuples(index=False):
            f.write('\t'.join('' if pd.isna(v) else str(v) for v in row) + '\n')
    print(f"Wrote {path}")


def top_with_next3(counts, key, value):
    """Per `key`: total, top `value` and its count/%, plus the next 3 values' % as JSON."""
    recs = {}
    for k, g in counts.sort_values([key, 'n', value], ascending=[True, False, True]).groupby(key, sort=False):
        tot = int(g['n'].sum())
        top = g.iloc[0]
        recs[k] = {'n': tot, 'top': top[value], 'n_top': int(top['n']), 'pct_top': round(100 * top['n'] / tot, 2),
                   'next3': json.dumps({r[value]: round(100 * r['n'] / tot, 2) for _, r in g.iloc[1:4].iterrows()}, ensure_ascii=False)}
    return recs


# --- Read the engine output: one pass, everything below derives from `agg`
con = duckdb.connect()
P = f"read_parquet('{args.input}')"
missing = {'SERVICE_PROVIDER', 'CODING_SYSTEM', 'CODING_SYSTEM_MAP'} - set(con.sql(f"select * from {P} limit 0").columns)
if missing:
    raise SystemExit(f"{args.input}: not an engine output file, missing columns {sorted(missing)}")
agg = con.sql(f"select SERVICE_PROVIDER s, CODING_SYSTEM cs, CODING_SYSTEM_MAP csm, count(*)::bigint n from {P} group by all").df()
x = agg.groupby(['s', 'cs'], dropna=False, as_index=False)['n'].sum()
write_tsv(x.sort_values('n', ascending=False), args.out / 'sp_x_codingsystem.tsv')
n_total = agg.groupby('s', dropna=False)['n'].sum()

# --- SP -> top CODING_SYSTEM_MAP org (the engine's resolved name), among mapped rows
csm = agg[agg['csm'].notna() & (agg['csm'] != 'NA')].groupby(['s', 'csm'], as_index=False)['n'].sum()
top_csm = top_with_next3(csm, 's', 'csm')
rows = []
for s, tot in n_total.items():
    t = top_csm.get(s)
    rows.append({'s': s, 'n_total': int(tot), 'n_mapped': t['n'] if t else 0,
                 'pct_na': round(100 * (1 - (t['n'] if t else 0) / tot), 2),
                 'top_csm': t and t['top'], 'n_top': t and t['n_top'], 'pct_top': t and t['pct_top'],
                 'next3_csm': t['next3'] if t else '{}'})
sp_csm = pd.DataFrame(rows).astype({'n_top': 'Int64'}).sort_values(['n_total', 'pct_top'], ascending=False, na_position='last')
write_tsv(sp_csm, args.out / 'sp_top_codingsystem_map.tsv')

# --- Reference tables: org code -> (name, sector, municipality), municipality -> regions
muni = {r['MunicipalityCode']: r for r in csv.DictReader(open(MUNI_REGION, encoding='utf-8'), delimiter='\t')}
orgs = {}  # level-0 entry preferred
sote_rows = csv.DictReader(gzip.open(SOTE, 'rt', encoding='iso-8859-1'), delimiter=';')
for r in sorted(sote_rows, key=lambda r: r['HierarchyLevel'] != '0'):
    m = re.match(r'1\.2\.246\.(?:537\.)?10\.0*(\d+)\.', r['OrganizationId'])
    if m and m.group(1) not in orgs:
        orgs[m.group(1)] = (r['Abbreviation'], r['A:Sektori'], r['A:Sijainti kunta'].split(' ')[0])
# the engine replaces CODING_SYSTEM OIDs found in the sote map with their LAB_NAME (e.g. "Eura_1"): map those back
labname = {r['LAB_NAME']: r['OrganizationId'] for r in csv.DictReader(gzip.open(SOTE_NAMED, 'rt', encoding='utf-8'), delimiter='\t')}


def org_code(cs):
    """Org code (Y-tunnus digits, no leading zeros) embedded in a CODING_SYSTEM value, or None."""
    if cs is None or pd.isna(cs) or cs in ('', 'NA'):
        return None
    cs = labname.get(cs, cs)
    # CGI-hosted systems embed the customer's Y-tunnus ("11" + 8 digits); the engine decodes it only into
    # CODING_SYSTEM_MAP, CODING_SYSTEM keeps the raw OID
    m = re.match(r'^1\.2\.246\.537\.10\.3575029\.12\.2\.10\.110?(\d{7,8})\.6\.17$', cs)
    if m:
        return m.group(1).lstrip('0')
    if '.3575029.' in cs:
        return 'CGI'
    m = re.match(r'1\.2\.246\.(?:537\.)?10\.0*(\d+)', cs)
    return m.group(1) if m else None


def classify(cs):
    code = org_code(cs)
    if code is None: return None, 'no_org', None
    if code == 'CGI': return None, 'cgi_undecoded', None
    if code in MULTI_REGION_PUBLIC_LABS: return code, 'multi_region_lab', None
    if code not in orgs: return code, 'not_in_sote', None
    _, sector, muni_code = orgs[code]
    if not sector.startswith('1'): return code, 'private', None
    if muni_code not in muni: return code, 'no_municipality', None
    return code, 'regional', muni[muni_code]


# --- Per coding system: org code, region, reason
cs_n = x.groupby('cs', dropna=False)['n'].sum()
cs_rows = []
for cs, n in cs_n.items():
    code, reason, region = classify(cs)
    cs_rows.append({'cs': cs, 'org': code, 'orgname': orgs[code][0] if code in orgs else None, 'reason': reason,
                    **{lvl: region[col] if region else None for lvl, col in LEVELS.items()}, 'n': int(n)})
cs_region = pd.DataFrame(cs_rows)
write_tsv(cs_region.sort_values('n', ascending=False), args.out / 'cs_region.tsv')
print("\nRows by reason:")
print(cs_region.groupby('reason')['n'].sum().sort_values(ascending=False).map('{:,}'.format).to_string())

# --- SP -> dominant org code (any reason), with purity both ways
xo = x.merge(cs_region[['cs', 'org']], on='cs', how='left').dropna(subset=['org'])
xo = xo.groupby(['s', 'org'], as_index=False)['n'].sum()
xo['frac_s'] = xo['n'] / xo.groupby('s')['n'].transform('sum')
xo['frac_o'] = xo['n'] / xo.groupby('org')['n'].transform('sum')
xo['tot'] = xo.groupby('s')['n'].transform('sum')
best = xo.sort_values(['s', 'n', 'org'], ascending=[True, False, True]).groupby('s').head(1)
best.insert(2, 'orgname', best['org'].map(lambda c: orgs[c][0] if c in orgs else None))
write_tsv(best[['s', 'org', 'orgname', 'tot', 'frac_s', 'frac_o']].sort_values('tot', ascending=False), args.out / 'sp_best_org.tsv')

# --- SP -> region: only 'regional' coding systems vote; purity is over those voting rows
xr = x.merge(cs_region[cs_region['reason'] == 'regional'][['cs', *LEVELS]], on='cs')
sp_region = sp_csm[['s', 'n_total', 'top_csm', 'pct_top']].copy()
for lvl in LEVELS:
    t = top_with_next3(xr.groupby(['s', lvl], as_index=False)['n'].sum(), 's', lvl)
    get = lambda key: sp_region['s'].map(lambda s: t[s][key] if s in t else None)
    if lvl == 'maakunta':
        sp_region['n_region'] = get('n').astype('Int64')
        sp_region['pct_region_of_total'] = (100 * sp_region['n_region'] / sp_region['n_total']).round(2)
    sp_region[lvl], sp_region[f'pct_{lvl}'], sp_region[f'next3_{lvl}'] = get('top'), get('pct_top'), get('next3')
write_tsv(sp_region, args.out / 'sp_region.tsv')

# --- Coverage: share of all rows whose SP gets a maakunta, by purity and minimum voting rows
tot = int(n_total.sum())
print(f"\nCoverage of all {tot:,} rows by SPs assigned a maakunta:")
for min_n in (0, 100, 1000):
    for thr in (0, 90, 99):
        ok = sp_region[(sp_region['n_region'].fillna(0) >= max(min_n, 1)) & (sp_region['pct_maakunta'] >= thr)]
        print(f"  n_region>={min_n:>4}  pct_maakunta>={thr:>2}: {len(ok):5d} SPs, {ok['n_total'].sum():>13,} rows ({100 * ok['n_total'].sum() / tot:5.1f}%)")
print("\nTop 30 SPs:")
print(sp_region.head(30).drop(columns=['next3_hva']).to_string(max_colwidth=40))
