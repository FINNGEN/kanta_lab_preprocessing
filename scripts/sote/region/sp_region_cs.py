"""Link each SERVICE_PROVIDER (SP) to a maakunta via the coding systems (CODING_SYSTEM) its rows carry.

Each row's coding system gives the maakunta of the organisation owning it, CODING_SYSTEM_MAAKUNTA in the engine
output (looked up in src/kanta/engine/data/coding_system_map.tsv, built from THL's sote register by
scripts/sote/data/coding_system_map.py). Only about a third of rows have one: the rest carry no coding system (NA),
THL's national code list, or a coding system of a private organisation or of a lab serving several regions. So per
SP, its rows with a maakunta vote, and the SP gets the top maakunta with its purity; region_people.py then uses it
for all the SP's rows, including those without a maakunta of their own.

Engine outputs from before CODING_SYSTEM_MAAKUNTA existed are handled by joining the table on CODING_SYSTEM instead
(exact OIDs only: the few coding systems the table doesn't list get no maakunta).

Input is the engine output parquet. Writes to --out:
- sp_x_codingsystem.tsv       SP x CODING_SYSTEM row counts (e.g. to find the SPs behind an unmapped coding system)
- sp_top_codingsystem_map.tsv per SP: top CODING_SYSTEM_MAP organisation among rows with one, its share, next 3
- sp_region.tsv               the SP -> maakunta link: top maakunta per SP with its purity (pct_maakunta, over the
                              SP's rows with a maakunta) and the next 3
"""
import argparse, json
from pathlib import Path
import duckdb
import pandas as pd

CS_MAP = Path(__file__).resolve().parents[3] / 'src' / 'kanta' / 'engine' / 'data' / 'coding_system_map.tsv'

ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', required=True, type=Path,
                help="Engine output parquet (needs SERVICE_PROVIDER, CODING_SYSTEM, CODING_SYSTEM_MAP; "
                     "CODING_SYSTEM_MAAKUNTA if present, else --cs-map is joined on CODING_SYSTEM)")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/service-provider/region'), help="Output directory")
ap.add_argument('--cs-map', type=Path, default=CS_MAP,
                help="Coding system table, used when the input lacks CODING_SYSTEM_MAAKUNTA (default: %(default)s)")
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


def is_set(s):
    return s.notna() & (s != 'NA') & (s != '')


# --- Read the engine output: one pass, everything below derives from `agg`
con = duckdb.connect()
P = f"read_parquet('{args.input}')"
cols = set(con.sql(f"select * from {P} limit 0").columns)
missing = {'SERVICE_PROVIDER', 'CODING_SYSTEM', 'CODING_SYSTEM_MAP'} - cols
if missing:
    raise SystemExit(f"{args.input}: not an engine output file, missing columns {sorted(missing)}")
has_mk = 'CODING_SYSTEM_MAAKUNTA' in cols
mk_expr = 'CODING_SYSTEM_MAAKUNTA' if has_mk else 'null::varchar'
agg = con.sql(f"""select SERVICE_PROVIDER s, CODING_SYSTEM cs, CODING_SYSTEM_MAP csm, {mk_expr} mk, count(*)::bigint n
                  from {P} group by all""").df()
if not has_mk:
    if not args.cs_map.exists():
        raise SystemExit(f"{args.input} has no CODING_SYSTEM_MAAKUNTA and {args.cs_map} isn't there (--cs-map)")
    cs_map = pd.read_csv(args.cs_map, sep='\t', usecols=['CODING_SYSTEM', 'maakunta'], dtype=str, keep_default_na=False)
    agg['mk'] = agg['cs'].map(dict(zip(cs_map['CODING_SYSTEM'], cs_map['maakunta'])))
    print(f"No CODING_SYSTEM_MAAKUNTA in {args.input.name}: maakunta from {args.cs_map}")

x = agg.groupby(['s', 'cs'], dropna=False, as_index=False)['n'].sum()
write_tsv(x.sort_values('n', ascending=False), args.out / 'sp_x_codingsystem.tsv')
n_total = agg.groupby('s', dropna=False)['n'].sum()

# --- SP -> top CODING_SYSTEM_MAP organisation (the engine's name), among rows with one
csm = agg[is_set(agg['csm'])].groupby(['s', 'csm'], as_index=False)['n'].sum()
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

# --- SP -> maakunta: rows with a maakunta vote; purity is over those rows
votes = top_with_next3(agg[is_set(agg['mk'])].groupby(['s', 'mk'], as_index=False)['n'].sum(), 's', 'mk')
sp_region = sp_csm[['s', 'n_total', 'top_csm', 'pct_top']].copy()
get = lambda key: sp_region['s'].map(lambda s: votes[s][key] if s in votes else None)
sp_region['n_region'] = get('n').astype('Int64')
sp_region['pct_region_of_total'] = (100 * sp_region['n_region'] / sp_region['n_total']).round(2)
sp_region['maakunta'], sp_region['pct_maakunta'], sp_region['next3_maakunta'] = get('top'), get('pct_top'), get('next3')
write_tsv(sp_region, args.out / 'sp_region.tsv')

# --- Coverage: rows with a maakunta of their own, and rows whose SP gets one (by purity, minimum voting rows)
tot = int(n_total.sum())
own = int(agg.loc[is_set(agg['mk']), 'n'].sum())
print(f"\n{tot:,} rows, {own:,} ({100 * own / tot:.1f}%) with a coding system maakunta of their own")
print(f"Coverage of all rows by SPs assigned a maakunta:")
for min_n in (0, 100, 1000):
    for thr in (0, 90, 99):
        ok = sp_region[(sp_region['n_region'].fillna(0) >= max(min_n, 1)) & (sp_region['pct_maakunta'] >= thr)]
        print(f"  n_region>={min_n:>4}  pct_maakunta>={thr:>2}: {len(ok):5d} SPs, {ok['n_total'].sum():>13,} rows ({100 * ok['n_total'].sum() / tot:5.1f}%)")
print("\nTop 30 SPs:")
print(sp_region.head(30).to_string(max_colwidth=40))
