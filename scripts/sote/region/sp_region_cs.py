"""Link each SERVICE_PROVIDER (SP) to a maakunta via the coding systems (CODING_SYSTEM) its rows carry.

Each row's coding system gives the maakunta of the organisation owning it, CODING_SYSTEM_MAAKUNTA in the engine
output (looked up in src/kanta/engine/data/coding_system_map.tsv, built from THL's sote register by
scripts/sote/data/coding_system_map.py). Only about a third of rows have one: the rest carry no coding system (NA),
THL's national code list, or a coding system of a private organisation or of a lab serving several regions. So per
SP, its rows with a maakunta vote, and the SP gets the top maakunta with its purity; region_people.py then uses it
for all the SP's rows, including those without a maakunta of their own.

Input is the engine output parquet. Writes to --out:
- sp_region.tsv          the SP -> maakunta link: top maakunta per SP with its purity (pct_maakunta, over the SP's
                         rows with a maakunta) and the next 3; also the SP's top CODING_SYSTEM_MAP organisation
- sp_x_codingsystem.tsv  SP x CODING_SYSTEM row counts, for diagnostics (e.g. the SPs behind an unmapped coding system)
"""
import argparse, json
from pathlib import Path
import duckdb
import pandas as pd

ENGINE_DIR = Path('/mnt/disks/data/kanta/engine')


def latest_engine_output(engine_dir=ENGINE_DIR):
    """Most recent engine output: runs sit in <engine_dir>/<prefix>_<YYYY_MM_DD>/<prefix>_<YYYY_MM_DD>.parquet; the one
    with the latest date in its folder name (ties: the newest file), or None if there is none."""
    runs = [(d.name[-10:], (d / f'{d.name}.parquet').stat().st_mtime, d / f'{d.name}.parquet')
            for d in engine_dir.glob('*_[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]') if (d / f'{d.name}.parquet').is_file()]
    return max(runs)[2] if runs else None


ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', type=Path, default=latest_engine_output(),
                help="Engine output parquet (needs SERVICE_PROVIDER, CODING_SYSTEM, CODING_SYSTEM_MAP, CODING_SYSTEM_MAAKUNTA) "
                     f"(default: the most recent run in {ENGINE_DIR}, now %(default)s)")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/service-provider/region'), help="Output directory")
args = ap.parse_args()
if args.input is None:
    raise SystemExit(f"No engine output found in {ENGINE_DIR}: pass --input <engine_output>.parquet")
print(f"Input: {args.input}")
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
missing = {'SERVICE_PROVIDER', 'CODING_SYSTEM', 'CODING_SYSTEM_MAP', 'CODING_SYSTEM_MAAKUNTA'} - cols
if missing:
    raise SystemExit(f"{args.input}: missing columns {sorted(missing)} (CODING_SYSTEM_MAAKUNTA: engine outputs from "
                     f"2026-11-05 on)")
agg = con.sql(f"""select SERVICE_PROVIDER s, CODING_SYSTEM cs, CODING_SYSTEM_MAP csm, CODING_SYSTEM_MAAKUNTA mk,
                  count(*)::bigint n from {P} group by all""").df()

x = agg.groupby(['s', 'cs'], dropna=False, as_index=False)['n'].sum()
write_tsv(x.sort_values('n', ascending=False), args.out / 'sp_x_codingsystem.tsv')
n_total = agg.groupby('s', dropna=False)['n'].sum()

# --- SP -> top CODING_SYSTEM_MAP organisation (the engine's name), among rows with one
csm = agg[is_set(agg['csm'])].groupby(['s', 'csm'], as_index=False)['n'].sum()
top_csm = top_with_next3(csm, 's', 'csm')
sp_region = pd.DataFrame([{'s': s, 'n_total': int(tot), 'top_csm': top_csm[s]['top'] if s in top_csm else None,
                           'pct_top': top_csm[s]['pct_top'] if s in top_csm else None} for s, tot in n_total.items()])
sp_region = sp_region.sort_values(['n_total', 'pct_top'], ascending=False, na_position='last')

# --- SP -> maakunta: rows with a maakunta vote; purity is over those rows
votes = top_with_next3(agg[is_set(agg['mk'])].groupby(['s', 'mk'], as_index=False)['n'].sum(), 's', 'mk')
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
