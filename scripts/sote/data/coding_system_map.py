"""Map each CODING_SYSTEM OID of an engine output to its organisation and region, via THL's sote register.

Step 1, counts (cached): the distinct CODING_SYSTEM values of the engine output with their row counts and the
engine's own organisation name (CODING_SYSTEM_MAP), keeping values with more than --min-count rows, are written to
<out>/coding_system_counts.tsv (columns CODING_SYSTEM, CODING_SYSTEM_MAP, CNT). When that file exists the engine
output isn't read again (--force recounts).

Step 2, mapping, per coding system, in this order:
  1. not_org        NA and THL's national code lists (1.2.246.537.6.*): no organisation.
  2. CGI-hosted systems (1.2.246.537.10.3575029.12.2.10.<customer>.6.17): the customer is "11" + its Y-tunnus digits;
     a customer number of another form can't be decoded -> cgi_undecoded (no organisation). Checked before the
     generic OID rule, which would otherwise return CGI itself (code 3575029).
  3. Other OIDs: the code is the arc after 1.2.246.10. / 1.2.246.537.10. (Y-tunnus digits, leading zeros
     stripped; old-form 7-digit codes completed with their check digit, as in kanta.engine). The org node is the
     OID minus its trailing .6.<n>.<year> / .<year>, when it names a register unit (<code>.10.<...>).
  Lookup of the code, first hit wins:
     node      the org node is itself a register OrganizationId (e.g. a city's lab unit, <code>.10.1.1): the unit
               gives the location, the organisation's own entry (as below) the name
     org_root  the register entry 1.2.246.10.<code>.10.0 (else that code's top-level entry)
     ytunnus   the register's A:Y-Tunnus column: organisations registered under an older OID code, e.g.
               Keski-Uudenmaan hyvinvointialue (OID code 28449694, Y-tunnus 3221340-6)
     manual    MANUAL_ORGS below: organisations missing from the register
     not_found nothing matched
Region of the resolved organisation (region_reason): `regional` only if it is public (A:Sektori 1), not a lab
serving several regions (MULTI_REGION_LABS), and its municipality (A:Sijainti kunta) maps to a maakunta via
municipality_to_region.tsv (municipality_history.tsv for merged municipalities: maakunta only). Otherwise
multi_region_lab / not_public / no_municipality, or the lookup outcome when unresolved.

Writes <out>/coding_system_map.tsv, one row per coding system; copied to src/kanta/engine/data/coding_system_map.tsv,
it is the table kanta.engine maps CODING_SYSTEM with (to CODING_SYSTEM_MAP and CODING_SYSTEM_MAAKUNTA):
    CODING_SYSTEM  CNT  engine_org  code  how  sote_id  org_name  unit_name  sector  municipality  maakunta  hva
    region_reason
engine_org is the name the engine output being counted gave (CODING_SYSTEM_MAP), kept for comparison.

    python3 data/coding_system_map.py --input <engine_output>.parquet [--out DIR] [--min-count 5] [--force]
"""
import argparse, csv, gzip, re
from collections import defaultdict
from pathlib import Path

DATA = Path(__file__).resolve().parent  # scripts/sote/data
SOTE = DATA / 'sote.txt.gz'  # THL sote organisation register (';'-delimited, ISO-8859-1)
MUNI_REGION = DATA / 'municipality_to_region.tsv'
MUNI_HISTORY = DATA / 'municipality_history.tsv'

NOT_ORG = re.compile(r'^(NA|1\.2\.246\.537\.6\..*)$')
CGI = re.compile(r'^1\.2\.246\.537\.10\.3575029\.12\.2\.10\.(\d+)\.6\.17$')
CGI_CUSTOMER = re.compile(r'^110?(\d{7,8})$')
OID_CODE = re.compile(r'^1\.2\.246\.(?:537\.)?10\.(\d+)')
# org node = root + register unit path (.10.<n>[.<n>...]), shortest one leaving only a code list suffix
ORG_NODE = re.compile(r'^(1\.2\.246\.(?:537\.)?10\.\d+\.10(?:\.\d+)+?)(?:\.6\.\d+)?(?:\.(?:19|20)\d\d)?$')

MULTI_REGION_LABS = {'24838684': 'NordLab', '21261066': 'ISLAB'}
# code -> (name, sector, maakunta): organisations the register doesn't have (dissolved kuntayhtymät reached through
# CGI, a few private companies). Private ones need no maakunta: they don't get a region anyway.
MANUAL_ORGS = {
    '19025988': ('Sastamalan perusturvakuntayhtymä', '1', 'Pirkanmaa'),
    '1577530': ('Etelä-Pirkanmaan terveydenhuollon kuntayhtymä', '1', 'Pirkanmaa'),
    '9335990': ('Sisä-Savon seutuyhtymä', '1', 'Pohjois-Savo'),
    '26174551': ('Pihlajalinna Oyj', '2', None),
    '6538773': ('Pihlajalinna Etelä-Savo Oy', '2', None),
    '26771567': ('Negen Oy', '2', None),
}
COUNT_COLS = ['CODING_SYSTEM', 'CODING_SYSTEM_MAP', 'CNT']
OUT_COLS = ['CODING_SYSTEM', 'CNT', 'engine_org', 'code', 'how', 'sote_id', 'org_name', 'unit_name', 'sector',
            'municipality', 'maakunta', 'hva', 'region_reason']

ENGINE_DIR = Path('/mnt/disks/data/kanta/engine')


def latest_engine_output(engine_dir=ENGINE_DIR):
    """Most recent engine output: runs sit in <engine_dir>/<prefix>_<YYYY_MM_DD>/<prefix>_<YYYY_MM_DD>.parquet; the one
    with the latest date in its folder name (ties: the newest file), or None if there is none."""
    runs = [(d.name[-10:], (d / f'{d.name}.parquet').stat().st_mtime, d / f'{d.name}.parquet')
            for d in engine_dir.glob('*_[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9]') if (d / f'{d.name}.parquet').is_file()]
    return max(runs)[2] if runs else None


ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', type=Path, default=latest_engine_output(),
                help="Engine output parquet (CODING_SYSTEM or CODING_SYSTEM_OID column; only read when the counts cache "
                     f"is missing or with --force) (default: the most recent run in {ENGINE_DIR}, now %(default)s)")
ap.add_argument('--min-count', type=int, default=5, help="Keep coding systems with more than this many rows (default: 5)")
ap.add_argument('--out', type=Path, default=Path('.'),
                help="Output directory: the counts cache coding_system_counts.tsv and coding_system_map.tsv go there "
                     "(default: current directory)")
ap.add_argument('--force', action='store_true', help="Recount from --input even if the counts cache exists")
args = ap.parse_args()
args.out.mkdir(parents=True, exist_ok=True)
counts_file = args.out / 'coding_system_counts.tsv'
map_file = args.out / 'coding_system_map.tsv'


def ytunnus_check_digit(body):
    """Check digit of a 7-digit Y-tunnus body (weights 7,9,10,5,8,4,2, mod 11); None if no valid one exists."""
    remainder = sum(int(d) * w for d, w in zip(body, (7, 9, 10, 5, 8, 4, 2))) % 11
    return None if remainder == 1 else (0 if remainder == 0 else 11 - remainder)


def norm_code(code):
    """Org code as in kanta.engine: old-form 7-digit codes ("0132239") completed with their check digit, then
    leading zeros stripped ("1322394")."""
    if len(code) == 7 and code.startswith('0') and code.isdigit():
        check = ytunnus_check_digit(code)
        if check is not None:
            code += str(check)
    return code.lstrip('0')


# --- Step 1: coding system counts, cached
if args.force or not counts_file.exists():
    if args.input is None:
        raise SystemExit(f"{counts_file} not found and no engine output in {ENGINE_DIR}: pass --input <engine_output>.parquet")
    print(f"Counting coding systems in {args.input}")
    import duckdb
    con = duckdb.connect()
    P = f"read_parquet('{args.input}')"
    cols = set(con.sql(f"select * from {P} limit 0").columns)
    cs_col = next((c for c in ('CODING_SYSTEM', 'CODING_SYSTEM_OID') if c in cols), None)
    if cs_col is None:
        raise SystemExit(f"{args.input}: no CODING_SYSTEM / CODING_SYSTEM_OID column")
    org_col = next((c for c in ('CODING_SYSTEM_MAP', 'CODING_SYSTEM_ORG') if c in cols), None)
    org_expr = f'coalesce("{org_col}", \'NA\')' if org_col else "'NA'"
    counts = con.sql(f"""select coalesce("{cs_col}", 'NA') cs, {org_expr} org, count(*)::bigint n from {P}
                         group by all having count(*) > {args.min_count} order by n desc, cs""").fetchall()
    with open(counts_file, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f, delimiter='\t', lineterminator='\n')
        w.writerow(COUNT_COLS)
        w.writerows(counts)
    print(f"Wrote {counts_file}: {len(counts):,} (coding system, engine org) pairs with > {args.min_count} rows")
else:
    with open(counts_file, encoding='utf-8') as f:
        counts = [(r['CODING_SYSTEM'], r.get('CODING_SYSTEM_MAP') or 'NA', int(r['CNT']))
                  for r in csv.DictReader(f, delimiter='\t')]
    counts = [c for c in counts if c[2] > args.min_count]
    print(f"Loaded {counts_file} (--force to recount): {len(counts):,} pairs with > {args.min_count} rows")

# one row per coding system: rows summed, the engine's name with the most rows kept
per_cs = defaultdict(lambda: [0, None, 0])
for cs, org, n in counts:
    p = per_cs[cs]
    p[0] += n
    if n > p[2]:
        p[1], p[2] = org, n

# --- Reference tables
with open(MUNI_REGION, encoding='utf-8') as f:
    muni = {r['MunicipalityCode']: (r['Maakunta'], r['Hyvinvointialue']) for r in csv.DictReader(f, delimiter='\t')}
muni_old = {}
if MUNI_HISTORY.exists():
    with open(MUNI_HISTORY, encoding='utf-8') as f:
        muni_old = {r['MunicipalityCode']: r['Maakunta'] for r in csv.DictReader(f, delimiter='\t')}
hvas = defaultdict(set)
for mk, hva in muni.values():
    hvas[mk].add(hva)
mk_hva = {mk: next(iter(h)) for mk, h in hvas.items() if len(h) == 1}  # all but Uusimaa

# register entries: (OrganizationId, LongName, sector digit, municipality code); per code / Y-tunnus the .10.0 root
# entry is preferred, then the lowest hierarchy level
by_id, by_code, by_yt = {}, {}, {}


def rank(r):
    lvl = r['HierarchyLevel']
    return (not r['OrganizationId'].endswith('.10.0'), int(lvl) if lvl.isdigit() else 99)


def keep_best(index, key, r, entry):
    if key and (key not in index or rank(r) < index[key][0]):
        index[key] = (rank(r), entry)


with gzip.open(SOTE, 'rt', encoding='iso-8859-1') as f:
    for r in csv.DictReader(f, delimiter=';'):
        oid = r['OrganizationId']
        name = (r['LongName'] or '').strip() or r['Abbreviation']  # full name ("Pohjois-Savon hyvinvointialue", not "... hva")
        entry = (oid, name, (r['A:Sektori'] or ' ')[0], (r['A:Sijainti kunta'] or '').split(' ')[0])
        by_id[oid] = entry
        m = OID_CODE.match(oid)
        keep_best(by_code, m and norm_code(m.group(1)), r, entry)
        yt = (r['A:Y-Tunnus'] or '').replace('-', '').strip()
        keep_best(by_yt, yt.isdigit() and yt.lstrip('0'), r, entry)


def resolve(cs):
    """(code, how, register entry or manual tuple) for one coding system."""
    if NOT_ORG.match(cs):
        return None, 'not_org', None
    node = None
    m = CGI.match(cs)
    if m:
        c = CGI_CUSTOMER.match(m.group(1))
        if not c:
            return 'CGI:' + m.group(1), 'cgi_undecoded', None
        code = c.group(1).lstrip('0')
    else:
        m = OID_CODE.match(cs)
        if not m:
            return None, 'not_found', None
        code = norm_code(m.group(1))
        n = ORG_NODE.match(cs)
        node = n and n.group(1)
    if node in by_id:
        return code, 'node', by_id[node]  # the unit; its organisation's name is looked up below
    if code in by_code:
        return code, 'org_root', by_code[code][1]
    if code in by_yt:
        return code, 'ytunnus', by_yt[code][1]
    if code in MANUAL_ORGS:
        return code, 'manual', MANUAL_ORGS[code]
    return code, 'not_found', None


def region(code, sector, mcode, maakunta=None):
    """(maakunta, hva, region_reason) of a resolved organisation."""
    if code in MULTI_REGION_LABS:
        return None, None, 'multi_region_lab'
    if sector != '1':
        return None, None, 'not_public'
    if maakunta:
        return maakunta, mk_hva.get(maakunta), 'regional'
    if mcode in muni:
        return (*muni[mcode], 'regional')
    if mcode in muni_old:
        return muni_old[mcode], None, 'regional'
    return None, None, 'no_municipality'


# --- Step 2: mapping
out = []
for cs, (n, engine_org, _) in sorted(per_cs.items(), key=lambda kv: (-kv[1][0], kv[0])):
    code, how, hit = resolve(cs)
    row = dict.fromkeys(OUT_COLS)
    row.update(CODING_SYSTEM=cs, CNT=n, engine_org=engine_org, code=code, how=how)
    if how == 'manual':
        name, sector, mk = hit
        row.update(org_name=name, sector=sector)
        row['maakunta'], row['hva'], row['region_reason'] = region(code, sector, None, mk)
    elif hit:
        oid, name, sector, mcode = hit
        if how == 'node':  # name the organisation, not its unit (e.g. Espoon kaupunki, not its sote department)
            org = by_code.get(code) or by_yt.get(code)
            row['unit_name'], name = name, org[1][1] if org else name
        row.update(sote_id=oid, org_name=name, sector=sector, municipality=mcode)
        row['maakunta'], row['hva'], row['region_reason'] = region(code, sector, mcode)
    else:
        row['region_reason'] = how
    out.append(row)

with open(map_file, 'w', encoding='utf-8', newline='') as f:
    w = csv.writer(f, delimiter='\t', lineterminator='\n')
    w.writerow(OUT_COLS)
    w.writerows([['' if r[c] is None else r[c] for c in OUT_COLS] for r in out])
print(f"Wrote {map_file}: {len(out):,} coding systems")

# --- Summary, over rows of coding systems that name an organisation
org_rows = [r for r in out if r['how'] != 'not_org']
tot = sum(r['CNT'] for r in org_rows) or 1
print(f"\n{sum(r['CNT'] for r in out):,} rows, {tot:,} of them with an organisation coding system")
for key in ('how', 'region_reason'):
    agg = defaultdict(lambda: [0, 0])
    for r in org_rows:
        agg[r[key]][0] += 1
        agg[r[key]][1] += r['CNT']
    print(f"\nBy {key}:")
    for k, (k_n, rows) in sorted(agg.items(), key=lambda kv: -kv[1][1]):
        print(f"  {k:<18}{k_n:>6} coding systems {rows:>14,} rows {100 * rows / tot:6.2f}%")
missing = defaultdict(lambda: [0, None])
for r in org_rows:
    if r['how'] in ('not_found', 'cgi_undecoded'):
        missing[r['code']][0] += r['CNT']
        missing[r['code']][1] = r['engine_org']
if missing:
    print("\nUnresolved codes (top 15), with the engine's name:")
    for code, (rows, name) in sorted(missing.items(), key=lambda kv: -kv[1][0])[:15]:
        print(f"  {str(code):<16}{rows:>12,}  {name}")
