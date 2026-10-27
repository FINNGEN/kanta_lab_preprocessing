#!/usr/bin/env python3
"""Reference-range texts -> numeric lower/upper limits, checked against the values, sex and age of the rows carrying them.

Step 1 (cached): one pass over the engine output or release, rows with a text only. The text is REFERENCE_RANGE_TEXT, else
REFERENCE_RANGE_GROUP (the same text with its spaces removed; it is filled on some rows without a text). Writes
(aggregates only; tabs/newlines inside a text become spaces; pairs with fewer than --min-n rows or --min-people people are dropped):
  --out/ref_text_table.tsv           one row per TEST_NAME x text: pair_id, test_name, ref_text, n (rows), pct (% of all rows
                                     with a text, before the filters), cum_pct, pct_group (% of the pair's rows whose text came
                                     from the group), people, pct_f (% women of the rows with a sex), n_value, v_mean,
                                     v_p05/p25/p50/p75/p95 (MEASUREMENT_VALUE: the unit the text is written in), age_p05/p50/p95
and the cache, in --tmp, named after the input file (<input>):
  ref_text_<input>_value_hist.parquet  pair_id, value (rounded to 3 significant figures), n
  ref_text_<input>_age_hist.parquet    pair_id, age (whole years; whole months under 2), n
  ref_text_<input>.json                what the cache was built from (input, --min-n, --min-people, --test, the table it goes with)
Step 1 is rerun only when any of those change (or the table is missing), or with --force. People are counted in
two plain group-bys (pair x person, then pair) and quantiles come from the histograms, so duckdb can spill to disk throughout.

Step 2 (always): each text is read as [prefix] limits [unit] [/qualifier | ,comment], case-insensitive, decimal commas accepted:
  a-b, a - b, a-<b                 -> lower a, upper b
  <x, <=x, =/<x, alle x, -<x, - x -> upper x (lower left empty)
  >x, >=x, =/>x, yli x, >x -, x -  -> lower x (upper left empty)
  [sex,] ala: a ylä: b             -> lower a, upper b
Sex and age written anywhere in the text are read first and taken out (sex_text F/M; age_lo/age_hi in years, age_hi
exclusive): naiset/nainen/N -> F, miehet/mies/M -> M; 17v-, yli 17 v, 14v-aik -> from 17 (14); alle 17 v, -17v -> under 17;
1-17 v -> 1 up to 18 (17 included); kk, vk/vko, vrk/pv convert to years; aikuiset/aik -> from 16, lapset -> under 16 (Finnish labs' adult age).
The prefix 'viitearvot:' is dropped. Units are optional (anything with a '/', or the start of a known unit word). Anything
else left over makes the text 'unparsed'.

Texts that are not a reference range are skipped before parsing, with the reason as status:
  skip_label (only sex/age/group: nainen, mies, aikuiset, yli15-vuotiaat), skip_target (tavoite..., treatment targets),
  skip_therapeutic (hoitotaso, ak-hoito, ter), skip_no_number (negatiivinen, NEGAT, lopullinen, ks.ohjekirja, ...)

Then the checks, from the step 1 histograms:
  value_check  value_ok when >= --value-min-pct of the values are inside the limits (patients sit partly outside a healthy
               range, so the median may too: med_in says whether it is); else scale_mismatch (lower > 0 and upper given: the
               middle half of the values, p25-p75, lands inside after x10^k, k = -3..3: probably a unit problem, factor in
               'scale'), else value_off; with pct_below/pct_in/pct_above
  sex_check    sex_ok when the text says F and >= --sex-min-pct of the rows are women (M: men), else sex_mismatch
  age_check    age_ok when >= --age-min-pct of the rows fall in [age_lo, age_hi) (an age bin counts when it overlaps it)
               and so does the median age (the bulk of the rows, not the tail), else age_mismatch; pct_age_in

Writes --out/ref_text_table_LIMITS.tsv: the table plus lower, upper, status (range / upper / lower / skip_* / unparsed /
bad_order when lower > upper), rest (what was not read, for unparsed texts), sex_text, age_lo, age_hi, the checks, and
flags (the failed ones: unparsed, bad_order, value_off, scale_mismatch, sex_mismatch, age_mismatch). The pairs with a
flag also go to --out/ref_text_table_FLAGGED.tsv (largest first, the columns needed to review them).
Prints the rows by status and check, and the top unparsed texts and failed checks.
"""
import argparse, json, os, re, time
from pathlib import Path
import duckdb
import pandas as pd

NUM = r'(?:\d+(?:\.\d+)?|\.\d+)'  # also .5
SNUM = rf'[-+]?{NUM}'  # signed, for ranges like -3 - 3 (base excess)
# a word that is the start of one of these is a unit (units often come truncated: 'nm', 'mmo', 'k'); age words
# (v, kk, vrk, pv, vk, vi, vko) start none of them, so an age is never taken as a unit
UNIT_WORDS = ('fl', 'pg', '%', 'kpa', 'mmol', 'umol', 'µmol', 'nmol', 'pmol', 'mg', 'ug', 'µg', 'ng', 'g', 'kg', 'sek',
              'min', 'osuus', 'mosm', 'mu', 'iu', 'u', 'ku', 'ratio', 'x', 'm2', 'm²', 'e6', 'e9', 'e12', 'titteri')
LABELS = {'nainen', 'naiset', 'mies', 'miehet', 'aikuiset', 'aik', 'lapset', 'kaikki', 'nnnnn'}
SKIP = [('skip_target', re.compile(r'^tav')),
        ('skip_therapeutic', re.compile(r'hoitotaso|ak-hoito|hoitoa|\bter\b|terap|varfariini'))]
PREFIX = re.compile(r'^viitearvot?:\s*')
SEP = r'\s*[-–]\s*'
PATTERNS = [  # (status, regex); groups: lower, upper as named
    ('range', re.compile(rf'^(?:\w+,\s*)?ala:\s*(?P<lo>{SNUM})\s*ylä:\s*(?P<hi>{SNUM})')),
    ('range', re.compile(rf'^(?P<lo>{SNUM}){SEP}<?\s*(?P<hi>{SNUM})')),
    ('upper', re.compile(rf'^(?:-\s*)?(?:<=?|=/?<|≤|alle)\s*(?P<hi>{NUM})')),
    ('upper', re.compile(rf'^-\s+(?P<hi>{NUM})')),
    ('lower', re.compile(rf'^(?:>=?|=/?>|≥|yli)\s*(?P<lo>{NUM})(?:\s*-(?!\s*\d))?')),
    ('lower', re.compile(rf'^(?P<lo>{NUM})\s*-(?!\s*[\d<])')),
]
# sex and age, searched anywhere in the text
SEX = {'F': re.compile(r'(?<!\w)nai\w*|^n(?=[\s:,])'), 'M': re.compile(r'(?<!\w)mie\w*|^m(?=[\s:,])')}
AGE_UNIT = r'(?:vrk|vko?|viik\w*|vuo\w*|v|kk|kuuk\w*|pv|päiv\w*)(?![a-zåäö])'
AU = rf'(?P<u>{AGE_UNIT})'
AGE_PATTERNS = [  # (kind, regex); a is the lower age, b the upper
    ('range', re.compile(rf'(?<![\d.])(?P<a>{NUM})\s*(?P<ua>{AGE_UNIT})?{SEP}(?P<b>{NUM})\s*-?\s*{AU}')),  # 1-17 v, 6kk-1v
    ('lower', re.compile(rf'(?:yli|>=?|≥)\s*(?P<a>{NUM})\s*-?\s*{AU}')),  # yli 17 v, yli15-vuotiaat
    ('upper', re.compile(rf'(?:alle|<=?|≤)\s*(?P<b>{NUM})\s*-?\s*{AU}')),  # alle 1 kk
    ('lower', re.compile(rf'(?<![\d.])(?P<a>{NUM})\s*{AU}\s*-(?:\s*aik\w*)?')),  # 17v-, 14v-aik
    ('upper', re.compile(rf'(?:^|(?<=[\s,/(]))-\s*(?P<b>{NUM})\s*{AU}')),  # -17v
]
AGE_WORDS = [(re.compile(r'(?<!\w)aik\w*'), 16, None), (re.compile(r'(?<!\w)laps\w*'), None, 16)]
OK = ['range', 'upper', 'lower']
FLAGS = ('unparsed', 'bad_order', 'value_off', 'scale_mismatch', 'sex_mismatch', 'age_mismatch')  # what puts a pair in _FLAGGED.tsv
FLAGGED_COLS = ['test_name', 'ref_text', 'n', 'flags', 'status', 'lower', 'upper', 'v_p05', 'v_p25', 'v_p50', 'v_p75', 'v_p95', 'pct_below', 'pct_in',
                'pct_above', 'scale', 'sex_text', 'pct_f', 'age_lo', 'age_hi', 'age_p05', 'age_p50', 'age_p95', 'pct_age_in', 'rest']

# short name -> (engine output column, release column)
COLUMNS = {
    'f': ('FINNGENID', 'FINNGENID'),
    'test_name': ('TEST_NAME_ABBREVIATION', 'TEST_NAME'),
    'ref_text': ('REFERENCE_RANGE_TEXT', 'REFERENCE_RANGE_TEXT'),
    'ref_group': ('REFERENCE_RANGE_GROUP', 'REFERENCE_RANGE_GROUP'),
    'sex': ('SEX', 'SEX'),
    'age': ('EVENT_AGE', 'EVENT_AGE'),
    'v': ('MEASUREMENT_VALUE', 'MEASUREMENT_VALUE_CLEANED'),
}
TEST_ROWS = 10_000_000
V_PROBS = {'v_p05': .05, 'v_p25': .25, 'v_p50': .5, 'v_p75': .75, 'v_p95': .95}
AGE_PROBS = {'age_p05': .05, 'age_p50': .5, 'age_p95': .95}


def years(unit):
    """Length of one age unit in years."""
    if unit.startswith(('kk', 'kuuk')):
        return 1 / 12
    if unit.startswith(('vk', 'viik')):
        return 7 / 365.25
    if unit.startswith(('vrk', 'pv', 'päiv')):
        return 1 / 365.25
    return 1


def groups(t):
    """Sex and age written in a (normalised) text -> (sex_text, age_lo, age_hi, the text without them)."""
    spans = []
    found = {s for s, r in SEX.items() if r.search(t)}
    sex = found.pop() if len(found) == 1 else None
    spans += [x.span() for r in SEX.values() for x in r.finditer(t)]
    lo = hi = None
    for kind, r in AGE_PATTERNS:
        x = r.search(t)
        if x:
            g, y = x.groupdict(), years(x['u'])
            lo = float(g['a']) * years(g.get('ua') or x['u']) if g.get('a') else None  # 6kk-1v: the first number's own unit
            hi = (float(g['b']) + (kind == 'range')) * y if g.get('b') else None
            spans.append(x.span())
            break
    for r, w_lo, w_hi in AGE_WORDS:
        for x in r.finditer(t):
            spans.append(x.span())
            if lo is None and hi is None:
                lo, hi = w_lo, w_hi
    keep = [True] * len(t)
    for a, b in spans:
        keep[a:b] = [False] * (b - a)
    s = ''.join(c if k else ' ' for c, k in zip(t, keep))
    s = re.sub(r'\s+', ' ', re.sub(r'\(\s*\)', ' ', s)).strip(' ,:;/')
    return sex, lo, hi, s


def is_unit(tok):
    return '/' in tok or any(u.startswith(tok) for u in UNIT_WORDS)


def tail_ok(rest):
    """True when what follows the limits is only units, then nothing, a /qualifier or a ,comment."""
    toks = rest.split()
    while toks and not toks[0].startswith(('/', ',')):
        tok = toks[0]
        if tok == '-' or is_unit(tok.rstrip(',')) or tok.rstrip(',') in LABELS:
            toks.pop(0)
            if tok.endswith(','):
                return True
        elif '/' in tok and is_unit(tok.split('/')[0] + '/'):
            return True
        else:
            return False
    return not toks or toks[0][0] in '/,'


def parse(text):
    """One reference text -> (lower, upper, status, rest, sex_text, age_lo, age_hi)."""
    t = re.sub(r'\s+', ' ', text.lower().replace('"', '')).strip()
    t = re.sub(r'(\d),(\d)', r'\1.\2', t)
    t = PREFIX.sub('', t)
    sex, age_lo, age_hi, s = groups(t)
    grp = (sex, age_lo, age_hi)
    if not s or s in LABELS:
        return None, None, 'skip_label', None, *grp
    for status, r in SKIP:
        if r.search(t):
            return None, None, status, None, *grp
    if not re.search(r'\d', s):
        return None, None, 'skip_no_number', None, *grp
    for status, r in PATTERNS:
        m = r.match(s)
        if m:
            if not tail_ok(s[m.end():].strip()):
                return None, None, 'unparsed', s, *grp
            g = m.groupdict()
            lo, hi = (float(g[k]) if g.get(k) is not None else None for k in ('lo', 'hi'))
            if lo is not None and hi is not None and lo > hi:
                return lo, hi, 'bad_order', None, *grp
            return lo, hi, status, None, *grp
    return None, None, 'unparsed', s, *grp


def build_table(con, args, files, log):
    """Step 1: the pair table and the value/age histograms, in one set of passes over the input."""
    src = f"(select * from read_parquet('{args.input}')" + (f" limit {TEST_ROWS})" if args.test else ')')
    cols = set(con.sql(f"select * from read_parquet('{args.input}') limit 0").columns)

    def name(short):
        c = next((c for c in COLUMNS[short] if c in cols), None)
        if c is None:
            raise SystemExit(f"{args.input}: missing column {COLUMNS[short][0]} / {COLUMNS[short][1]}")
        return f'"{c}"'

    def col(short):
        """The input's column as a cleaned string ("NA"/'' -> null)."""
        return f"nullif(nullif(trim(cast({name(short)} as varchar)), ''), 'NA')"

    def num(short):
        return f"try_cast({name(short)} as double)"

    text = f"coalesce({col('ref_text')}, {col('ref_group')})"
    con.sql(f"""create or replace temp view rows_ as
        select coalesce({col('test_name')}, 'NA') as test_name, regexp_replace({text}, '[\\t\\r\\n]+', ' ', 'g') as ref_text,
               {col('ref_text')} is null as from_group, {col('f')} as f,
               case when lower({col('sex')}) in ('female', 'f') then 'F' when lower({col('sex')}) in ('male', 'm') then 'M' end as sex,
               case when isfinite({num('v')}) then {num('v')} end as v,
               case when {num('age')} >= 0 then {num('age')} end as age
        from {src} where {text} is not null""")

    log("pairs: rows and people per test name x text")
    con.sql(f"""create or replace temp table pairs as
        with per_person as (
            select test_name, ref_text, f, count(*) n, count(*) filter (where from_group) n_group,
                   count(*) filter (where sex = 'F') n_f, count(*) filter (where sex = 'M') n_m, count(v) n_v, sum(v) sum_v
            from rows_ group by all)
        , pairs as (select test_name, ref_text, sum(n)::bigint n, sum(n_group) n_group, count(f) people,
                           sum(n_f) n_f, sum(n_m) n_m, sum(n_v)::bigint n_v, sum(sum_v) sum_v from per_person group by all)
        , kept as (select *, 100 * n / sum(n) over () pct from pairs
                   qualify n >= {args.min_n} and people >= {args.min_people})
        select row_number() over (order by n desc, test_name, ref_text) pair_id, hash(test_name, ref_text) k, * from kept""")
    log(f"{con.sql('select count(*) from pairs').fetchone()[0]:,} pairs kept")

    for kind, expr, where in (('value', 'case when v = 0 then 0 else round(v, 2 - floor(log10(abs(v)))::int) end', 'v is not null'),
                              ('age', 'case when age < 2 then floor(age * 12) / 12 else floor(age) end', 'age is not null')):
        log(f"{kind} histogram")
        part = files[kind].with_name(files[kind].name + '.part')
        con.sql(f"""copy (
            select p.pair_id, h.{kind}, h.n
            from (select hash(test_name, ref_text) k, {expr} as {kind}, count(*) n from rows_ where {where} group by all) h
            join pairs p using (k) order by 1, 2
        ) to '{part}' (format parquet)""")
        os.replace(part, files[kind])

    def quantiles(kind, probs, digits):
        sel = ', '.join(f"round(min({kind}) filter (where cum >= {p} * tot), {digits}) {c}" for c, p in probs.items())
        return f"""(select pair_id, {sel} from (
            select pair_id, {kind}, sum(n) over (partition by pair_id order by {kind} rows unbounded preceding) cum,
                   sum(n) over (partition by pair_id) tot from read_parquet('{files[kind]}')) group by pair_id)"""

    part = files['table'].with_name(files['table'].name + '.part')
    con.sql(f"""copy (
        select pair_id, test_name, ref_text, n, round(pct, 3) pct,
               round(sum(pct) over (order by pair_id rows unbounded preceding), 3) cum_pct,
               round(100 * n_group / n, 1) pct_group, people, round(100 * n_f / nullif(n_f + n_m, 0), 1) pct_f,
               n_v n_value, round(sum_v / nullif(n_v, 0), 6) v_mean, {', '.join(V_PROBS)}, {', '.join(AGE_PROBS)}
        from pairs left join {quantiles('value', V_PROBS, 12)} using (pair_id) left join {quantiles('age', AGE_PROBS, 2)} using (pair_id)
        order by pair_id
    ) to '{part}' (header, delimiter '\t', quote '')""")
    os.replace(part, files['table'])
    n, rows, pct, grp = con.sql(f"""select count(*), sum(n), sum(pct), sum(n * pct_group / 100)
                                  from read_csv('{files['table']}', delim='\t', header=true, quote='')""").fetchone()
    log(f"Wrote {files['table']}: {n:,} pairs, {rows:,} rows ({pct:.1f}% of all rows with a text), ~{grp:,.0f} of them from the group")


def check(con, df, files, args):
    """Step 2 checks: values vs. limits, rows' sex vs. sex_text, rows' age vs. age_lo/age_hi."""
    lim = df.loc[df['status'].isin(OK), ['pair_id', 'lower', 'upper']].astype({'lower': 'Float64', 'upper': 'Float64'})
    con.register('lim', lim)
    v = con.sql(f"""select pair_id, sum(n) tot, coalesce(sum(n) filter (where value < lower), 0) below,
                           coalesce(sum(n) filter (where value > upper), 0) above
                    from read_parquet('{files['value']}') join lim using (pair_id) group by 1""").df()
    v['pct_below'] = (100 * v['below'] / v['tot']).round(1)
    v['pct_above'] = (100 * v['above'] / v['tot']).round(1)
    v['pct_in'] = (100 - v['pct_below'] - v['pct_above']).round(1)
    df = df.merge(v[['pair_id', 'pct_below', 'pct_in', 'pct_above']], on='pair_id', how='left')

    def value_check(r):
        lo, hi = r['lower'], r['upper']
        if r['status'] not in OK or pd.isna(r['pct_in']):
            return None, None
        if r['pct_in'] >= args.value_min_pct:  # patients sit partly outside a healthy range; only a range that misses them is off
            return 'value_ok', None
        inside = lambda x: (pd.isna(lo) or x >= lo) and (pd.isna(hi) or x <= hi)
        if pd.notna(lo) and lo > 0 and pd.notna(hi) and pd.notna(r['v_p25']) and pd.notna(r['v_p75']):  # 0-x is one-sided
            for k in (1, -1, 2, -2, 3, -3):
                if inside(r['v_p25'] * 10 ** k) and inside(r['v_p75'] * 10 ** k):
                    return 'scale_mismatch', 10.0 ** k
        return 'value_off', None
    df['med_in'] = [None if pd.isna(m) or s not in OK else bool((pd.isna(lo) or m >= lo) and (pd.isna(hi) or m <= hi))
                    for m, lo, hi, s in zip(df['v_p50'], df['lower'], df['upper'], df['status'])]
    df[['value_check', 'scale']] = pd.DataFrame([value_check(r) for _, r in df.iterrows()], index=df.index)

    ages = df.loc[df['age_lo'].notna() | df['age_hi'].notna(), ['pair_id', 'age_lo', 'age_hi']]
    con.register('ages', ages.astype({'age_lo': 'Float64', 'age_hi': 'Float64'}))
    # a bin (whole months under 2, whole years above) counts as inside when it overlaps [age_lo, age_hi): a 3.6-month-old
    # is in the 3-month bin, which a range from 3.5 months must not leave out
    a = con.sql(f"""select pair_id, round(100 * coalesce(sum(n) filter (
                               where (age_lo is null or age + case when age < 2 then 1 / 12 else 1 end > age_lo + 1e-9)
                                 and (age_hi is null or age < age_hi - 1e-9)), 0) / sum(n), 1) pct_age_in
                    from read_parquet('{files['age']}') join ages using (pair_id) group by 1""").df()
    df = df.merge(a, on='pair_id', how='left')
    df['age_check'] = None
    # the bulk, not the tail: a lab that gives a band's text to some patients just outside it is fine; a text read wrongly
    # or put on the wrong group misses most of them. The median age is a bin start too, so it counts when its bin overlaps.
    m = df['age_p50']
    med_in = ((df['age_lo'].isna() | (m + m.lt(2).map({True: 1 / 12, False: 1}) > df['age_lo'] + 1e-9))
              & (df['age_hi'].isna() | (m < df['age_hi'] - 1e-9)))
    has = df['pct_age_in'].notna() & m.notna()
    df.loc[has, 'age_check'] = (df['pct_age_in'].ge(args.age_min_pct) & med_in)[has].map({True: 'age_ok', False: 'age_mismatch'})

    df['sex_check'] = None
    for s, ok in (('F', df['pct_f'] >= args.sex_min_pct), ('M', df['pct_f'] <= 100 - args.sex_min_pct)):
        m = (df['sex_text'] == s) & df['pct_f'].notna()
        df.loc[m, 'sex_check'] = ok[m].map({True: 'sex_ok', False: 'sex_mismatch'})
    return df


if __name__ == '__main__':
    D = Path('/mnt/disks/data/kanta/ref/limits')
    RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--input', required=True, type=Path, help="Engine output (or release) parquet")
    ap.add_argument('--out', type=Path, default=D, help=f"Output directory for the table, LIMITS and FLAGGED files (default: {D})")
    ap.add_argument('--min-n', type=int, default=1000, help="Drop pairs with fewer rows than this (default: 1000)")
    ap.add_argument('--min-people', type=int, default=5, help="Drop pairs seen by fewer people than this (default: 5)")
    ap.add_argument('--force', action='store_true', help="Rebuild the step 1 cache even if it matches the input")
    ap.add_argument('--test', action='store_true', help=f"Read only the first {TEST_ROWS:,} input rows; the files get a _test suffix")
    ap.add_argument('--value-min-pct', type=float, default=10, help="value_ok when at least this %% of values are inside the limits (default: 10)")
    ap.add_argument('--age-min-pct', type=float, default=50, help="age_ok when at least this %% of rows, and the median age, are in the text's ages (default: 50)")
    ap.add_argument('--sex-min-pct', type=float, default=90, help="sex_ok when at least this %% of rows are of the text's sex (default: 90)")
    ap.add_argument('--show', type=int, default=30, help="Unparsed texts and failed checks shown (default: 30)")
    ap.add_argument('--tmp', type=Path, default=Path('/mnt/disks/data/kanta/tmp/ref_text'),
                    help="Step 1 cache and duckdb spill folder (default: /mnt/disks/data/kanta/tmp/ref_text)")
    ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                    help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
    ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads; fewer threads lower peak memory (default: min(8, cores))")
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.tmp / 'duckdb').mkdir(parents=True, exist_ok=True)
    sfx = '_test' if args.test else ''
    stem = args.out / f"ref_text_table{sfx}"
    cstem = args.tmp / f"ref_text_{args.input.stem}{sfx}"
    files = {'table': stem.with_suffix('.tsv'), 'value': Path(f"{cstem}_value_hist.parquet"),
             'age': Path(f"{cstem}_age_hist.parquet"), 'meta': Path(f"{cstem}.json")}
    out = Path(f"{stem}_LIMITS.tsv")
    out_flagged = Path(f"{stem}_FLAGGED.tsv")
    t0 = time.time()
    log = lambda msg: print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)

    con = duckdb.connect()
    con.sql(f"set temp_directory = '{args.tmp / 'duckdb'}'")
    con.sql("set preserve_insertion_order = false")
    con.sql(f"set memory_limit = '{args.memory_limit}'")
    con.sql(f"set threads = {args.threads}")

    st = args.input.stat()
    meta = {'input': str(args.input.resolve()), 'input_mtime': st.st_mtime, 'input_size': st.st_size,
            'min_n': args.min_n, 'min_people': args.min_people, 'test': args.test,
            'table': str(files['table'].resolve())}
    cached = all(f.exists() for f in files.values()) and json.loads(files['meta'].read_text()) == meta
    if cached and not args.force:
        log(f"Step 1: cache matches {args.input.name}, reusing {files['table']} (--force to rebuild)")
    else:
        log(f"Step 1: building the table from {args.input}; duckdb memory limit {args.memory_limit} "
            f"(machine has {RAM_GB:.0f}GB), {args.threads} threads, spilling to {args.tmp / 'duckdb'}")
        files['meta'].unlink(missing_ok=True)
        build_table(con, args, files, log)
        files['meta'].write_text(json.dumps(meta, indent=1) + '\n')

    log("Step 2: parsing and checking the texts")
    df = pd.read_csv(files['table'], sep='\t', dtype=str, keep_default_na=False, quoting=3)
    num_cols = ['pair_id', 'n', 'pct_f', 'v_p25', 'v_p50', 'v_p75', 'age_p50']
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
    parsed = {t: parse(t) for t in df['ref_text'].unique()}  # a text repeats across test names
    new = ['lower', 'upper', 'status', 'rest', 'sex_text', 'age_lo', 'age_hi']
    df[new] = pd.DataFrame([parsed[t] for t in df['ref_text']], index=df.index, columns=new)
    df = check(con, df, files, args)
    df['flags'] = [';'.join(f for f in fs if isinstance(f, str) and f in FLAGS)
                   for fs in zip(df['status'], df['value_check'], df['sex_check'], df['age_check'])]
    df.to_csv(out, sep='\t', index=False, quoting=3, escapechar='\\')
    tot = df['n'].sum()
    log(f"{len(df):,} pairs ({len(parsed):,} texts), {tot:,} rows -> {out}")
    flagged = df[df['flags'] != ''][FLAGGED_COLS]
    flagged.to_csv(out_flagged, sep='\t', index=False, quoting=3, escapechar='\\')
    log(f"{len(flagged):,} pairs with a flag ({flagged['n'].sum():,} rows) -> {out_flagged}\n")

    pd.set_option('display.width', 250)
    pd.set_option('display.max_colwidth', 60)
    for c in ('status', 'value_check', 'sex_check', 'age_check'):
        s = df.groupby(c)['n'].agg(pairs='size', rows='sum').sort_values('rows', ascending=False)
        s['pct_rows'] = (100 * s['rows'] / tot).round(2)
        print(s.to_string(), '\n')
    ok = df['status'].isin(OK)
    ranged = ~df['status'].str.startswith('skip')
    print(f"with limits: {100 * df.loc[ok, 'n'].sum() / tot:.2f}% of rows; "
          f"{100 * df.loc[ok, 'n'].sum() / df.loc[ranged, 'n'].sum():.2f}% of the rows not skipped")
    shows = [('unparsed', df['status'] == 'unparsed', ['test_name', 'ref_text', 'n']),
             ('scale_mismatch', df['value_check'] == 'scale_mismatch', ['test_name', 'ref_text', 'n', 'lower', 'upper', 'v_p05', 'v_p50', 'v_p95', 'scale']),
             ('value_off', df['value_check'] == 'value_off', ['test_name', 'ref_text', 'n', 'lower', 'upper', 'v_p05', 'v_p50', 'v_p95', 'pct_in']),
             ('sex_mismatch', df['sex_check'] == 'sex_mismatch', ['test_name', 'ref_text', 'n', 'sex_text', 'pct_f']),
             ('age_mismatch', df['age_check'] == 'age_mismatch', ['test_name', 'ref_text', 'n', 'age_lo', 'age_hi', 'age_p05', 'age_p50', 'age_p95', 'pct_age_in'])]
    for label, m, cols in shows:
        if m.any():
            print(f"\nTop {args.show} {label}:")
            print(df.loc[m, cols].head(args.show).to_string(index=False))
