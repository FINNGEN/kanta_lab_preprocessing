#!/usr/bin/env python3
"""Reference-range texts -> numeric lower/upper limits, checked against the values, sex and age of the rows carrying them.

Step 1 (cached): one pass over the engine output or release, rows with a text only. The text is REFERENCE_RANGE_TEXT, else
REFERENCE_RANGE_GROUP (the same text with its spaces removed; it is filled on some rows without a text). Writes
(aggregates only; tabs/newlines inside a text become spaces and double quotes are dropped, so the files need no escaping; pairs with fewer than 20 rows or 5 people are dropped, a fixed
floor so the cache serves any --min-n):
  --out/ref_text_table.tsv           one row per TEST_NAME x text (the pairs kept by --min-n / --min-people): pair_id, test_name, ref_text, n (rows), pct (% of all rows
                                     with a text, before the filters), cum_pct, pct_group (% of the pair's rows whose text came
                                     from the group), people, pct_f (% women of the rows with a sex), n_value, v_mean,
                                     v_p05/p25/p50/p75/p95 (MEASUREMENT_VALUE: the unit the text is written in), age_p05/p50/p95
and the cache, in --tmp, named after the input file (<input>):
  ref_text_<input>_table.tsv           the same table for every pair above the floor
  ref_text_<input>_value_hist.parquet    pair_id, value (rounded to 3 significant figures), n
  ref_text_<input>_outcome_hist.parquet  pair_id, side (< or >), bound (3 significant figures), n: rows without a numeric
                                         value whose extracted::TEST_OUTCOME_TEXT reads '<x' / '>x' (e.g. '<4mg/l'), i.e.
                                         results reported as text beyond a limit; empty when the input has no such column
  ref_text_<input>_age_hist.parquet      pair_id, age (whole years; whole months under 2), n
  ref_text_<input>.json                what the cache was built from (input, floor, --test)
Step 1 is rerun only when any of those change (or a cache file is missing), or with --force; --min-n and --min-people are
applied in step 2, so runs with different values reuse it. People are counted in
two plain group-bys (pair x person, then pair) and quantiles come from the histograms, so duckdb can spill to disk throughout.

Step 2 (always): each text is read as [prefix] limits [unit] [/qualifier | ,comment], case-insensitive, decimal commas accepted:
  a-b, a - b, a-<b                 -> lower a, upper b
  <x, <=x, =/<x, alle x, -<x, - x -> upper x (lower left empty)
  >x, >=x, =/>x, yli x, >x -, x -  -> lower x (upper left empty)
  [sex,] ala: a ylä: b             -> lower a, upper b
Sex and age written anywhere in the text are read first and taken out (sex_text F/M; age_lo/age_hi in years, age_hi
exclusive): naiset/nainen/N -> F, miehet/mies/M -> M; 17v-, yli 17 v, 14v-aik -> from 17 (14); alle 17 v, -17v -> under 17;
1-17 v -> 1 up to 18 (17 included); kk, vk/vko, vrk/pv convert to years; aikuiset/aik -> from 16, lapset -> under 16 (Finnish labs' adult age).
The prefix 'viitearvot:' and a leading target word (tavoite, tavoit, tav.) are dropped: a target is read as the limits
('tavoite <3' -> upper 3; the lipid targets are the limits labs report). Units are optional (anything with a '/', or the
start of a known unit word). Anything else left over, or limits for both sexes in one text ('miehet >1,naiset >1.2'),
makes the text 'unparsed'.

Texts that are not a reference range are skipped before parsing, with the reason as status:
  skip_label (only sex/age/group: nainen, mies, aikuiset, yli15-vuotiaat), skip_therapeutic (hoitotaso, ak-hoito, ter),
  skip_no_number (negatiivinen, NEGAT, lopullinen, ks.ohjekirja, ...)
A therapeutic text still gets lower/upper when its limits read once those words are out ('2-3/Varfariinihoito' -> 2-3):
candidates with pct_in but no value_check, not used unless whitelisted with use = yes.

Then the checks, from the step 1 histograms:
  value_check  value_ok when >= --value-min-pct of the values are inside the limits (patients sit partly outside a healthy
               range, so the median may too: med_in says whether it is), else value_off; with pct_below/pct_in/pct_above.
               Rows with an outcome '<x' / '>x' instead of a number count only when x settles it: '<x' is inside when
               x <= upper and there is no lower limit above 0, below when x <= lower; '>x' is inside when x >= lower and
               there is no upper limit, above when x >= upper; otherwise left out. n_outcome: rows counted this way;
               pct_in_numeric: the share inside from the numeric values alone. value_unchecked when fewer than
               --value-min-rows rows can be counted (numbers + outcomes): nothing to compare the limits with (e.g. rows
               without a result, or cancelled tests); not a flag
  sex_check    sex_ok when the text says F and >= --sex-min-pct of the rows are women (M: men), else sex_mismatch
  age_check    age_ok when >= --age-min-pct of the rows fall in [age_lo, age_hi) (an age bin counts when it overlaps it)
               and so does the median age (the bulk of the rows, not the tail), else age_mismatch; pct_age_in

Writes --out/ref_text_table_LIMITS.tsv: the pairs with >= --min-n rows and >= --min-people people, with lower, upper,
status (range / upper / lower / skip_* / unparsed / bad_order when lower > upper), rest (what was not read, for unparsed
texts), sex_text, age_lo, age_hi, the checks, flags (the failed ones: unparsed, bad_order, value_off, sex_mismatch,
age_mismatch), and the curation columns: ratio_n, change, the verdict (approved, reason, final_lower, final_upper) and
the manual columns use (yes / no / empty: yes whitelists a pair, no keeps it out), lower_override, upper_override, NOTES.
This file is the one to edit and pass back as --curated on the next run.

The verdict is the limits in use (what the engine fills): final_lower/final_upper = the override, else the parsed (or
candidate) limit, filled only when the pair is approved:
  use = yes                         approved (a whitelist), if it has a limit
  use = no                          not approved
  use empty, an override set        approved (the override is the decision)
  use empty                         approved when the status is range/upper/lower and no check failed (flags empty;
                                    value_unchecked passes)
reason: use=yes / override / parsed for approved pairs; use=no, use=yes, no limits, the failed checks, or the status
(skip_*) for the others.

With --curated <table> (a LIMITS file from an earlier run, possibly edited by hand), each pair is reconciled against it
by test_name x ref_text:
  in both               the manual columns are carried over; ratio_n = n / n in the curated table (informational only);
                        change PARSE_CHANGED when lower, upper or status differ from the curated parse, and also
                        OVERRIDE_CHECK when an override is set on such a pair
  only in this run      change NEW_ENTRY
  only in the curated   kept, with its curated values, change NOT_SEEN (never dropped)
--curated defaults to the current curated table, ref_text_table_LIMITS.tsv next to this script.

Every pair not approved also goes to one of two files (largest first, with the reason and the columns needed to review
it, NOT_SEEN ones included):
  --out/ref_text_table_UNDECIDED.tsv  a decision could still bring it in: not use = no, and parsed or candidate limits
                                      (failed checks, therapeutic texts; whitelist with use = yes) or an unparsed text
                                      (set an override)
  --out/ref_text_table_REJECTED.tsv   out for good: use = no, or no limit in the text (skip_label, skip_no_number)
Prints the rows by status and check, the approved pairs whose parse changed (PARSE_CHANGED / OVERRIDE_CHECK), then the
top undecided pairs.
"""
import argparse, json, os, re, time
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

NUM = r'(?:\d+(?:\.\d+)?|\.\d+)'  # also .5
SNUM = rf'[-+]?{NUM}'  # signed, for ranges like -3 - 3 (base excess)
# a word that is the start of one of these is a unit (units often come truncated: 'nm', 'mmo', 'k'); age words
# (v, kk, vrk, pv, vk, vi, vko) start none of them, so an age is never taken as a unit
UNIT_WORDS = ('fl', 'pg', '%', 'kpa', 'mmol', 'umol', 'µmol', 'nmol', 'pmol', 'mg', 'ug', 'µg', 'ng', 'g', 'kg', 'sek',
              'min', 'osuus', 'mosm', 'mu', 'iu', 'u', 'ku', 'ratio', 'x', 'm2', 'm²', 'e6', 'e9', 'e12', 'titteri')
LABELS = {'nainen', 'naiset', 'mies', 'miehet', 'aikuiset', 'aik', 'lapset', 'kaikki', 'nnnnn'}
SKIP = [('skip_therapeutic', re.compile(r'hoitotaso|ak-hoito|hoitoa|\bter\b|terap|varfariini'))]
SKIP_WORDS = re.compile(r'ak-hoito,?|hoito\w*|\bter\b|terap\w*\.?|varfariini\w*')  # taken out for the candidate limits
# 'viitearvot:' and a leading target word ('tavoite <3', 'Tav. <2.0', 'tavoit<3'): a target is read as the limits
PREFIX = re.compile(r'^(?:viitearvot?:\s*)?(?:tav\w*\.?:?\s*)?')
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
FLAGS = ('unparsed', 'bad_order', 'value_off', 'sex_mismatch', 'age_mismatch')  # failed checks: keep a pair out unless decided
MANUAL = ['use', 'lower_override', 'upper_override', 'NOTES']  # written by hand, carried over by --curated
VERDICT = ['approved', 'reason', 'final_lower', 'final_upper']  # worked out by approve() on every run
REVIEW = ('PARSE_CHANGED', 'OVERRIDE_CHECK')  # changes on approved pairs, printed for a look
REVIEW_COLS = ['test_name', 'ref_text', 'n', 'n_value', 'n_outcome', 'reason', 'change', 'use', 'NOTES', 'ratio_n', 'status', 'lower', 'upper', 'v_p05', 'v_p25', 'v_p50', 'v_p75', 'v_p95',
                 'pct_below', 'pct_in', 'pct_above', 'pct_in_numeric', 'sex_text', 'pct_f', 'age_lo', 'age_hi', 'age_p05', 'age_p50', 'age_p95', 'pct_age_in', 'rest']

# short name -> (engine output column, release column)
COLUMNS = {
    'f': ('FINNGENID', 'FINNGENID'),
    'test_name': ('TEST_NAME_ABBREVIATION', 'TEST_NAME'),
    'ref_text': ('REFERENCE_RANGE_TEXT', 'REFERENCE_RANGE_TEXT'),
    'ref_group': ('REFERENCE_RANGE_GROUP', 'REFERENCE_RANGE_GROUP'),
    'sex': ('SEX', 'SEX'),
    'age': ('EVENT_AGE', 'EVENT_AGE'),
    'v': ('MEASUREMENT_VALUE', 'MEASUREMENT_VALUE_CLEANED'),
    'outcome': ('extracted::TEST_OUTCOME_TEXT', 'TEST_OUTCOME_TEXT'),
}
OPTIONAL = {'outcome'}  # left null when the input lacks it
NULL = '\x1fnull\x1f'  # stands for a missing text/group in the key join (SQL '=' never matches null)
CACHE_VERSION = 6  # bump when step 1's outputs change, so old caches are rebuilt
FLOOR_N, FLOOR_PEOPLE = 20, 5  # step 1 keeps pairs above this floor; --min-n / --min-people filter in step 2
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
    if all(r.search(t) for r in SEX.values()):  # limits per sex in one text ('miehet >1,naiset >1.2'): one pair can't hold them
        return None, None, 'unparsed', s, *grp
    for status, r in SKIP:
        if r.search(t):
            # the limits still written once the skip words are out are kept as candidates (used only with use = yes)
            lo, hi, st, _ = limits(re.sub(r'\s+', ' ', SKIP_WORDS.sub(' ', s)).strip(' ,:;/'))
            if st not in OK:
                lo = hi = None
            return lo, hi, status, None, *grp
    if not re.search(r'\d', s):
        return None, None, 'skip_no_number', None, *grp
    return *limits(s), *grp


def limits(s):
    """The limits at the start of a text without sex/age -> (lower, upper, status, rest)."""
    for status, r in PATTERNS:
        m = r.match(s)
        if m:
            if not tail_ok(s[m.end():].strip()):
                return None, None, 'unparsed', s
            g = m.groupdict()
            lo, hi = (float(g[k]) if g.get(k) is not None else None for k in ('lo', 'hi'))
            if lo is not None and hi is not None and lo > hi:
                return lo, hi, 'bad_order', None
            return lo, hi, status, None
    return None, None, 'unparsed', s


def ref_key(s):
    """A raw REFERENCE_RANGE_TEXT (or REFERENCE_RANGE_GROUP) value -> the cleaned text that keys the curated table, None
    when empty: runs of tabs/newlines become one space, double quotes are dropped, the ends are trimmed; '' and 'NA' are
    empty. A row's key is ref_key(text), else ref_key(group); it joins the table with its test name. The one definition:
    the engine must use this function too."""
    if s is None or pd.isna(s):
        return None
    s = re.sub(r'[\t\r\n]+', ' ', str(s)).replace('"', '').strip()
    return None if s in ('', 'NA') else s


def build_table(con, args, files, log):
    """Step 1: the pair table and the value/age histograms, in one set of passes over the input."""
    src = f"(select * from read_parquet('{args.input}')" + (f" limit {TEST_ROWS})" if args.test else ')')
    cols = set(con.sql(f"select * from read_parquet('{args.input}') limit 0").columns)

    def name(short):
        c = next((c for c in COLUMNS[short] if c in cols), None)
        if c is None:
            if short in OPTIONAL:
                return None
            raise SystemExit(f"{args.input}: missing column {COLUMNS[short][0]} / {COLUMNS[short][1]}")
        return f'"{c}"'

    def col(short):
        """The input's column as a cleaned string ("NA"/'' -> null)."""
        return 'null' if name(short) is None else f"nullif(nullif(trim(cast({name(short)} as varchar)), ''), 'NA')"

    def num(short):
        """The input's column as a number; text values are trimmed and a decimal comma is read as a point ('35,0' -> 35)."""
        return f"try_cast(replace({col(short)}, ',', '.') as double)"

    # the key: ref_key() run in Python once per distinct (text, group), then joined back (one definition, see ref_key)
    raw = lambda short: f"coalesce(cast({name(short)} as varchar), '{NULL}')"
    log("keys: cleaning the distinct reference texts / groups with ref_key()")
    keys = con.sql(f"select distinct {raw('ref_text')} t, {raw('ref_group')} g from {src}").df()
    tk, gk = (keys[c].where(keys[c] != NULL).map(ref_key) for c in ('t', 'g'))
    keys['ref_text'], keys['from_group'] = tk.fillna(gk), tk.isna()
    keys = keys[keys['ref_text'].notna()].reset_index(drop=True)
    con.register('keys', keys)
    log(f"{len(keys):,} distinct text/group values with a key ({keys['ref_text'].nunique():,} keys)")
    if name('outcome') is None:
        log("no extracted::TEST_OUTCOME_TEXT column: outcomes '<x' / '>x' not used")
    # outcome: for rows without a numeric value, '<4mg/l' -> side '<', bound 4 (a result reported as text beyond a limit)
    out_re = "'^\\s*([<>])=?\\s*([0-9]+(?:[.,][0-9]+)?)'"
    con.sql(f"""create or replace temp view rows_ as
        select *, case when v is null and o is not null then nullif(regexp_extract(o, {out_re}, 1), '') end as side,
               case when v is null and o is not null then try_cast(replace(regexp_extract(o, {out_re}, 2), ',', '.') as double) end as bound
        from (select coalesce({col('test_name')}, 'NA') as test_name, k.ref_text, k.from_group, {col('f')} as f,
                     case when lower({col('sex')}) in ('female', 'f') then 'F' when lower({col('sex')}) in ('male', 'm') then 'M' end as sex,
                     case when isfinite({num('v')}) then {num('v')} end as v,
                     case when {num('age')} >= 0 then {num('age')} end as age,
                     lower({col('outcome')}) as o
              from {src} s join keys k on {raw('ref_text')} = k.t and {raw('ref_group')} = k.g)""")

    log("pairs: rows and people per test name x text")
    con.sql(f"""create or replace temp table pairs as
        with per_person as (
            select test_name, ref_text, f, count(*) n, count(*) filter (where from_group) n_group,
                   count(*) filter (where sex = 'F') n_f, count(*) filter (where sex = 'M') n_m, count(v) n_v, sum(v) sum_v
            from rows_ group by all)
        , pairs as (select test_name, ref_text, sum(n)::bigint n, sum(n_group) n_group, count(f) people,
                           sum(n_f) n_f, sum(n_m) n_m, sum(n_v)::bigint n_v, sum(sum_v) sum_v from per_person group by all)
        , kept as (select *, 100 * n / sum(n) over () pct from pairs
                   qualify n >= {FLOOR_N} and people >= {FLOOR_PEOPLE})
        select row_number() over (order by n desc, test_name, ref_text) pair_id, hash(test_name, ref_text) k, * from kept""")
    log(f"{con.sql('select count(*) from pairs').fetchone()[0]:,} pairs kept")

    sig3 = lambda x: f"case when {x} = 0 then 0 else round({x}, 2 - floor(log10(abs({x})))::int) end"
    for kind, expr, where in (('value', f"{sig3('v')} as value", 'v is not null'),
                              ('outcome', f"side, {sig3('bound')} as bound", 'side is not null and bound is not null'),
                              ('age', 'case when age < 2 then floor(age * 12) / 12 else floor(age) end as age', 'age is not null')):
        log(f"{kind} histogram")
        part = files[kind].with_name(files[kind].name + '.part')
        con.sql(f"""copy (
            select p.pair_id, h.* exclude (k)
            from (select hash(test_name, ref_text) k, {expr}, count(*) n from rows_ where {where} group by all) h
            join pairs p using (k) order by all
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
    # every pair with limits, so skipped pairs' candidate limits get pct_in too (value_check stays for parsed pairs only)
    has_lim = df['lower'].notna() | df['upper'].notna()
    lim = df.loc[has_lim, ['pair_id', 'lower', 'upper']].astype({'lower': 'Float64', 'upper': 'Float64'})
    con.register('lim', lim)
    v = con.sql(f"""select pair_id, sum(n) tot, coalesce(sum(n) filter (where value < lower), 0) below,
                           coalesce(sum(n) filter (where value > upper), 0) above
                    from read_parquet('{files['value']}') join lim using (pair_id) group by 1""").df()
    # rows reported as '<x' / '>x' instead of a number count where x settles which side of the limits they are on
    o = con.sql(f"""select pair_id,
            coalesce(sum(n) filter (where side = '<' and lower is not null and bound <= lower), 0) o_below,
            coalesce(sum(n) filter (where side = '<' and not (lower is not null and bound <= lower)
                                     and (upper is null or bound <= upper) and (lower is null or lower <= 0)), 0) o_in_lt,
            coalesce(sum(n) filter (where side = '>' and upper is not null and bound >= upper), 0) o_above,
            coalesce(sum(n) filter (where side = '>' and upper is null and (lower is null or bound >= lower)), 0) o_in_gt
        from read_parquet('{files['outcome']}') join lim using (pair_id) group by 1""").df()
    v = v.merge(o, on='pair_id', how='outer').fillna(0)
    v['n_outcome'] = (v['o_below'] + v['o_in_lt'] + v['o_above'] + v['o_in_gt']).astype('int64')
    tot = v['tot'] + v['n_outcome']
    v['pct_in_numeric'] = (100 * (v['tot'] - v['below'] - v['above']) / v['tot'].where(v['tot'] > 0)).round(1)
    v['pct_below'] = (100 * (v['below'] + v['o_below']) / tot).round(1)
    v['pct_above'] = (100 * (v['above'] + v['o_above']) / tot).round(1)
    v['pct_in'] = (100 - v['pct_below'] - v['pct_above']).round(1)
    v['n_counted'] = (tot).astype('int64')
    df = df.merge(v[['pair_id', 'n_outcome', 'n_counted', 'pct_below', 'pct_in', 'pct_above', 'pct_in_numeric']], on='pair_id', how='left')

    df['med_in'] = [None if pd.isna(m) or s not in OK else bool((pd.isna(lo) or m >= lo) and (pd.isna(hi) or m <= hi))
                    for m, lo, hi, s in zip(df['v_p50'], df['lower'], df['upper'], df['status'])]
    # patients sit partly outside a healthy range; only a range that misses them is off
    df['value_check'] = None
    has = df['status'].isin(OK) & df['pct_in'].notna()
    df.loc[has, 'value_check'] = df.loc[has, 'pct_in'].ge(args.value_min_pct).map({True: 'value_ok', False: 'value_off'})
    # too few rows with a number or an outcome to judge the limits either way
    few = df['status'].isin(OK) & df['n_counted'].fillna(0).lt(args.value_min_rows)
    df.loc[few, 'value_check'] = 'value_unchecked'

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


def approve(d):
    """The limits in use (see the docstring): approved yes/no, the reason, and final_lower/final_upper (an override, else
    the parsed limit), filled on approved pairs only."""
    use = d['use'].str.strip().str.lower()
    set_ = [d[c].astype(str).str.strip() != '' for c in ('lower_override', 'upper_override')]
    lo, hi = (pd.to_numeric(d[c].astype(str).str.strip(), errors='coerce').fillna(pd.to_numeric(d[p], errors='coerce'))
              for c, p in (('lower_override', 'lower'), ('upper_override', 'upper')))
    has, override = lo.notna() | hi.notna(), set_[0] | set_[1]
    flags = d['flags'].fillna('').astype(str)
    approved = has & ((use == 'yes') | ((use == '') & (override | (d['status'].isin(OK) & (flags == '')))))
    d['approved'] = np.where(approved, 'yes', 'no')
    d['reason'] = np.select(
        [approved & (use == 'yes'), approved & override, approved, use == 'no', use == 'yes', use != ''],
        ['use=yes', 'override', 'parsed', 'use=no', 'use=yes, no limits', 'use=' + use],
        default=np.where(flags != '', flags, d['status']))  # undecided: the failed checks, else the status (skip_*)
    d['final_lower'], d['final_upper'] = lo.where(approved), hi.where(approved)
    return d[[c for c in d.columns if c not in VERDICT + MANUAL] + VERDICT + MANUAL]


def undecided(d):
    """Pairs not approved that a decision could still bring in: not use = no, and parsed or candidate limits (a
    whitelist) or an unparsed text (an override). The other pairs not approved have no limit to use: rejected."""
    lim = pd.to_numeric(d['lower'], errors='coerce').notna() | pd.to_numeric(d['upper'], errors='coerce').notna()
    return (d['approved'] == 'no') & (d['use'].str.strip().str.lower() != 'no') & (lim | (d['status'] == 'unparsed'))


def reconcile(df, curated, log):
    """Carry the curated table's manual columns over to this run's pairs and mark what changed (see the docstring).
    Returns this run's pairs and the curated pairs not seen in this run."""
    key = ['test_name', 'ref_text']
    order = lambda d: d[[c for c in d.columns if c not in ['ratio_n', 'change'] + MANUAL] + ['ratio_n', 'change'] + MANUAL]
    if curated is None:
        for c in MANUAL + ['change']:
            df[c] = ''
        df['ratio_n'] = np.nan
        return order(df), df.iloc[:0]
    cur = pd.read_csv(curated, sep='\t', dtype=str, keep_default_na=False, quoting=3, escapechar='\\')
    missing = set(key + ['n', 'lower', 'upper', 'status']) - set(cur.columns)
    if missing:
        raise SystemExit(f"{curated}: missing columns {sorted(missing)}; it must be a LIMITS file from this script")
    dup = cur.duplicated(key)
    if dup.any():
        raise SystemExit(f"{curated}: {dup.sum()} duplicated test_name x ref_text keys, e.g. {cur.loc[dup, key].iloc[0].tolist()}")
    for c in MANUAL:
        if c not in cur:
            cur[c] = ''
    old = cur[key + MANUAL + ['n', 'lower', 'upper', 'status']].rename(
        columns={'n': 'n_old', 'lower': 'lower_old', 'upper': 'upper_old', 'status': 'status_old'})
    m = df.merge(old, on=key, how='left', indicator=True)
    both = m['_merge'] == 'both'
    for c in MANUAL:
        m[c] = m[c].fillna('')
    m['ratio_n'] = (m['n'] / pd.to_numeric(m['n_old'], errors='coerce')).round(3).where(both)

    def differ(a, b):
        a, b = pd.to_numeric(a, errors='coerce'), pd.to_numeric(b, errors='coerce')
        same = (a.isna() & b.isna()) | (a.notna() & b.notna() & np.isclose(a.fillna(0), b.fillna(0)))
        return ~same
    parse_changed = both & ((m['status'] != m['status_old']) | differ(m['lower'], m['lower_old']) | differ(m['upper'], m['upper_old']))
    override = (m['lower_override'].str.strip() != '') | (m['upper_override'].str.strip() != '')
    m['change'] = [';'.join(c for c, on in (('NEW_ENTRY', not b), ('PARSE_CHANGED', pc), ('OVERRIDE_CHECK', pc and ov)) if on)
                   for b, pc, ov in zip(both, parse_changed, override)]
    m = m.drop(columns=['n_old', 'lower_old', 'upper_old', 'status_old', '_merge'])
    gone = cur[~cur.set_index(key).index.isin(m.set_index(key).index)].copy()
    gone['change'], gone['pair_id'] = 'NOT_SEEN', ''
    log(f"Reconciled with {curated}: {both.sum():,} pairs in both, {(~both).sum():,} new, {len(gone):,} not seen in this run; "
        f"{parse_changed.sum():,} parse changes, {(parse_changed & override).sum():,} on overridden pairs")
    return order(m), gone


if __name__ == '__main__':
    D = Path('/mnt/disks/data/kanta/ref/limits')
    CURATED = Path(__file__).resolve().parent / 'ref_text_table_LIMITS.tsv'  # hardcoded for now, to move to kanta.config
    RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--input', required=True, type=Path, help="Engine output (or release) parquet")
    ap.add_argument('--out', type=Path, default=D, help=f"Output directory for the table, LIMITS, UNDECIDED and REJECTED files (default: {D})")
    ap.add_argument('--min-n', type=int, default=1000, help=f"Drop pairs with fewer rows than this, >= {FLOOR_N} (default: 1000)")
    ap.add_argument('--min-people', type=int, default=5, help=f"Drop pairs seen by fewer people than this, >= {FLOOR_PEOPLE} (default: 5)")
    ap.add_argument('--curated', type=Path, default=CURATED,
                    help="A LIMITS file from an earlier run (possibly edited by hand) to reconcile with: manual columns carried over, "
                         f"NEW_ENTRY / PARSE_CHANGED / OVERRIDE_CHECK / NOT_SEEN marked (default: the current curated table, {CURATED.name})")
    ap.add_argument('--force', action='store_true', help="Rebuild the step 1 cache even if it matches the input")
    ap.add_argument('--test', action='store_true', help=f"Read only the first {TEST_ROWS:,} input rows; the files get a _test suffix")
    ap.add_argument('--value-min-pct', type=float, default=10, help="value_ok when at least this %% of values are inside the limits (default: 10)")
    ap.add_argument('--value-min-rows', type=int, default=100, help="value_unchecked when fewer rows than this have a number or an outcome (default: 100)")
    ap.add_argument('--age-min-pct', type=float, default=50, help="age_ok when at least this %% of rows, and the median age, are in the text's ages (default: 50)")
    ap.add_argument('--sex-min-pct', type=float, default=90, help="sex_ok when at least this %% of rows are of the text's sex (default: 90)")
    ap.add_argument('--show', type=int, default=30, help="Unparsed texts and failed checks shown (default: 30)")
    ap.add_argument('--tmp', type=Path, default=Path('/mnt/disks/data/kanta/tmp/ref_text'),
                    help="Step 1 cache and duckdb spill folder (default: /mnt/disks/data/kanta/tmp/ref_text)")
    ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                    help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
    ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads; fewer threads lower peak memory (default: min(8, cores))")
    args = ap.parse_args()
    if args.min_n < FLOOR_N or args.min_people < FLOOR_PEOPLE:
        raise SystemExit(f"--min-n must be >= {FLOOR_N} and --min-people >= {FLOOR_PEOPLE} (step 1 keeps only pairs above that floor)")
    if args.curated and not args.curated.exists():
        raise SystemExit(f"--curated {args.curated}: not found")
    args.out.mkdir(parents=True, exist_ok=True)
    (args.tmp / 'duckdb').mkdir(parents=True, exist_ok=True)
    sfx = '_test' if args.test else ''
    stem = args.out / f"ref_text_table{sfx}"
    cstem = args.tmp / f"ref_text_{args.input.stem}{sfx}"
    files = {'table': Path(f"{cstem}_table.tsv"), 'value': Path(f"{cstem}_value_hist.parquet"),
             'outcome': Path(f"{cstem}_outcome_hist.parquet"), 'age': Path(f"{cstem}_age_hist.parquet"), 'meta': Path(f"{cstem}.json")}
    out = Path(f"{stem}_LIMITS.tsv")
    out_rejected = Path(f"{stem}_REJECTED.tsv")
    out_undecided = Path(f"{stem}_UNDECIDED.tsv")
    t0 = time.time()
    log = lambda msg: print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)

    con = duckdb.connect()
    con.sql(f"set temp_directory = '{args.tmp / 'duckdb'}'")
    con.sql("set preserve_insertion_order = false")
    con.sql(f"set memory_limit = '{args.memory_limit}'")
    con.sql(f"set threads = {args.threads}")

    st = args.input.stat()
    meta = {'input': str(args.input.resolve()), 'input_mtime': st.st_mtime, 'input_size': st.st_size,
            'floor_n': FLOOR_N, 'floor_people': FLOOR_PEOPLE, 'test': args.test,
            'version': CACHE_VERSION}
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
    table_cols = list(df.columns)
    num_cols = ['pair_id', 'n', 'people', 'pct', 'pct_f', 'v_p50', 'age_p50']
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors='coerce')
    floor_pairs = len(df)
    df = df[(df['n'] >= args.min_n) & (df['people'] >= args.min_people)].reset_index(drop=True)
    df['cum_pct'] = df['pct'].cumsum().round(3)  # the table is sorted by n, so this is the running share of the kept pairs
    df[table_cols].to_csv(stem.with_suffix('.tsv'), sep='\t', index=False, quoting=3, escapechar='\\')
    log(f"{len(df):,} of {floor_pairs:,} pairs have >= {args.min_n} rows and >= {args.min_people} people -> {stem.with_suffix('.tsv')}")
    parsed = {t: parse(t) for t in df['ref_text'].unique()}  # a text repeats across test names
    new = ['lower', 'upper', 'status', 'rest', 'sex_text', 'age_lo', 'age_hi']
    df[new] = pd.DataFrame([parsed[t] for t in df['ref_text']], index=df.index, columns=new)
    df = check(con, df, files, args)
    df['flags'] = [';'.join(f for f in fs if isinstance(f, str) and f in FLAGS)
                   for fs in zip(df['status'], df['value_check'], df['sex_check'], df['age_check'])]
    df, gone = reconcile(df, args.curated, log)
    df = approve(df)
    gone = approve(gone.reindex(columns=df.columns, fill_value='')) if len(gone) else gone.reindex(columns=df.columns)
    full = pd.concat([df, gone], ignore_index=True)
    full.to_csv(out, sep='\t', index=False, quoting=3, escapechar='\\')
    tot = df['n'].sum()
    log(f"{len(df):,} pairs ({len(parsed):,} texts), {tot:,} rows" + (f", plus {len(gone):,} not seen" if len(gone) else '') + f" -> {out}")
    ap_ = df['approved'] == 'yes'
    log(f"approved: {ap_.sum():,} pairs, {df.loc[ap_, 'n'].sum():,} rows ({100 * df.loc[ap_, 'n'].sum() / tot:.2f}%)")
    # the pairs not approved, split in two: still to decide on, and out for good
    und = undecided(full)
    for name, m, path in (('undecided', und, out_undecided), ('rejected', (full['approved'] == 'no') & ~und, out_rejected)):
        full.loc[m, REVIEW_COLS].to_csv(path, sep='\t', index=False, quoting=3, escapechar='\\')
        log(f"{name}: {m.sum():,} pairs, {pd.to_numeric(full.loc[m, 'n']).sum():,} rows -> {path}")
    print()

    pd.set_option('display.width', 250)
    pd.set_option('display.max_colwidth', 60)
    if args.curated:
        ch = pd.concat([df, gone])['change'].replace('', 'unchanged').str.split(';').explode().value_counts()
        print(ch.rename_axis('change').to_frame('pairs').to_string(), '\n')
    for c in ('status', 'value_check', 'sex_check', 'age_check'):
        s = df.groupby(c)['n'].agg(pairs='size', rows='sum').sort_values('rows', ascending=False)
        s['pct_rows'] = (100 * s['rows'] / tot).round(2)
        print(s.to_string(), '\n')
    ok = df['status'].isin(OK)
    ranged = ~df['status'].str.startswith('skip')
    print(f"with limits: {100 * df.loc[ok, 'n'].sum() / tot:.2f}% of rows; "
          f"{100 * df.loc[ok, 'n'].sum() / df.loc[ranged, 'n'].sum():.2f}% of the rows not skipped")
    # approved pairs whose parse changed since the curated table: still used, worth a look
    changed = ap_ & df['change'].str.contains('|'.join(REVIEW))
    if changed.any():
        print(f"\nTop {args.show} of {changed.sum():,} approved pairs whose parse changed since the curated table:")
        print(df.loc[changed, ['test_name', 'ref_text', 'change', 'reason', 'status', 'n', 'lower', 'upper', 'final_lower', 'final_upper',
                               'v_p05', 'v_p50', 'v_p95', 'pct_in']].head(args.show).to_string(index=False))
    # the decisions to make: the _UNDECIDED.tsv pairs
    todo = undecided(df)
    if todo.any():
        cols = ['test_name', 'ref_text', 'reason', 'n', 'n_value', 'lower', 'upper', 'v_p05', 'v_p50', 'v_p95', 'pct_in',
                'sex_text', 'pct_f', 'age_lo', 'age_hi', 'age_p50', 'pct_age_in']
        print(f"\nTop {args.show} of {todo.sum():,} pairs to decide on ({df.loc[todo, 'n'].sum():,} rows; all in {out_undecided}):")
        print(df.loc[todo, cols].sort_values('n', ascending=False).head(args.show).to_string(index=False))
