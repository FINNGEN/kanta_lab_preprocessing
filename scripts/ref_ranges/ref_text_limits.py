#!/usr/bin/env python3
"""Add lower/upper reference limits to ref_text_table.tsv (from ref_text_table.py), parsed from each text.

A text is read as: [prefix] limits [unit] [/qualifier | ,comment], case-insensitive, decimal commas accepted:
  a-b, a - b, a-<b                 -> lower a, upper b
  <x, <=x, =/<x, alle x, -<x, - x -> upper x (lower left empty)
  >x, >=x, =/>x, yli x, >x -, x -  -> lower x (upper left empty)
  [sex,] ala: a ylä: b             -> lower a, upper b
Prefixes dropped: 'viitearvot:', '14v-aik '. Units are optional (anything with a '/', or the start of a known unit word), and
sex/group words may follow them ('117 - 155 g/l Naiset g/l'). The qualifier (sex, age, group) is not read here. Anything else left over makes the text 'unparsed', so a text that only
gives an age ('yli15-vuotiaat', 'yli65vuotta') is not taken as a limit.

Texts that are not a reference range are skipped before parsing, with the reason as status:
  skip_label (sex/group only: nainen, mies, aikuiset), skip_target (tavoite..., treatment targets),
  skip_therapeutic (hoitotaso, ak-hoito, ter), skip_no_number (negatiivinen, NEGAT, lopullinen, ks.ohjekirja, ...)

Writes --out/<input name>_LIMITS.tsv: the input columns plus lower, upper, status (range / upper / lower / skip_* / unparsed / bad_order when
lower > upper), rest (what was not read, for unparsed texts). Prints the rows by status and the top unparsed texts.
"""
import argparse, re
from pathlib import Path
import pandas as pd

NUM = r'(?:\d+(?:\.\d+)?|\.\d+)'  # also .5
SNUM = rf'[-+]?{NUM}'  # signed, for ranges like -3 - 3 (base excess)
# a word that is the start of one of these is a unit (units often come truncated: 'nm', 'mmo', 'k'); age words
# (v, kk, vrk, pv, vk, vi, vko) start none of them, so an age is never taken as a unit
UNIT_WORDS = ('fl', 'pg', '%', 'kpa', 'mmol', 'umol', 'µmol', 'nmol', 'pmol', 'mg', 'ug', 'µg', 'ng', 'g', 'kg', 'sek',
              'min', 'osuus', 'mosm', 'mu', 'iu', 'u', 'ku', 'ratio', 'x', 'm2', 'm²', 'e6', 'e9', 'e12', 'titteri')
LABELS = {'nainen', 'naiset', 'mies', 'miehet', 'aikuiset', 'aik', 'lapset', 'kaikki', 'nnnnn'}
SKIP = [('skip_target', re.compile(r'^tav')),
        ('skip_therapeutic', re.compile(r'hoitotaso|ak-hoito|hoitoa|\bter\b'))]
PREFIX = re.compile(r'^(?:viitearvot?:\s*|\d+\s*v-aik\w*\s+)')
SEP = r'\s*[-–]\s*'
PATTERNS = [  # (status, regex); groups: lower, upper as named
    ('range', re.compile(rf'^(?:\w+,\s*)?ala:\s*(?P<lo>{SNUM})\s*ylä:\s*(?P<hi>{SNUM})')),
    ('range', re.compile(rf'^(?P<lo>{SNUM}){SEP}<?\s*(?P<hi>{SNUM})')),
    ('upper', re.compile(rf'^(?:-\s*)?(?:<=?|=/?<|≤|alle)\s*(?P<hi>{NUM})')),
    ('upper', re.compile(rf'^-\s+(?P<hi>{NUM})')),
    ('lower', re.compile(rf'^(?:>=?|=/?>|≥|yli)\s*(?P<lo>{NUM})(?:\s*-(?!\s*\d))?')),
    ('lower', re.compile(rf'^(?P<lo>{NUM})\s*-(?!\s*[\d<])')),
]


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
    """One reference text -> (lower, upper, status, rest)."""
    t = re.sub(r'\s+', ' ', text.lower().replace('"', '')).strip()
    t = re.sub(r'(\d),(\d)', r'\1.\2', t)
    t = PREFIX.sub('', t)
    if t in LABELS:
        return None, None, 'skip_label', None
    for status, r in SKIP:
        if r.search(t):
            return None, None, status, None
    if not re.search(r'\d', t):
        return None, None, 'skip_no_number', None
    for status, r in PATTERNS:
        m = r.match(t)
        if m:
            rest = t[m.end():].strip()
            if not tail_ok(rest):
                return None, None, 'unparsed', t
            g = m.groupdict()
            lo, hi = (float(g[k]) if g.get(k) is not None else None for k in ('lo', 'hi'))
            if lo is not None and hi is not None and lo > hi:
                return lo, hi, 'bad_order', None
            return lo, hi, status, None
    return None, None, 'unparsed', t


if __name__ == '__main__':
    D = Path('/mnt/disks/data/kanta/ref')
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--input', type=Path, default=D / 'ref_text_table.tsv', help=f"ref_text_table.tsv (default: {D / 'ref_text_table.tsv'})")
    ap.add_argument('--out', type=Path, default=D, help=f"Output directory (default: {D})")
    ap.add_argument('--show', type=int, default=30, help="Unparsed texts shown (default: 30)")
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    out = args.out / f"{args.input.stem}_LIMITS.tsv"

    df = pd.read_csv(args.input, sep='\t', dtype=str, keep_default_na=False, quoting=3)
    df['n'] = df['n'].astype(int)
    parsed = {t: parse(t) for t in df['ref_text'].unique()}  # a text repeats across test names
    df[['lower', 'upper', 'status', 'rest']] = pd.DataFrame([parsed[t] for t in df['ref_text']], index=df.index)
    df.to_csv(out, sep='\t', index=False, quoting=3, escapechar='\\')
    tot = df['n'].sum()
    print(f"{len(df):,} pairs ({len(parsed):,} texts), {tot:,} rows -> {out}\n")

    s = df.groupby('status')['n'].agg(pairs='size', rows='sum').sort_values('rows', ascending=False)
    s['pct_rows'] = (100 * s['rows'] / tot).round(2)
    print(s.to_string(), '\n')
    ok = df['status'].isin(['range', 'upper', 'lower'])
    ranged = ~df['status'].str.startswith('skip')
    print(f"with limits: {100 * df.loc[ok, 'n'].sum() / tot:.2f}% of rows; "
          f"{100 * df.loc[ok, 'n'].sum() / df.loc[ranged, 'n'].sum():.2f}% of the rows not skipped")
    pd.set_option('display.width', 250)
    print(f"\nTop {args.show} unparsed:")
    print(df.loc[df['status'] == 'unparsed', ['test_name', 'ref_text', 'n']].head(args.show).to_string(index=False))
