#!/usr/bin/env python3
"""Parse the reference-range texts of ref_text_table.tsv (from ref_text_table.py) into limits, unit, sex and age.

A text is read as: range [unit] [/qualifier]. Range forms: `a-b` / `a - b` (range), `<x`, `alle x`, `-<x` (upper),
`>x`, `yli x` (lower); decimal commas are accepted, units are optional and may repeat (`mmol/mol mmol/mol`). The
qualifier after `/` gives sex (naiset/tytöt -> F, miehet/pojat -> M, also N/M), group (aikuiset, lapset, kaikki) and
age: `17V-`, `yli 18 v`, `alkaen 18V`, `18 v täyttäneet`, `60-69V`, `1KK-16V`, `8 vrk - 16 v`, `alle 17V`, with
V = years, KK = months, VI/VKO/VK = weeks, VRK/PV = days (converted to years). Whatever is not read is kept in `rest`.

Writes --out: the input columns plus kind (range/upper/lower), low, high, unit, sex, group, age_from, age_to, rest,
parsed (true when nothing is left over). Prints the coverage by rows; when the input has pct_female and
age_p05/age_p95 columns, also checks the parsed sex and age against them.
"""
import argparse, re
from pathlib import Path
import pandas as pd

NUM = r'-?\d+(?:[.,]\d+)?'
UNIT = (r'(?:10e9/l|e9/l|e12/l|e6/l|fl|pg|g/l|mg/l|ug/l|µg/l|umol/l|µmol/l|mmol/mol|mmol/l|nmol/l|pmol/l|ng/l|mu/l|u/l|iu/l|'
        r'ku/l|mg/mmol|ml/min(?:/1[.,]7\d\s*m(?:2|²)?)?|%|osuus)')
SEP = r'\s*[-–]\s*'
UNITS = rf'((?:\s*{UNIT})*)'
PATTERNS = [('range', re.compile(rf'^({NUM}){SEP}({NUM}){UNITS}')),
            ('upper', re.compile(rf'^(?:-\s*)?(?:<=?|≤|alle)\s*({NUM}){UNITS}')),
            ('lower', re.compile(rf'^(?:>=?|≥|yli)\s*({NUM}){UNITS}'))]
SEX = [(re.compile(r'\b(nai\w*|tyt\w*|n)\b'), 'F'), (re.compile(r'\b(mie\w*|poj\w*|m)\b'), 'M')]
GROUP = [(re.compile(r'\baik\w*'), 'adults'), (re.compile(r'\blap\w*'), 'children'), (re.compile(r'\bkaik\w*'), 'all')]
AGE_UNIT = {'v': 1, 'vuotta': 1, 'vi': 7 / 365, 'vko': 7 / 365, 'vk': 7 / 365, 'kk': 1 / 12, 'vrk': 1 / 365, 'pv': 1 / 365}
AU = r'(vuotta|vrk|vko|vi|pv|kk|vk|v)'  # longer units first, so 'v' doesn't swallow 'vrk'/'vi'
AGE_RANGE = re.compile(rf'(\d+)\s*{AU}?\s*-\s*(\d+)\s*{AU}')
AGE_FROM = re.compile(rf'(?:yli|alkaen|alk\.?)\s*(\d+)\s*{AU}?|(\d+)\s*{AU}\s*(?:-|tai yli|täyttäneet)')
AGE_TO = re.compile(rf'alle\s*(\d+)\s*{AU}?')
num = lambda x: float(x.replace(',', '.'))


def parse(t):
    """One reference-range text -> dict of kind, low, high, unit, sex, group, age_from, age_to, rest."""
    t0 = re.sub(r'\s+', ' ', t.strip().lower().replace('"', ''))
    out = dict(kind=None, low=None, high=None, unit=None, sex=None, group=None, age_from=None, age_to=None, rest=None)
    for kind, r in PATTERNS:
        m = r.match(t0)
        if m:
            break
    else:
        out['rest'] = t0
        return out
    g = m.groups()
    out['kind'] = kind
    if kind == 'range':
        out['low'], out['high'] = num(g[0]), num(g[1])
    elif kind == 'upper':
        out['high'] = num(g[0])
    else:
        out['low'] = num(g[0])
    units = g[-1].split()
    out['unit'] = units[0] if units else None
    rest = t0[m.end():].strip()
    if rest.startswith('/'):
        q = rest[1:].strip()
        out['sex'] = next((v for r, v in SEX if r.search(q)), None)
        out['group'] = next((v for r, v in GROUP if r.search(q)), None)
        if (a := AGE_RANGE.search(q)):
            out['age_from'] = round(int(a[1]) * AGE_UNIT[a[2] or a[4]], 2)
            out['age_to'] = round(int(a[3]) * AGE_UNIT[a[4]], 2)
        elif (a := AGE_FROM.search(q)):
            n, u = (a[1], a[2]) if a[1] else (a[3], a[4])
            out['age_from'] = round(int(n) * AGE_UNIT[u or 'v'], 2)
        elif (a := AGE_TO.search(q)):
            out['age_to'] = round(int(a[1]) * AGE_UNIT[a[2] or 'v'], 2)
    elif rest:
        out['rest'] = rest
    return out


def check(full, show):
    """Parsed sex/age vs the rows (1 year of slack for whole-year ages and the +-2 week date shift)."""
    pf, a05, a95 = (pd.to_numeric(full[c], errors='coerce') for c in ('pct_female', 'age_p05', 'age_p95'))
    sex_bad = ((full['sex'] == 'F') & (pf < 90)) | ((full['sex'] == 'M') & (pf > 10))
    age_bad = (full['age_from'].notna() & (a05 < full['age_from'] - 1)) | (full['age_to'].notna() & (a95 > full['age_to'] + 1))
    print(f"\nsex check: {int(full['sex'].notna().sum())} texts with a parsed sex, {int(sex_bad.sum())} disagree with pct_female")
    print(full.loc[sex_bad, ['ref_text', 'n', 'pct_female', 'sex']].head(show).to_string(index=False))
    print(f"\nage check: {int((full['age_from'].notna() | full['age_to'].notna()).sum())} texts with a parsed age, "
          f"{int(age_bad.sum())} disagree with age_p05/age_p95")
    print(full.loc[age_bad, ['ref_text', 'n', 'age_p05', 'age_p95', 'age_from', 'age_to']].head(show).to_string(index=False))


if __name__ == '__main__':
    D = Path('/mnt/disks/data/kanta/service-provider/ref_text')
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument('--input', type=Path, default=D / 'ref_text_table.tsv', help=f"ref_text_table.tsv (default: {D / 'ref_text_table.tsv'})")
    ap.add_argument('--out', type=Path, default=D / 'ref_text_table_parsed.tsv', help=f"Output TSV (default: {D / 'ref_text_table_parsed.tsv'})")
    ap.add_argument('--show', type=int, default=25, help="Texts shown in the not-parsed and check listings (default: 25)")
    args = ap.parse_args()

    df = pd.read_csv(args.input, sep='\t', dtype=str, keep_default_na=False, quoting=3)
    df['n'] = df['n'].astype(int)
    parsed = {t: parse(t) for t in df['ref_text'].unique()}  # a text repeats across test names
    full = pd.concat([df, pd.DataFrame([parsed[t] for t in df['ref_text']], index=df.index)], axis=1)
    full['parsed'] = full['kind'].notna() & full['rest'].isna()
    full.to_csv(args.out, sep='\t', index=False, quoting=3, escapechar='\\')
    tot = full['n'].sum()
    print(f"{len(full):,} test name x text pairs ({len(parsed):,} texts), {tot:,} rows -> {args.out}\n")

    s = full.assign(kind=full['kind'].fillna('none')).groupby(['kind', 'parsed'])['n'].agg(pairs='size', rows='sum')
    s['pct_rows'] = (100 * s['rows'] / tot).round(2)
    print(s.to_string(), '\n')
    has_qual = full[['sex', 'group', 'age_from', 'age_to']].notna().any(axis=1)
    print(f"fully parsed: {100 * full.loc[full['parsed'], 'n'].sum() / tot:.2f}% of rows; with sex/group/age from the text: "
          f"{100 * full.loc[has_qual, 'n'].sum() / tot:.2f}%")

    pd.set_option('display.width', 250)
    if {'pct_female', 'age_p05', 'age_p95'} <= set(full.columns):
        check(full, args.show)
    print(f"\nTop {args.show} not fully parsed:")
    print(full.loc[~full['parsed'], ['test_name', 'ref_text', 'n', 'kind', 'rest']].fillna('').head(args.show).to_string(index=False))

