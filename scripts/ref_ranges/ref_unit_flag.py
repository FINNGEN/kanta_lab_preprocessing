#!/usr/bin/env python3
"""Flag the rows whose reference range belongs to another unit than their MEASUREMENT_UNIT_CLEANED.

One test x OMOP at a time: reads a folder written by ref_unit_sets.py (dir: <out>/<test>/<OMOP id>/, with
metadata.json and ref_unit_sets.tsv): the learned range per stratum x unit (use = true), and from the metadata the
injected rows it was learned from (rows with REFERENCE_RANGE_LOW/HIGH_VALUE already filled from the parsed texts).
Rows checked: the test's rows mapped to that OMOP id, and its unmapped ones (no OMOP id: no unit, or a unit outside the
harmonization, which a match can bring back in); rows mapped to the test's other OMOP ids are left out, and so are
rows without a numeric MEASUREMENT_VALUE_CLEANED (no value, nothing a unit would apply to).
--input overrides the injected rows file.
The verdict comes from the range alone; the row's own value then confirms it (below).

For each row with a range, in its stratum (F/M x minor/adult from SEX and EVENT_AGE; all/all when either is unknown;
per unit, the stratum's learned range, else the pooled all/all one):
  distance to a unit = the larger of |log10(row lower / learned lower)| and |log10(row upper / learned upper)|, each
  only when both are > 0 (so a lower of 0 or a one-sided range is decided on the other limit); a unit matches when its
  distance is within log10(--margin).
  status:
    ok             the claimed unit matches
    unit_mismatch  the claimed unit doesn't (or has no learned range), exactly one other unit matches -> suggested_unit
    unit_missing   the row has no unit, exactly one unit matches -> suggested_unit
    ambiguous      the claimed unit doesn't match, several others do
    range_outlier  the claimed unit has a learned range, no unit matches
    no_reference   the claimed unit has no learned range, no unit matches (or no unit could be compared)
    no_range       the row has no range
The verdict is worked out once per signature (test x stratum x unit x lower x upper), then joined back to the rows.
Confirmation, per row with a suggested unit, with the unit factors (median harmonized / source value over the ok rows
of each unit) and the test's core (all/all of ref_unit_core.tsv): corrected = source value x the suggested unit's
factor. The suggestion stands when |log10(corrected / median)| < |log10(harmonized now / median)|, i.e. the change
brings the value closer to the median; for a unit not harmonized now, when corrected is within --far robust SDs of the
median. Otherwise the status becomes value_range_conflict (the range points to another unit, the value doesn't: e.g. a
41 % hematocrit with a 0.35-0.46 range), and suggested_unit is left empty (range_suggested keeps the range's unit).

Outputs to the test folder, next to metadata.json (with --test the names end in _test); the run's settings are
added to metadata.json under "flags":
- ref_unit_flags_rows.parquet  row level (stays on the data side): ROWID, TEST_NAME, OMOP_CONCEPT_ID, sex, age, unit, lower, upper,
                               REFERENCE_RANGE_SOURCE, status, suggested_unit, range_suggested, x_claimed, x_suggested
                               (distances as factors, e.g. 100 = a hundred times off), stratum_used; only the rows with a
                               confirmed suggested unit (unit_mismatch, unit_missing), unless --all-rows
- ref_unit_flags.tsv           per signature: rows, the range's verdict (range_status, range_suggested) and how many of
                               its rows were confirmed / value_range_conflict (aggregate, >= --min-n rows)
- ref_unit_flags_summary.tsv   rows per test x unit x status x suggested_unit, with % of the test x unit rows
Check of the suggestions against the values, per group (ok rows per unit; every other status per unit; flagged and
value_range_conflict ones per unit -> the unit their range points to):
- ref_unit_check_ranges.tsv    rows; their ranges (lower/upper 5th/50th/95th percentiles, top 3 ranges); the factors;
                               median source value, harmonized as is and corrected; % as is / corrected inside the
                               test's core (all/all of ref_unit_core.tsv; as is empty when the unit isn't harmonized)
- ref_unit_check_hist.tsv      rows per 0.02-log10 bin of the harmonized value: the ok rows (reference), each other
                               group as harmonized now (as_is), and the flagged ones corrected (aggregate, >= --min-n
                               rows per bin)
- ref_unit_check.png           one row per flagged group (a suggested unit, i.e. a change; at most 12) that has
                               harmonized values both now and corrected (the others, e.g. units not harmonized now,
                               are listed at the bottom): the group now and corrected against the reference, and a
                               table of its ranges next to the ok rows' ranges in the claimed and the suggested unit
                               (needs matplotlib; skipped without)
"""
import argparse, json, math, os, time
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
D = Path('/mnt/disks/data/kanta/ref')
ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('dir', type=Path, help=f"Test x OMOP folder written by ref_unit_sets.py, e.g. {D / 'results' / 'b-hkr' / '3009542'}")
ap.add_argument('--input', type=Path, help="Injected rows to use instead of the file named in metadata.json")
ap.add_argument('--margin', type=float, default=1.5, help="A unit matches when both limits are within this factor of its learned range (default: 1.5)")
ap.add_argument('--core-sd', type=float, default=1.0, help="--core-sd of the ref_unit_sets.py run (to get one robust SD from the core) (default: 1)")
ap.add_argument('--far', type=float, default=3.0, help="Unit not harmonized now: the corrected value must be within this many robust SDs of the median (default: 3)")
ap.add_argument('--all-rows', action='store_true', help="Write every row to the row-level file, not only the ones with a suggested unit")
ap.add_argument('--min-n', type=int, default=5, help="Groups with fewer rows are left out of the TSV outputs (default: 5)")
ap.add_argument('--tmp', type=Path, default=Path('/mnt/disks/data/kanta/tmp/ref_unit'),
                help="duckdb spill folder (default: /mnt/disks/data/kanta/tmp/ref_unit)")
ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads; fewer threads lower peak memory (default: min(8, cores))")
ap.add_argument('--test', action='store_true', help="Output names get a _test suffix (use with the _test files of ref_unit_sets.py)")
args = ap.parse_args()
(args.tmp / 'duckdb').mkdir(parents=True, exist_ok=True)
sfx = '_test' if args.test else ''
t0 = time.time()
log = lambda msg: print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)

con = duckdb.connect()
con.sql(f"set temp_directory = '{args.tmp / 'duckdb'}'")
con.sql("set preserve_insertion_order = false")
con.sql(f"set memory_limit = '{args.memory_limit}'")
con.sql(f"set threads = {args.threads}")
log(f"duckdb: memory limit {args.memory_limit} (machine has {RAM_GB:.0f}GB), {args.threads} threads, spilling to {args.tmp / 'duckdb'}")

# --- 0. The test folder: metadata (test name, injected rows file) and learned ranges
META, SETS = args.dir / f"metadata{sfx}.json", args.dir / f"ref_unit_sets{sfx}.tsv"
for f in (META, SETS):
    if not f.exists():
        raise SystemExit(f"{f}: not found; dir must be a test x OMOP folder written by ref_unit_sets.py")
meta = json.loads(META.read_text())
test, omop = meta['test_name'], meta.get('omop_id')
if omop is None:
    raise SystemExit(f"{META}: no omop_id; rerun ref_unit_sets.py")
inj = args.input or Path(meta['injected_rows'])
if not inj.exists():
    raise SystemExit(f"{inj}: injected rows file not found (rerun ref_unit_sets.py)")
if inj.is_dir():  # bucketed by test name (ref_unit_sets.py): read only this test's bucket
    bucket = con.sql(f"select hash({test!r}) % {meta.get('injected_buckets', 64)}").fetchone()[0]
    SRC = f"read_parquet('{inj}/bucket={bucket}/*.parquet')"
else:
    SRC = f"read_parquet('{inj}')"
if not {'REFERENCE_RANGE_SOURCE', 'OMOP_CONCEPT_ID'} <= set(con.sql(f"select * from {SRC} limit 0").columns):
    raise SystemExit(f"{inj}: no REFERENCE_RANGE_SOURCE / OMOP_CONCEPT_ID column; it must be the injected rows from "
                     "a current ref_unit_sets.py run, not the engine output")
THIS = f"TEST_NAME = {test!r} and (OMOP_CONCEPT_ID = {omop!r} or OMOP_CONCEPT_ID is null)"
I = f"(select * from {SRC} where {THIS} and MEASUREMENT_VALUE_CLEANED is not null)"
no_value = con.sql(f"select count(*) from {SRC} where {THIS} and MEASUREMENT_VALUE_CLEANED is null").fetchone()[0]
other = con.sql(f"""select OMOP_CONCEPT_ID, count(*) from {SRC}
                     where TEST_NAME = {test!r} and OMOP_CONCEPT_ID != {omop!r} group by all""").fetchall()
log(f"{test} / OMOP {omop}: rows from {inj}; left out: {no_value:,} without a value"
    + (f", mapped to other OMOP ids: {dict(other)}" if other else ''))
SEX = "case when lower(SEX) in ('male', 'm') then 'M' when lower(SEX) in ('female', 'f') then 'F' end"
AGE = "case when EVENT_AGE < 18 then 'minor' when EVENT_AGE >= 18 then 'adult' end"
# the rows with their stratum and signature key k (the same query in both passes, so the keys match)
R = f"""(select *, hash(test, sex, age, unit, lower, upper) k from (
          select ROWID, OMOP_CONCEPT_ID, REFERENCE_RANGE_SOURCE, TEST_NAME test, {SEX} sex, {AGE} age, MEASUREMENT_UNIT_CLEANED unit,
                 REFERENCE_RANGE_LOW_VALUE lower, REFERENCE_RANGE_HIGH_VALUE upper,
                 MEASUREMENT_VALUE_CLEANED v, MEASUREMENT_VALUE_HARMONIZED h from {I}))"""

# --- 1. Signatures and their rows (pass 1)
sig = con.sql(f"select k, test, sex, age, unit, lower, upper, count(*) n from {R} group by all").df()
log(f"{len(sig):,} signatures over {sig['n'].sum():,} rows")

# --- 2. Verdict per signature
sets = pd.read_csv(SETS, sep='\t', keep_default_na=False, na_values=[''])
sets = sets[sets['use'].astype(str).str.lower() == 'true']
ref = {(r.test, r.sex, r.age, r.unit): (r.lower, r.upper) for r in sets.itertuples()}
units = sets.groupby('test')['unit'].agg(lambda u: sorted(set(u))).to_dict()
tol = math.log10(args.margin) + 1e-9


def dist(lo, hi, rlo, rhi):
    """Larger log10 distance over the limits both sides have (> 0); None when none can be compared."""
    d = [abs(math.log10(a / b)) for a, b in ((lo, rlo), (hi, rhi)) if pd.notna(a) and pd.notna(b) and a > 0 and b > 0]
    return max(d) if d else None


def verdict(s):
    """(status, suggested_unit, x_claimed, x_suggested, stratum_used) for one signature."""
    if pd.isna(s.lower) and pd.isna(s.upper):
        return 'no_range', None, None, None, None
    strat = (s.sex, s.age) if pd.notna(s.sex) and pd.notna(s.age) else ('all', 'all')
    cand, used = {}, set()
    for u in units.get(s.test, []):  # per unit: the stratum's learned range, else the pooled one
        for st, label in ((strat, 'stratum' if strat != ('all', 'all') else 'pooled'), (('all', 'all'), 'pooled')):
            r = ref.get((s.test, *st, u))
            if r is not None:
                cand[u] = dist(s.lower, s.upper, *r)
                used.add(label)
                break
    used = '+'.join(sorted(used)) or None
    fx = lambda d: None if d is None else round(10 ** d, 2)
    claimed = cand.get(s.unit) if pd.notna(s.unit) else None
    if pd.notna(s.unit) and claimed is not None and claimed <= tol:
        return 'ok', None, fx(claimed), None, used
    match = sorted((d, u) for u, d in cand.items() if u != s.unit and d is not None and d <= tol)
    if len(match) == 1:
        return ('unit_missing' if pd.isna(s.unit) else 'unit_mismatch'), match[0][1], fx(claimed), fx(match[0][0]), used
    if len(match) > 1:
        return 'ambiguous', '|'.join(u for _, u in match), fx(claimed), None, used
    return ('range_outlier' if s.unit in cand and claimed is not None else 'no_reference'), None, fx(claimed), None, used


sig[['status', 'suggested_unit', 'x_claimed', 'x_suggested', 'stratum_used']] = pd.DataFrame(
    [verdict(s) for s in sig.itertuples()], index=sig.index, columns=['status', 'suggested_unit', 'x_claimed', 'x_suggested', 'stratum_used'])
con.register('verdict_df', sig[['k', 'status', 'suggested_unit', 'x_claimed', 'x_suggested', 'stratum_used']])

# --- 3. Confirm each suggestion with the row's own value (pass 2): the corrected harmonized value must be closer to
# the core's median than the current one (log10); without a current one (unit not harmonized), within --far robust SDs
fac = dict(con.sql(f"""select r.unit, median(r.h / r.v) from {R} r join verdict_df v using (k)
                       where v.status = 'ok' and r.v > 0 and r.h > 0 group by all""").fetchall())
con.register('fac_df', pd.DataFrame({'unit': list(fac), 'f': list(fac.values())}))
core = pd.read_csv(SETS.with_name(f"ref_unit_core{sfx}.tsv"), sep='\t', keep_default_na=False, na_values=[''])
core = core[(core['sex'] == 'all') & (core['age'] == 'all')].iloc[0]
med_c, lo_c, hi_c = float(core['median']), float(core['core_lo']), float(core['core_hi'])
m, sd = math.log10(med_c), math.log10(hi_c / med_c) / args.core_sd  # median and one robust SD, log10
FLAG = "('unit_mismatch', 'unit_missing')"
con.sql(f"""create temp table rv as
    with x as (
        select r.*, v.status range_status, v.suggested_unit range_suggested, v.x_claimed, v.x_suggested, v.stratum_used,
               case when v.status in {FLAG} then r.v * fs.f end hc
        from {R} r join verdict_df v using (k) left join fac_df fs on fs.unit = v.suggested_unit)
    select *, case
        when range_status not in {FLAG} then range_status
        when hc is null or hc <= 0 then 'value_range_conflict'
        when h > 0 then case when abs(log10(hc) - {m}) < abs(log10(h) - {m}) then range_status else 'value_range_conflict' end
        else case when abs(log10(hc) - {m}) <= {args.far} * {sd} then range_status else 'value_range_conflict' end
        end status
    from x""")
con.sql(f"create temp table rv2 as select *, case when status in {FLAG} then range_suggested end suggested_unit from rv")
by_status = dict(con.sql("select status, count(*) from rv2 group by all").fetchall())
log("Rows by status: " + ', '.join(f"{k} {v:,}" for k, v in sorted(by_status.items(), key=lambda kv: -kv[1])))

# --- 4. Row-level flags
path = args.dir / f"ref_unit_flags_rows{sfx}.parquet"
part = path.with_name(path.name + '.part')
keep = '' if args.all_rows else f"where status in {FLAG}"
con.sql(f"""copy (
    select ROWID, test TEST_NAME, OMOP_CONCEPT_ID, sex, age, unit, lower, upper, REFERENCE_RANGE_SOURCE,
           status, suggested_unit, range_suggested, x_claimed, x_suggested, stratum_used
    from rv2 {keep}) to '{part}' (format parquet)""")
os.replace(part, path)
n_rows = con.sql(f"select count(*) from read_parquet('{path}')").fetchone()[0]
log(f"Wrote {path} ({n_rows:,} rows)")

# --- 5. Aggregates
conf = con.sql(f"""select k, count(*) filter (where status in {FLAG}) n_confirmed,
                          count(*) filter (where status = 'value_range_conflict') n_value_range_conflict
                   from rv2 group by all""").df()
out = (sig.merge(conf, on='k', how='left').rename(columns={'status': 'range_status', 'suggested_unit': 'range_suggested'})
       .drop(columns='k').sort_values(['test', 'n'], ascending=[True, False]))
p = args.dir / f"ref_unit_flags{sfx}.tsv"
out[out['n'] >= args.min_n].to_csv(p, sep='\t', index=False)
log(f"Wrote {p} ({(out['n'] >= args.min_n).sum():,} signatures)")
summ = con.sql("""select test, coalesce(unit, 'NA') unit, status, coalesce(suggested_unit, coalesce(range_suggested, '')) suggested_unit,
                         count(*) n from rv2 group by all""").df()
summ['pct'] = (100 * summ['n'] / summ.groupby(['test', 'unit'])['n'].transform('sum')).round(2)
summ = summ[summ['n'] >= args.min_n].sort_values(['test', 'unit', 'n'], ascending=[True, True, False])
p = args.dir / f"ref_unit_flags_summary{sfx}.tsv"
summ.to_csv(p, sep='\t', index=False)
log(f"Wrote {p} ({len(summ):,} rows)")

# --- 6. Check of the suggestions against the values, per group (value_range_conflict rows with the unit their range
# pointed to, and where they would have landed)
con.sql("""create temp table chk2 as
    select coalesce(c.unit, 'NA') unit, c.status, coalesce(c.range_suggested, '') suggested_unit, c.lower, c.upper, c.v, c.h,
           case when c.status = 'ok' then c.h else c.hc end hc, fc.f f_claimed, fs.f f_suggested
    from rv2 c left join fac_df fc on fc.unit = c.unit left join fac_df fs on fs.unit = c.range_suggested""")
rng = con.sql(f"""
    with top as (select status, unit, suggested_unit, string_agg(rg || ' (' || n || ')', ', ' order by n desc) top_ranges from (
                   select status, unit, suggested_unit,
                          case when lower is null then '<' || upper::double when upper is null then '>' || lower::double
                               else lower::double || '-' || upper::double end rg,
                          count(*) n, row_number() over (partition by status, unit, suggested_unit order by count(*) desc) rk
                   from chk2 group by status, unit, suggested_unit, lower, upper) where rk <= 3 group by all)
    select status, unit, suggested_unit, count(*) as rows,
           quantile_cont(lower, 0.05) lower_p05, median(lower) lower_p50, quantile_cont(lower, 0.95) lower_p95,
           quantile_cont(upper, 0.05) upper_p05, median(upper) upper_p50, quantile_cont(upper, 0.95) upper_p95,
           any_value(top_ranges) top_ranges, any_value(f_claimed) f_claimed, any_value(f_suggested) f_suggested,
           median(v) value_p50, median(h) harmonized_as_is_p50, median(hc) harmonized_corrected_p50,
           round(100 * avg(case when h is null then null when h between {lo_c} and {hi_c} then 1 else 0 end), 2) pct_as_is_in_core,
           round(100 * avg(case when hc is null then null when hc between {lo_c} and {hi_c} then 1 else 0 end), 2) pct_corrected_in_core
    from chk2 join top using (status, unit, suggested_unit) group by status, unit, suggested_unit having count(*) >= {args.min_n}
    order by status != 'ok', status not in {FLAG}, rows desc""").df()
p = args.dir / f"ref_unit_check_ranges{sfx}.tsv"
rng.to_csv(p, sep='\t', index=False, float_format='%.6g')
log(f"Wrote {p} ({len(rng)} groups; unit factors {fac}; core median {med_c:g}, {lo_c:g}-{hi_c:g})")
W = 0.02
hist = con.sql(f"""
    select * from (
        select 'reference' grp, 'ok' status, '' unit, '' suggested_unit, floor(log10(h) / {W}) * {W} bin, count(*) n from chk2
        where status = 'ok' and h > 0 group by all
        union all
        select 'as_is', status, unit, suggested_unit, floor(log10(h) / {W}) * {W}, count(*) from chk2
        where status != 'ok' and h > 0 group by all
        union all
        select 'corrected', status, unit, suggested_unit, floor(log10(hc) / {W}) * {W}, count(*) from chk2
        where status != 'ok' and hc > 0 group by all)
    where n >= {args.min_n} order by grp, status, unit, suggested_unit, bin""").df()
p = args.dir / f"ref_unit_check_hist{sfx}.tsv"
hist.to_csv(p, sep='\t', index=False, float_format='%.4g')
log(f"Wrote {p} ({len(hist):,} bins)")


def plot(path):
    """One row per flagged group (a suggested unit): its values now and corrected against the reference, and a table
    of its ranges next to the ok rows' ranges in the claimed and the suggested unit."""
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        log("No matplotlib: ref_unit_check.png skipped (the hist table has the same data)")
        return
    flagged = rng[rng['status'].isin(['unit_mismatch', 'unit_missing'])]
    has = lambda grp, r: ((hist['grp'] == grp) & (hist['status'] == r.status) & (hist['unit'] == r.unit)
                          & (hist['suggested_unit'] == r.suggested_unit)).any()
    drawable = (flagged.apply(lambda r: has('as_is', r) and has('corrected', r), axis=1) if len(flagged)
                else pd.Series(dtype=bool)).astype(bool)
    groups = list(flagged[drawable].head(12).itertuples(index=False))
    left_out = flagged[~drawable]
    if not groups:
        log("No flagged group with harmonized values now and corrected: ref_unit_check.png skipped")
        return
    INK, MUTED, SURFACE, REF, AS_IS, CORR, LINE = '#1a1a19', '#6b6a63', '#fcfcfb', '#c9c7bd', '#eb6834', '#2a78d6', '#d6d4cc'
    ref = hist[hist['grp'] == 'reference']
    ok = rng[rng['status'] == 'ok'].set_index('unit')
    unit_h = meta.get('harmonized_unit') or 'harmonized unit'
    fmt = lambda x: '' if pd.isna(x) else f"{x:g}"
    top2 = lambda t: '' if not isinstance(t, str) else ', '.join(t.split(', ')[:2])
    nrow = len(groups)
    H = 3.4 * nrow + 1.4  # inches: 3.4 per row, plus title, legend and footnote
    fig = plt.figure(figsize=(15, H), facecolor=SURFACE)
    gs = fig.add_gridspec(nrow, 2, width_ratios=[1.6, 1], wspace=0.06, hspace=0.5,
                          left=0.06, right=0.99, top=1 - 0.6 / H, bottom=(1.0 if len(left_out) else 0.75) / H)
    for i, gr in enumerate(groups):
        st, u, su = gr.status, gr.unit, gr.suggested_unit
        ax = fig.add_subplot(gs[i, 0], facecolor=SURFACE)
        ax.fill_between(10 ** ref['bin'], ref['n'] / ref['n'].sum(), step='post', color=REF, lw=0)
        for grp, color in (('as_is', AS_IS), ('corrected', CORR)):
            g = hist[(hist['grp'] == grp) & (hist['status'] == st) & (hist['unit'] == u) & (hist['suggested_unit'] == su)]
            # on the full bin grid: bins with no rows (or < --min-n, not exported) are 0 within a cluster and at its
            # ends, so the curve comes down to the baseline; gaps longer than 0.2 log10 split clusters (no line along zero)
            idx = ((g['bin'] - g['bin'].min()) / 0.02).round().astype(int).to_numpy() + 1
            share = pd.Series(float('nan'), index=range(idx.max() + 3))
            share[idx] = (g['n'] / g['n'].sum()).to_numpy()
            for a, b in zip(idx[:-1], idx[1:]):
                if b - a <= 10:
                    share[a + 1:b - 1] = 0.0
            ends = [i for i in share.index if pd.isna(share.get(i)) and (pd.notna(share.get(i - 1)) or pd.notna(share.get(i + 1)))]
            share[ends] = 0.0
            ax.step(10 ** (g['bin'].min() + 0.02 * (share.index.to_numpy() - 1)), share.to_numpy(), where='post', color=color, lw=2)
        ax.axvspan(lo_c, hi_c, color=MUTED, alpha=0.08, lw=0)
        ax.set_xscale('log')
        ax.set_title(f"{st}: {u} -> {su}, {int(gr.rows):,} rows    in core: {gr.pct_as_is_in_core:g}% now, "
                     f"{gr.pct_corrected_in_core:g}% corrected", fontsize=9, color=INK, loc='left')
        ax.set_xlabel(f"harmonized value ({unit_h}, log scale)", fontsize=8, color=MUTED)
        ax.set_ylabel('share of rows per bin', fontsize=8, color=MUTED)
        ax.tick_params(labelsize=7, colors=MUTED)
        for side in ('top', 'right'):
            ax.spines[side].set_visible(False)
        for side in ('left', 'bottom'):
            ax.spines[side].set_color(LINE)
        # the ranges: this group's, and the ok rows' in the claimed and in the suggested unit
        rows = [[f"flagged, {u}", f"{int(gr.rows):,}", fmt(gr.lower_p50), fmt(gr.upper_p50), top2(gr.top_ranges)]]
        for label, unit in ((f"ok rows, {u}", u), (f"ok rows, {su}", su)):
            if unit in ok.index:
                o = ok.loc[unit]
                rows.append([label, f"{int(o['rows']):,}", fmt(o['lower_p50']), fmt(o['upper_p50']), top2(o['top_ranges'])])
            else:
                rows.append([label, '0', '', '', ''])
        tx = fig.add_subplot(gs[i, 1])
        tx.axis('off')
        tb = tx.table(cellText=rows, colLabels=['', 'rows', 'lower', 'upper', 'most common ranges'], loc='center',
                      cellLoc='left', colLoc='left', colWidths=[0.25, 0.12, 0.09, 0.09, 0.45])
        tb.auto_set_font_size(False)
        tb.set_fontsize(8)
        tb.scale(1, 1.5)
        for (r, c), cell in tb.get_celld().items():
            cell.set_edgecolor(LINE)
            cell.set_facecolor(SURFACE)
            cell.get_text().set_color(MUTED if r == 0 else INK)
            if r == 1:  # the flagged group's own row
                cell.get_text().set_color(AS_IS if c == 0 else INK)
            if r == 3:
                cell.get_text().set_color(CORR if c == 0 else INK)
        tx.set_title('reference ranges (median lower / upper, on each unit\'s own scale)', fontsize=8, color=MUTED, loc='left')
    handles = [plt.Rectangle((0, 0), 1, 1, color=REF), plt.Line2D([], [], color=AS_IS, lw=2), plt.Line2D([], [], color=CORR, lw=2),
               plt.Rectangle((0, 0), 1, 1, color=MUTED, alpha=0.15)]
    fig.legend(handles, ['ok rows (reference)', 'flagged rows, harmonized now', 'flagged rows, corrected to the suggested unit',
                         'core of the ok rows'], loc='lower left', ncol=4, frameon=False, fontsize=8,
               bbox_to_anchor=(0.01, 0.3 / H if len(left_out) else 0.02))
    fig.suptitle(f"{test} / OMOP {omop}: rows with a suggested unit, now and corrected (shaded: core)", fontsize=10, color=INK, x=0.01, ha='left')
    if len(left_out):
        items = [f"{r.unit} -> {r.suggested_unit} ({int(r.rows):,} rows, {'not harmonized now' if pd.isna(r.pct_as_is_in_core) else 'too few rows'}"
                 + ('' if pd.isna(r.pct_corrected_in_core) else f", {r.pct_corrected_in_core:g}% in core corrected") + ')'
                 for r in left_out.itertuples(index=False)]
        fig.text(0.01, 0.06 / H, 'Not drawn: ' + '; '.join(items), fontsize=7.5, color=MUTED, ha='left', va='bottom', wrap=True)
    fig.savefig(path, dpi=130, facecolor=SURFACE)
    plt.close(fig)
    log(f"Wrote {path}")


plot(args.dir / f"ref_unit_check{sfx}.png")
meta['flags'] = {'created': datetime.now().isoformat(timespec='seconds'), 'script': str(Path(__file__).resolve()),
                 'injected_rows': str(inj.resolve()), 'margin': args.margin, 'all_rows': args.all_rows, 'min_n': args.min_n,
                 'core_sd': args.core_sd, 'far': args.far, 'rows_by_status': {k: int(v) for k, v in by_status.items()},
                 'unit_factors': fac, 'core_all': {'median': med_c, 'lo': lo_c, 'hi': hi_c}}
META.write_text(json.dumps(meta, indent=2, ensure_ascii=False) + '\n')
log(f"Added the run to {META}")

pd.set_option('display.width', 250)
pd.set_option('display.max_rows', 300)
print("\nRows per test x unit x status:")
print(summ.to_string(index=False))
flagged = out[~out['range_status'].isin(['ok', 'no_range']) & (out['n'] >= args.min_n)]
print("\nFlagged signatures (largest first):")
print(flagged.sort_values('n', ascending=False).head(40).to_string(index=False))
print("\nCheck against the values (ok rows per unit; every other status per unit, flagged ones per unit -> suggested unit):")
print(rng.drop(columns=['top_ranges']).to_string(index=False))
