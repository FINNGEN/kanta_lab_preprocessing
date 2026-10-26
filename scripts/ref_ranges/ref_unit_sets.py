#!/usr/bin/env python3
"""Learn the reference range of each unit, per test name x OMOP x sex x age group, from the rows whose unit is trusted.

Assumptions: (1) the OMOP mapping is right, so a row whose harmonized value sits in the core of its test's
distribution has the right MEASUREMENT_UNIT_CLEANED; (2) a row's reference range is on the scale of its source value,
even when the declared unit is wrong (in the unit and in the reference-range unit alike).

Steps 1-2 are cached in --tmp, always for every test (rebuilt only when missing or older than the input or --limits,
or with --force), so runs on different tests or pairs reuse them; steps 3-4 take seconds and run every time:
1. inject   A slim copy of the input rows (every test), as if the engine had already filled the reference-range columns: rows
            without numeric limits get lower/upper from --limits (*_LIMITS.tsv from ref_text_limits.py, statuses
            range/upper/lower), matched on test name + REFERENCE_RANGE_TEXT (else REFERENCE_RANGE_GROUP), as in
            ref_text_table.py. Release column names: ROWID, TEST_NAME, OMOP_CONCEPT_ID (null when unmapped or 0),
            OMOP_QUANTITY, SEX, EVENT_AGE, MEASUREMENT_VALUE_CLEANED, MEASUREMENT_UNIT_CLEANED, MEASUREMENT_VALUE_HARMONIZED,
            MEASUREMENT_UNIT_HARMONIZED, REFERENCE_RANGE_LOW_VALUE, REFERENCE_RANGE_HIGH_VALUE, REFERENCE_RANGE_SOURCE
            (num / text / null). Written as a folder split into 64 buckets by test name (bucket=hash(TEST_NAME) % 64),
            so ref_unit_flag.py reads only its test's bucket.
2. hist     test x OMOP x sex x age group x unit x lower x upper x 0.002-log10 bin of the harmonized value -> rows.
            A test name can map to several OMOP concepts (e.g. % and count), with harmonized values of different
            quantities, so everything from here on is per test x OMOP (--omop: only these concepts).
3. core     Per test x OMOP x stratum (F/M x minor/adult, and all/all pooled over every row): the rows whose log10
            harmonized value is within median +- --core-sd x 1.4826 x MAD (a robust 1 SD by default), from the bins.
4. sets     Per test x OMOP x stratum x unit, over the core rows with a range: weighted median of the lower limits (> 0) and
            of the upper limits (> 0), with 5th/95th percentiles. Used (use = true) when it has >= --min-rows rows and
            >= --min-pct % of the stratum's core rows with a range, and neither limit spans >= 10x between its
            percentiles (mixed_scale: the core let rows of another scale in).

Outputs, one folder per test name x OMOP in --out (--out/<test name>/<OMOP id>/, '/' in a name becomes '_';
aggregates only, groups of >= --min-n rows; with --test the file names end in _test):
- ref_unit_core.tsv  test, omop, sex, age, rows, median, core_lo, core_hi (harmonized), core_rows, core_rows_with_range
- ref_unit_sets.tsv  test, omop, sex, age, unit, rows, pct_of_core, n_lower, lower, lower_p05, lower_p95, n_upper, upper,
                     upper_p05, upper_p95, use, note
- metadata.json      the run: test name, OMOP id, quantity and harmonized unit, when, script, input and limits files
                     (with modification times), injected rows file, parameters, strata, rows by range source (of the
                     OMOP, and of the test's unmapped rows), the test's other OMOP ids, learned units
"""
import argparse, json, os, shutil, time
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# short name -> (engine output column, release column)
COLUMNS = {
    'rowid': ('ROWID', 'ROWID'),
    'test_name': ('TEST_NAME_ABBREVIATION', 'TEST_NAME'),
    'omop': ('harmonization_omop::OMOP_ID', 'OMOP_CONCEPT_ID'),
    'quantity': ('harmonization_omop::OMOP_QUANTITY', 'OMOP_QUANTITY'),
    'h_unit': ('harmonization_omop::MEASUREMENT_UNIT', 'MEASUREMENT_UNIT_HARMONIZED'),
    'sex': ('SEX', 'SEX'),
    'age': ('EVENT_AGE', 'EVENT_AGE'),
    'v_raw': ('MEASUREMENT_VALUE', 'MEASUREMENT_VALUE_CLEANED'),
    'unit': ('MEASUREMENT_UNIT', 'MEASUREMENT_UNIT_CLEANED'),
    'v': ('harmonization_omop::MEASUREMENT_VALUE', 'MEASUREMENT_VALUE_HARMONIZED'),
    'ref_low': ('REFERENCE_RANGE_LOWER_VALUE', 'REFERENCE_RANGE_LOW_VALUE'),
    'ref_high': ('REFERENCE_RANGE_UPPER_VALUE', 'REFERENCE_RANGE_HIGH_VALUE'),
    'ref_text': ('REFERENCE_RANGE_TEXT', 'REFERENCE_RANGE_TEXT'),
    'ref_group': ('REFERENCE_RANGE_GROUP', 'REFERENCE_RANGE_GROUP'),
}
OPTIONAL = {'rowid', 'quantity', 'h_unit'}  # left null when the input lacks them
NBUCKETS = 64  # injected rows: folders bucket=0..63 by hash(TEST_NAME)
BIN = 0.002  # log10 width of the harmonized-value bins (0.5%: fine enough for narrow tests like hematocrit)
TEST_ROWS = 10_000_000

RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
D = Path('/mnt/disks/data/kanta/ref')
ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', type=Path, default=Path('/mnt/disks/data/kanta/engine/kanta_dev_2026_10_15_RELEASE.parquet'),
                help="Engine output (or release) parquet (default: /mnt/disks/data/kanta/engine/kanta_dev_2026_10_15_RELEASE.parquet)")
ap.add_argument('--limits', type=Path, default=D / 'ref_text_table_LIMITS.tsv', help=f"*_LIMITS.tsv from ref_text_limits.py (default: {D / 'ref_text_table_LIMITS.tsv'})")
ap.add_argument('--tests', help="Comma-separated test names (default: all)")
ap.add_argument('--omop', help="Comma-separated OMOP ids to learn ranges for (default: every OMOP id of the tests)")
ap.add_argument('--pairs', type=Path, help="TSV with TEST_NAME and OMOP_CONCEPT_ID columns: learn ranges for exactly these test x OMOP pairs "
                                         "(instead of --tests / --omop)")
ap.add_argument('--out', type=Path, default=D / 'results', help=f"Output directory, one folder per test x OMOP in it (default: {D / 'results'})")
ap.add_argument('--core-sd', type=float, default=1.0, help="Core = median +- this many robust SDs (1.4826 x MAD) of log10 harmonized value (default: 1)")
ap.add_argument('--min-rows', type=int, default=100, help="Rows a unit's learned range needs, per stratum (default: 100)")
ap.add_argument('--min-pct', type=float, default=1.0, help="%% of the stratum's core rows with a range a unit needs (default: 1)")
ap.add_argument('--min-n', type=int, default=5, help="Groups with fewer rows are left out of the outputs (default: 5)")
ap.add_argument('--tmp', type=Path, default=Path('/mnt/disks/data/kanta/tmp/ref_unit'),
                help="Cached files and duckdb spill folder (default: /mnt/disks/data/kanta/tmp/ref_unit)")
ap.add_argument('--force', action='store_true', help="Rebuild every cached file")
ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads; fewer threads lower peak memory (default: min(8, cores))")
ap.add_argument('--test', action='store_true', help=f"Read only the first {TEST_ROWS:,} input rows; files and outputs get a _test suffix")
args = ap.parse_args()
args.out.mkdir(parents=True, exist_ok=True)
(args.tmp / 'duckdb').mkdir(parents=True, exist_ok=True)
t0 = time.time()
log = lambda msg: print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)

con = duckdb.connect()
con.sql(f"set temp_directory = '{args.tmp / 'duckdb'}'")
con.sql("set preserve_insertion_order = false")
con.sql(f"set memory_limit = '{args.memory_limit}'")
con.sql(f"set threads = {args.threads}")
log(f"duckdb: memory limit {args.memory_limit} (machine has {RAM_GB:.0f}GB), {args.threads} threads, spilling to {args.tmp / 'duckdb'}")

pairs = None
if args.pairs:
    if args.tests or args.omop:
        raise SystemExit("--pairs replaces --tests and --omop")
    pairs = pd.read_csv(args.pairs, sep='\t', dtype=str, keep_default_na=False)[['TEST_NAME', 'OMOP_CONCEPT_ID']]
    pairs.columns = ['test', 'omop']
    tests = sorted(set(pairs['test']))
else:
    tests = sorted({t.strip() for t in args.tests.split(',') if t.strip()}) if args.tests else []
sfx = '_test' if args.test else ''
tag = f"{args.input.stem}{sfx}"
INJ = args.tmp / f"ref_injected_{tag}"  # folder of bucket=<n>/ parquet files
HIST = args.tmp / f"ref_unit_hist_{tag}.parquet"
INFO = args.tmp / f"ref_unit_info_{tag}.parquet"  # rows per test x OMOP x range source, with quantity and harmonized unit


def pq(path):
    """read_parquet() of a parquet file, or of a folder of bucket=<n>/ files."""
    return f"read_parquet('{path}/*/*.parquet', hive_partitioning = true)" if path.is_dir() else f"read_parquet('{path}')"


def stale(out, deps, need=None):
    """True when `out` must be rebuilt: --force, missing, older than a dep, or lacking column `need`."""
    if args.force or not out.exists() or any(out.stat().st_mtime < d.stat().st_mtime for d in deps):
        return True
    return need is not None and need not in con.sql(f"select * from {pq(out)} limit 0").columns


def build(out, deps, sql, need=None, partition=None):
    """Write `sql` to parquet `out` (a folder split by column `partition`, if given) unless it is newer than all
    `deps` (make-like); written under a temporary name, then renamed."""
    if not stale(out, deps, need):
        log(f"Cached {out.name}")
        return
    log(f"Building {out.name} ...")
    part = out.with_name(out.name + '.part')
    shutil.rmtree(part, ignore_errors=True) if part.is_dir() else part.unlink(missing_ok=True)
    con.sql(f"copy ({sql}) to '{part}' (format parquet" + (f", partition_by ({partition}))" if partition else ')'))
    if out.is_dir():
        shutil.rmtree(out)
    elif out.exists():
        out.unlink()
    os.replace(part, out)
    log(f"Built {out.name}")


# --- 1. Inject the parsed text limits into the rows without numeric limits
src = f"(select * from read_parquet('{args.input}')" + (f" limit {TEST_ROWS})" if args.test else ')')
cols = set(con.sql(f"select * from read_parquet('{args.input}') limit 0").columns)


def col(name):
    """The input's column for `name` (engine or release spelling), as a cleaned string ("NA"/'' -> null)."""
    c = next((c for c in COLUMNS[name] if c in cols), None)
    if c is None:
        if name in OPTIONAL:
            return 'null'
        raise SystemExit(f"{args.input}: missing column {COLUMNS[name][0]} / {COLUMNS[name][1]}")
    return f"nullif(nullif(trim(cast(\"{c}\" as varchar)), ''), 'NA')"


num = lambda name: f"try_cast(replace({col(name)}, ',', '.') as double)"
# read like ref_text_limits.py wrote it, so the texts compare equal to the input's
lim = pd.read_csv(args.limits, sep='\t', dtype=str, keep_default_na=False, quoting=3, escapechar='\\')
lim = lim[lim['status'].isin(['range', 'upper', 'lower'])]
lim = pd.DataFrame({'test_name': lim['test_name'], 'ref_text': lim['ref_text'],
                    'lower': pd.to_numeric(lim['lower'], errors='coerce'), 'upper': pd.to_numeric(lim['upper'], errors='coerce')})
con.register('lim_df', lim)
log(f"{len(lim):,} test name x text pairs with limits in {args.limits.name}")
has_num = f"({num('ref_low')} is not null or {num('ref_high')} is not null)"
build(INJ, [args.input, args.limits], f"""
    with r as (
        select try_cast({col('rowid')} as bigint) as ROWID, coalesce({col('test_name')}, 'NA') as TEST_NAME,
               nullif({col('omop')}, '0') as OMOP_CONCEPT_ID, {col('quantity')} as OMOP_QUANTITY, {col('sex')} as SEX, try_cast({col('age')} as double) as EVENT_AGE,
               {num('v_raw')} as MEASUREMENT_VALUE_CLEANED, {col('unit')} as MEASUREMENT_UNIT_CLEANED,
               {num('v')} as MEASUREMENT_VALUE_HARMONIZED, {col('h_unit')} as MEASUREMENT_UNIT_HARMONIZED, {num('ref_low')} as lo, {num('ref_high')} as hi, {has_num} as has_num,
               regexp_replace(coalesce({col('ref_text')}, {col('ref_group')}), '[\\t\\r\\n]+', ' ', 'g') as txt
        from {src})
    select hash(r.TEST_NAME) % {NBUCKETS} as bucket, r.ROWID, r.TEST_NAME, r.OMOP_CONCEPT_ID, r.OMOP_QUANTITY, r.SEX, r.EVENT_AGE, r.MEASUREMENT_VALUE_CLEANED,
           r.MEASUREMENT_UNIT_CLEANED, r.MEASUREMENT_VALUE_HARMONIZED, r.MEASUREMENT_UNIT_HARMONIZED,
           case when r.has_num then r.lo else l.lower end as REFERENCE_RANGE_LOW_VALUE,
           case when r.has_num then r.hi else l.upper end as REFERENCE_RANGE_HIGH_VALUE,
           case when r.has_num then 'num' when l.lower is not null or l.upper is not null then 'text' end as REFERENCE_RANGE_SOURCE
    from r left join lim_df l on l.test_name = r.TEST_NAME and l.ref_text = r.txt""", need='OMOP_CONCEPT_ID', partition='bucket')
I = pq(INJ)
build(INFO, [INJ], f"""select TEST_NAME, OMOP_CONCEPT_ID omop, coalesce(REFERENCE_RANGE_SOURCE, 'none') src, count(*) n,
                              mode(OMOP_QUANTITY) quantity, mode(MEASUREMENT_UNIT_HARMONIZED) harmonized_unit
                       from {I} group by all""")
info = con.sql(f"select * from read_parquet('{INFO}')").df()
if tests:
    info = info[info['TEST_NAME'].isin(tests)]
src_rows = info[['TEST_NAME', 'omop', 'src', 'n']]
omop_info = (info[info['omop'].notna()].sort_values('n', ascending=False).groupby(['TEST_NAME', 'omop'])[['quantity', 'harmonized_unit']]
             .first().reset_index().rename(columns={'TEST_NAME': 'test'}))
for src_name, n in src_rows.groupby('src')['n'].sum().sort_values(ascending=False).items():
    log(f"  range from {src_name}: {n:,} rows ({100 * n / src_rows['n'].sum():.1f}%)")

# --- 2. Histogram of the harmonized values by stratum, unit and range
SEX = "case when lower(SEX) in ('male', 'm') then 'M' when lower(SEX) in ('female', 'f') then 'F' end"
AGE = "case when EVENT_AGE < 18 then 'minor' when EVENT_AGE >= 18 then 'adult' end"
build(HIST, [INJ], f"""
    select TEST_NAME test, OMOP_CONCEPT_ID omop, {SEX} sex, {AGE} age, coalesce(MEASUREMENT_UNIT_CLEANED, 'NA') unit,
           REFERENCE_RANGE_LOW_VALUE lower, REFERENCE_RANGE_HIGH_VALUE upper,
           floor(log10(MEASUREMENT_VALUE_HARMONIZED) / {BIN})::integer hbin, count(*) n
    from {I} where MEASUREMENT_VALUE_HARMONIZED > 0 and OMOP_CONCEPT_ID is not null group by all""")

# --- 3. Core per test x OMOP x stratum: the strata, plus all/all pooled over every row
omops = sorted({o.strip() for o in args.omop.split(',') if o.strip()}) if args.omop else []
where = ([f"test in ({', '.join(repr(t) for t in tests)})"] if tests else []) + \
        ([f"omop in ({', '.join(repr(o) for o in omops)})"] if omops else [])
H = f"(select * from read_parquet('{HIST}')" + (f" where {' and '.join(where)})" if where else ')')
if pairs is not None:
    con.register('pairs_df', pairs)
    H = f"(select h.* from read_parquet('{HIST}') h semi join pairs_df p on h.test = p.test and h.omop = p.omop)"
con.sql(f"""create temp table s as
    select test, omop, sex, age, unit, lower, upper, hbin, n from {H} where sex is not null and age is not null
    union all
    select test, omop, 'all', 'all', unit, lower, upper, hbin, sum(n) from {H} group by all""")
if not con.sql("select count(*) from s").fetchone()[0]:
    raise SystemExit("No rows with a harmonized value for the chosen tests / OMOP ids / pairs")


def wquant(table, keys, val, qs, where='true'):
    """Weighted quantiles of `val` (weights n) per `keys` from `table`: columns <val>_q<qq> for each q in `qs`."""
    sel = ', '.join(f"min({val}) filter (where cum >= {q} * tot) {val.split('.')[-1]}_q{round(q * 100):02d}" for q in qs)
    return f"""select {keys}, {sel} from (
        select {keys}, {val}, sum(n) over (partition by {keys} order by {val} rows unbounded preceding) cum,
               sum(n) over (partition by {keys}) tot
        from (select {keys}, {val}, sum(n) n from {table} where {where} group by all)) group by all"""


K = 'test, omop, sex, age'
con.sql(f"create temp table med as {wquant('s', K, 'hbin', [0.5])}")
con.sql(f"""create temp table mad as {wquant(f"(select s.*, abs(s.hbin - m.hbin_q50) dev from s join med m using ({K}))", K, 'dev', [0.5])}""")
con.sql(f"""create temp table core as
    select m.test, m.omop, m.sex, m.age, m.hbin_q50 med, {args.core_sd} * 1.4826 * greatest(d.dev_q50, 1) halfw
    from med m join mad d using ({K})""")
con.sql(f"""create temp table srow as
    select s.*, abs(s.hbin - c.med) <= c.halfw in_core from s join core c using ({K})""")

core_out = con.sql(f"""
    select test, omop, sex, age, sum(n) as rows,
           round(pow(10, (any_value(c.med) + 0.5) * {BIN}), 4) median,
           round(pow(10, (any_value(c.med) + 0.5 - any_value(c.halfw)) * {BIN}), 4) core_lo,
           round(pow(10, (any_value(c.med) + 0.5 + any_value(c.halfw)) * {BIN}), 4) core_hi,
           coalesce(sum(n) filter (where in_core), 0) core_rows,
           coalesce(sum(n) filter (where in_core and (lower is not null or upper is not null)), 0) core_rows_with_range
    from srow join core c using ({K}) group by test, omop, sex, age
    having sum(n) >= {args.min_n} order by test, omop, sex = 'all', sex, age""").df()

# --- 4. Learned range per test x stratum x unit, over the core rows with a range
con.sql("""create temp table cr as select test, omop, sex, age, unit, lower, upper, sum(n) n from srow
           where in_core and (lower is not null or upper is not null) group by all""")
QS = [0.05, 0.5, 0.95]
sets = con.sql(f"""
    with tot as (select test, omop, sex, age, sum(n) core_n from cr group by all),
         u as (select test, omop, sex, age, unit, sum(n) as rows, sum(n) filter (where lower > 0) n_lower,
                      sum(n) filter (where upper > 0) n_upper from cr group by all),
         lo as ({wquant('cr', K + ', unit', 'lower', QS, 'lower > 0')}),
         hi as ({wquant('cr', K + ', unit', 'upper', QS, 'upper > 0')})
    select test, omop, sex, age, unit, rows, round(100 * rows / core_n, 2) pct_of_core,
           coalesce(n_lower, 0) n_lower, lower_q50 lower, lower_q05 lower_p05, lower_q95 lower_p95,
           coalesce(n_upper, 0) n_upper, upper_q50 upper, upper_q05 upper_p05, upper_q95 upper_p95
    from u join tot using ({K}) left join lo using ({K}, unit) left join hi using ({K}, unit)
    where rows >= {args.min_n} order by test, omop, sex = 'all', sex, age, rows desc""").df()
mixed = (sets['upper_p95'] / sets['upper_p05'] >= 10) | (sets['lower_p95'] / sets['lower_p05'] >= 10)
low = (sets['rows'] < args.min_rows) | (sets['pct_of_core'] < args.min_pct)
sets['note'] = [';'.join(n for n, f in (('low_support', a), ('mixed_scale', b)) if f) for a, b in zip(low, mixed)]
sets['use'] = ~low & ~mixed

count_cols = ['rows', 'core_rows', 'core_rows_with_range', 'n_lower', 'n_upper']
for df in (core_out, sets):
    for c in set(count_cols) & set(df.columns):
        df[c] = df[c].fillna(0).astype('int64')
mtime = lambda p: datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec='seconds')
run = {
    'created': datetime.now().isoformat(timespec='seconds'), 'script': str(Path(__file__).resolve()),
    'input': str(args.input.resolve()), 'input_modified': mtime(args.input),
    'limits': str(args.limits.resolve()), 'limits_modified': mtime(args.limits),
    'injected_rows': str(INJ.resolve()), 'injected_buckets': NBUCKETS,
    **({'pairs_in_run': str(args.pairs.resolve())} if pairs is not None else {'tests_in_run': tests or 'all', 'omop_in_run': omops or 'all'}),
    'test_rows_only': args.test,
    'parameters': {'core_sd': args.core_sd, 'min_rows': args.min_rows, 'min_pct': args.min_pct, 'min_n': args.min_n,
                   'bin_log10': BIN, 'mixed_scale_factor': 10},
    'strata': {'sex': 'F / M from SEX', 'age': 'minor (EVENT_AGE < 18) / adult (>= 18)', 'pooled': 'all/all, every row'},
}
by_src = lambda df: {k: int(v) for k, v in df.groupby('src')['n'].sum().items()}
for test, omop in sorted(set(zip(core_out['test'], core_out['omop'])) | set(zip(sets['test'], sets['omop']))):
    d = args.out / test.replace('/', '_') / str(omop)
    d.mkdir(parents=True, exist_ok=True)
    for df, name in ((core_out, 'ref_unit_core'), (sets, 'ref_unit_sets')):
        df[(df['test'] == test) & (df['omop'] == omop)].to_csv(d / f"{name}{sfx}.tsv", sep='\t', index=False)
    t_sets = sets[(sets['test'] == test) & (sets['omop'] == omop)]
    t_src = src_rows[src_rows['TEST_NAME'] == test]
    oi = omop_info[(omop_info['test'] == test) & (omop_info['omop'] == omop)].iloc[0]
    meta = {'test_name': test, 'omop_id': omop, 'omop_quantity': oi['quantity'], 'harmonized_unit': oi['harmonized_unit'],
            **run,
            'rows_by_range_source': by_src(t_src[t_src['omop'] == omop]),
            'unmapped_rows_by_range_source': by_src(t_src[t_src['omop'].isna()]),
            'other_omop_ids': {o: int(n) for o, n in t_src[t_src['omop'].notna() & (t_src['omop'] != omop)].groupby('omop')['n'].sum().items()},
            'learned_units': sorted(t_sets.loc[t_sets['use'], 'unit'].unique().tolist()),
            'unused_units': sorted(set(t_sets['unit']) - set(t_sets.loc[t_sets['use'], 'unit']))}
    (d / f"metadata{sfx}.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str) + '\n')
    log(f"Wrote {d}/ (core {len(core_out[(core_out['test'] == test) & (core_out['omop'] == omop)])} rows, sets {len(t_sets)} rows, metadata)")
log(f"Injected rows for ref_unit_flag.py: {INJ}")

pd.set_option('display.width', 250)
pd.set_option('display.max_rows', 200)
print("\nCore per test x OMOP x stratum:")
print(core_out.to_string(index=False))
print("\nLearned ranges per test x OMOP x stratum x unit:")
print(sets.to_string(index=False))
