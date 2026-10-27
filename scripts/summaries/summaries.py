#!/usr/bin/env python3
"""Per test name x unit summary tables (port of summaries.R), safe to share: every row has more than --min-subjects people.

MEASUREMENT_UNIT is the unit right after the primary injection step (cleaned-post-inj::MEASUREMENT_UNIT, release
spelling MEASUREMENT_UNIT_POST_INJ), so the input must be the engine's main output (or a release built with that
column). Missing values are written as '' in every table.

  [out] summaryTest          TEST_NAME, MEASUREMENT_UNIT, n_records, n_subjects
  [out] summaryUnitSource    ... unit_source (Source, PrimaryInjection, SecondaryCorrection; rows with a final unit)
  [out] summaryValuesSource  ... value_source (Source, Extracted, QCOut; rows with a value)
  [out] summaryValues        TEST_NAME, MEASUREMENT_UNIT, n_subjects, n_records (both per decile: total // 10),
                             decile (0.1 .. 0.9), decile_MEASUREMENT_VALUE (smallest value with >= decile of the records
                             at or below it); only test x unit combos with n_subjects // 10 > --min-subjects
  [out] summaryOutcomes      TEST_NAME, MEASUREMENT_UNIT, TEST_OUTCOME, n_TEST_OUTCOME, n_subjects
  [out] summaryReference     TEST_NAME, MEASUREMENT_UNIT, DEMOGRAPHICS_BIN (Adult-Male, Adult-Female, Minor-Male,
                             Minor-Female; minor = age < --adult-age), REFERENCE_RANGE_LOW_MEAN, REFERENCE_RANGE_HIGH_MEAN
                             (means over the records with that limit), n_records (with any limit), n_subjects

  [cache] value_counts       TEST_NAME x unit x value -> records, for the deciles (row-level detail: not for export)

People are counted in two plain group-bys (key x person, then key), which spill to disk, instead of count(distinct).
Each file is rebuilt when missing, older than the input, or with --force; --test uses everyone whose FINNGENID hashes to
1 in --test-every (default 30, ~10M of 276M rows), the same rows in every table (_test names).

Usage
-----
  python3 scripts/summaries/summaries.py --input <engine_output>.parquet [--test]
"""
import argparse, os, time
from pathlib import Path
import duckdb

# short name -> (engine output column, release column)
COLUMNS = {
    'f': ('FINNGENID', 'FINNGENID'), 'test_name': ('TEST_NAME_ABBREVIATION', 'TEST_NAME'),
    'unit': ('MEASUREMENT_UNIT', 'MEASUREMENT_UNIT_CLEANED'),
    'unit_post_inj': ('cleaned-post-inj::MEASUREMENT_UNIT', 'MEASUREMENT_UNIT_POST_INJ'),
    'unit_pre_inj': ('cleaned-pre-inj::MEASUREMENT_UNIT', 'MEASUREMENT_UNIT_PRE_INJ'),
    'unit_extracted': ('IS_UNIT_EXTRACTED', 'IS_UNIT_EXTRACTED'),
    'v': ('MEASUREMENT_VALUE', 'MEASUREMENT_VALUE_CLEANED'), 'v_extracted': ('IS_VALUE_EXTRACTED', 'IS_VALUE_EXTRACTED'),
    'qc_pass': ('QC_PASS', 'QC_PASS'), 'outcome': ('TEST_OUTCOME', 'TEST_OUTCOME'),
    'sex': ('SEX', 'SEX'), 'age': ('EVENT_AGE', 'EVENT_AGE'),
    'ref_low': ('REFERENCE_RANGE_LOWER_VALUE', 'REFERENCE_RANGE_LOW_VALUE'),
    'ref_high': ('REFERENCE_RANGE_UPPER_VALUE', 'REFERENCE_RANGE_HIGH_VALUE'),
}
TABLES = ('Test', 'UnitSource', 'ValuesSource', 'Values', 'Outcomes', 'Reference')

RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', required=True, type=Path, help="Engine output parquet (needs the post-injection unit)")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/javier_counts'),
                help="Exportable summary tables (default: /mnt/disks/data/kanta/javier_counts)")
ap.add_argument('--out-cache', type=Path, default=Path('/mnt/disks/data/kanta/tmp/summaries'),
                help="Row-level and intermediate files, not for export; duckdb spills to <out-cache>/duckdb "
                     "(default: /mnt/disks/data/kanta/tmp/summaries)")
ap.add_argument('--min-subjects', type=int, default=5, help="Keep only rows with more than this many people (default: 5)")
ap.add_argument('--adult-age', type=float, default=18, help="Age from which a record is Adult rather than Minor (default: 18)")
ap.add_argument('--force', action='store_true', help="Rebuild every file")
ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                help=f"duckdb memory limit; it spills beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads (default: min(8, cores))")
ap.add_argument('--test', action='store_true', help="Build from a fixed sample of people (see --test-every); names get a _test suffix")
ap.add_argument('--test-every', type=int, default=30, help="--test keeps 1 in this many people (default: 30)")
args = ap.parse_args()
args.out.mkdir(parents=True, exist_ok=True)
TMP = args.out_cache / 'duckdb'
TMP.mkdir(parents=True, exist_ok=True)
t0 = time.time()
log = lambda msg: print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)

con = duckdb.connect()
con.sql(f"set temp_directory = '{TMP}'")
con.sql("set preserve_insertion_order = false")
con.sql(f"set memory_limit = '{args.memory_limit}'")
con.sql(f"set threads = {args.threads}")
log(f"duckdb: memory limit {args.memory_limit} (machine has {RAM_GB:.0f}GB), {args.threads} threads, spilling to {TMP}")
log(f"summary tables -> {args.out}; row-level files -> {args.out_cache}")

sfx = '_test' if args.test else ''
F = {name: args.out / f"summary{name}{sfx}.tsv" for name in TABLES}
VC = args.out_cache / f"value_counts{sfx}.parquet"
TSV = "format csv, delimiter '\t', header true, nullstr ''"


def stale(out, deps):
    return args.force or not out.exists() or any(out.stat().st_mtime < d.stat().st_mtime for d in deps)


def build(out, deps, sql, options='format parquet'):
    """Write `sql` to `out` unless it is newer than all `deps`."""
    if not stale(out, deps):
        log(f"Cached {out.name}")
        return
    part = out.with_name(out.name + '.part')
    part.unlink(missing_ok=True)
    con.sql(f"copy ({sql}) to '{part}' ({options})")
    os.replace(part, out)
    log(f"Built {out.name}")


# --- Input columns (engine or release spelling), cleaned: "NA"/'' -> null
desc = {r[0] for r in con.sql(f"describe select * from read_parquet('{args.input}')").fetchall()}


def col(name):
    c = next((c for c in COLUMNS[name] if c in desc), None)
    if c is None:
        raise SystemExit(f"{args.input}: missing column {' / '.join(COLUMNS[name])}")
    return f'"{c}"'


# --test: whole people, so every table sees the same rows (a plain limit picks different rows on each pass)
src = f"(select * from read_parquet('{args.input}')" + (f" where hash({col('f')}) % {args.test_every} = 0)" if args.test else ')')
s = lambda name: f"nullif(nullif(trim(cast({col(name)} as varchar)), ''), 'NA')"
num = lambda name: f"try_cast(replace({s(name)}, ',', '.') as double)"
flag = lambda name: f"coalesce(lower({s(name)}) in ('1', 'true'), false)"

# TEST_NAME keeps a literal "NA" (an unmapped name is not missing data)
test = f"trim(cast({col('test_name')} as varchar))"
unit = s('unit_post_inj')
person = f"hash({s('f')})"
v = num('v')
unit_source = f"""case when {s('unit')} is null then null
                       when {s('unit')} is distinct from {s('unit_post_inj')} then 'SecondaryCorrection'
                       when {s('unit_post_inj')} is distinct from {s('unit_pre_inj')} then 'PrimaryInjection'
                       when {flag('unit_extracted')} then 'PrimaryInjection'
                       else 'Source' end"""
value_source = f"""case when {v} is null then null when try_cast({s('qc_pass')} as tinyint) = 0 then 'QCOut'
                        when {flag('v_extracted')} then 'Extracted' else 'Source' end"""
sex = f"case lower({s('sex')}) when 'male' then 'Male' when 'm' then 'Male' when 'female' then 'Female' when 'f' then 'Female' end"
demo = f"(case when {num('age')} < {args.adult_age} then 'Minor' when {num('age')} >= {args.adult_age} then 'Adult' end) || '-' || {sex}"


def counts(keys, where='true', n_name='n_records'):
    """n_records and n_subjects per `keys` ({output name: expression}), people counted as key x person -> key."""
    names = ', '.join(keys)
    return f"""
    with tf as (select {', '.join(f'{e} as {k}' for k, e in keys.items())}, {person} f, count(*) n
                from {src} where {s('f')} is not null and ({where}) group by all)
    select {names}, sum(n)::bigint {n_name}, count(*)::bigint n_subjects
    from tf group by all having count(*) > {args.min_subjects} order by {names}"""


TU = {'TEST_NAME': test, 'MEASUREMENT_UNIT': unit}
deps = [args.input]
build(F['Test'], deps, counts(TU), TSV)
build(F['UnitSource'], deps, counts(TU | {'unit_source': unit_source}, f"{s('unit')} is not null"), TSV)
build(F['ValuesSource'], deps, counts(TU | {'value_source': value_source}, f"{v} is not null"), TSV)
build(F['Outcomes'], deps, counts(TU | {'TEST_OUTCOME': s('outcome')}, f"{s('outcome')} is not null", 'n_TEST_OUTCOME'), TSV)

# --- Reference ranges: sums carried through the person stage so the means are over records
build(F['Reference'], deps, f"""
    with tf as (select {test} TEST_NAME, {unit} MEASUREMENT_UNIT, {demo} DEMOGRAPHICS_BIN, {person} f, count(*) n,
                       sum({num('ref_low')}) sum_low, count({num('ref_low')}) n_low,
                       sum({num('ref_high')}) sum_high, count({num('ref_high')}) n_high
                from {src} where {s('f')} is not null and {demo} is not null
                                 and ({num('ref_low')} is not null or {num('ref_high')} is not null)
                group by all)
    select TEST_NAME, MEASUREMENT_UNIT, DEMOGRAPHICS_BIN,
           round(sum(sum_low) / nullif(sum(n_low), 0), 6) REFERENCE_RANGE_LOW_MEAN,
           round(sum(sum_high) / nullif(sum(n_high), 0), 6) REFERENCE_RANGE_HIGH_MEAN,
           sum(n)::bigint n_records, count(*)::bigint n_subjects
    from tf group by all having count(*) > {args.min_subjects}
    order by TEST_NAME, MEASUREMENT_UNIT, DEMOGRAPHICS_BIN""", TSV)

# --- Deciles from cumulative counts over the (much smaller) test x unit x value table; null keys are '' here so the
# joins match them, and null again in the output
build(VC, deps, f"select coalesce({test}, '') test, coalesce({unit}, '') unit, {v} v, count(*) n from {src} where {v} is not null group by all")
dcols = ', '.join(f"min(v) filter (where cum * 10 >= {q} * tot) d{q}" for q in range(1, 10))
build(F['Values'], [*deps, VC], f"""
    with tf as (select coalesce({test}, '') test, coalesce({unit}, '') unit, {person} f, count(*) n
                from {src} where {s('f')} is not null and {v} is not null group by all),
         p as (select test, unit, count(*) // 10 n_subjects, sum(n)::bigint // 10 n_records
               from tf group by all having count(*) // 10 > {args.min_subjects}),
         c as (select vc.*, sum(n) over (partition by test, unit order by v rows unbounded preceding) cum,
                            sum(n) over (partition by test, unit) tot
               from read_parquet('{VC}') vc semi join p using (test, unit)),
         d as (select test, unit, {dcols} from c group by all),
         l as (unpivot d on {', '.join(f'd{q}' for q in range(1, 10))} into name k value x)
    select nullif(test, '') TEST_NAME, nullif(unit, '') MEASUREMENT_UNIT, n_subjects::bigint n_subjects, n_records::bigint n_records,
           cast(substr(k, 2) as integer) / 10 decile, x decile_MEASUREMENT_VALUE
    from l join p using (test, unit) order by TEST_NAME, MEASUREMENT_UNIT, decile""", TSV)

# --- Check: no table has a row with too few people
for name, path in F.items():
    rows, low = con.sql(f"select count(*), count(*) filter (where try_cast(n_subjects as bigint) <= {args.min_subjects}) "
                        f"from read_csv('{path}', delim = '\t', header = true, all_varchar = true)").fetchone()
    print(f"  {path.name:30s} {rows:>10,} rows  {path.stat().st_size / 2**20:8.2f} MB")
    if low:
        raise SystemExit(f"{path}: {low} rows with n_subjects <= {args.min_subjects}")
log("Done")
