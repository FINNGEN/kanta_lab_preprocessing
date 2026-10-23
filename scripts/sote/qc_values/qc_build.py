#!/usr/bin/env python3
"""Build the QC-by-service-provider datasets: a slim row-level base plus aggregates, all tests in one pass.

--out holds only exportable aggregates: every row stands for at least --min-people (default 5) distinct people and
carries a `people` column (rows below are dropped, not masked). Everything row-level or unmasked goes to --out-cache.

Chain of cached files (each rebuilt when missing or older than what it is built from, or with --force;
written under a temporary name and renamed when complete; --test uses the first 10M input rows, *_test names):

  [cache] base/              one row per result with a value, partitioned by OMOP (base/omop=<id>/*.parquet), so a
                             query on one OMOP reads only its folder. Numbers and 8-byte hashes: rowid (back to the
                             engine output), k_test/k_sp/k_unit/k_note/k_ref (hashes, see qc_lookup), f (hashed
                             FINNGENID), year, month, sex (1 male, 0 female), age, v (harmonized value), v_raw
                             (cleaned value before harmonization), extracted, unit_missing (no unit before
                             injection), decimals, qc_pass, outcome, ref_low/ref_high (numeric limits)
  [cache] qc_lookup_full     kind (test, sp, unit, note, ref) x key -> value (the original string), all strings
  [cache] qc_hist_full       histogram below without the people filter (input of the national limits)
  [cache] qc_national_full   per OMOP: n, p0.1 .. p99.9, Tukey fences (k = --fence-k) on log10, from qc_hist_full
  [cache] qc_extremes        every base row below p0.1 / above p99.9 or outside the fences, with flags and limits

  [out] qc_lookup     the qc_lookup_full rows whose key is used by >= --min-people people
  [out] qc_hist       OMOP x k_test x k_sp x year x extracted x sign x bin -> n, people (harmonized values);
                      bin = floor(100 * log10(|v|)), 0.01 log10 (~2.3%) wide, lower edge sign * 10^(bin/100); sign 0 = zero
  [out] qc_national   qc_national_full for OMOPs with >= --min-people people; a tail quantile q is left empty
                      when fewer than --min-people rows lie beyond it (n * min(q, 1 - q) < --min-people)
  [out] qc_mix        OMOP x k_test x k_sp x year x extracted x dim x cat -> n, people; dim = unit, qc_note, outcome,
                      decimals (category mix) and has_value, qc_fail, unit_missing, has_ref_text, has_ref_num
                      (cat true/false); for unit and qc_note cat is the qc_lookup key as text

Join qc_lookup on (kind, key) for names; SP region/org are in region/sp_region.tsv (column s = SP hash).
"""
import argparse, os, shutil, time
from pathlib import Path
import duckdb

# short name -> (engine output column, release column)
COLUMNS = {
    'rowid': ('ROWID', 'ROWID'), 'f': ('FINNGENID', 'FINNGENID'), 's': ('SERVICE_PROVIDER', 'SERVICE_PROVIDER_ID'),
    'omop': ('harmonization_omop::OMOP_ID', 'OMOP_CONCEPT_ID'), 'test_name': ('TEST_NAME_ABBREVIATION', 'TEST_NAME'),
    'v': ('harmonization_omop::MEASUREMENT_VALUE', 'MEASUREMENT_VALUE_HARMONIZED'),
    'v_raw': ('MEASUREMENT_VALUE', 'MEASUREMENT_VALUE_CLEANED'), 'unit': ('MEASUREMENT_UNIT', 'MEASUREMENT_UNIT_CLEANED'),
    'unit_pre_inj': ('cleaned-pre-inj::MEASUREMENT_UNIT', None), 'extracted': ('IS_VALUE_EXTRACTED', 'IS_VALUE_EXTRACTED'),
    'sex': ('SEX', 'SEX'), 'age': ('EVENT_AGE', 'EVENT_AGE'), 'dt': ('APPROX_EVENT_DATETIME', 'APPROX_EVENT_DATETIME'),
    'outcome': ('TEST_OUTCOME', 'TEST_OUTCOME'), 'qc_pass': ('QC_PASS', 'QC_PASS'), 'qc_notes': ('QC_NOTES', 'QC_NOTES'),
    'ref_low': ('REFERENCE_RANGE_LOWER_VALUE', 'REFERENCE_RANGE_LOW_VALUE'),
    'ref_high': ('REFERENCE_RANGE_UPPER_VALUE', 'REFERENCE_RANGE_HIGH_VALUE'), 'ref_text': ('REFERENCE_RANGE_TEXT', 'REFERENCE_RANGE_TEXT'),
}
QUANTILES = {'p001': 0.001, 'p005': 0.005, 'p01': 0.01, 'p05': 0.05, 'p25': 0.25, 'p50': 0.5, 'p75': 0.75,
             'p95': 0.95, 'p99': 0.99, 'p995': 0.995, 'p999': 0.999}
TEST_ROWS = 10_000_000

RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', required=True, type=Path, help="Engine output (or release) parquet")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/service-provider/qc_values'),
                help="Exportable aggregates, every row >= --min-people people (default: /mnt/disks/data/kanta/service-provider/qc_values)")
ap.add_argument('--out-cache', type=Path, default=Path('/mnt/disks/data/kanta/tmp/qc_values'),
                help="Row-level and unmasked files, not for export; duckdb spills to <out-cache>/duckdb "
                     "(default: /mnt/disks/data/kanta/tmp/qc_values)")
ap.add_argument('--min-people', type=int, default=5, help="Exported rows need at least this many distinct people (default: 5)")
ap.add_argument('--fence-k', type=float, default=3.0, help="Tukey fence multiplier on log10 values (default: 3)")
ap.add_argument('--force', action='store_true', help="Rebuild every cached file")
ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads (default: min(8, cores))")
ap.add_argument('--test', action='store_true', help=f"Build from the first {TEST_ROWS:,} input rows; names get a _test suffix")
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
log(f"exportable aggregates -> {args.out}; row-level files -> {args.out_cache}")

sfx = '_test' if args.test else ''
BASE = args.out_cache / f"base{sfx}"
F = {name: args.out / f"qc_{name}{sfx}.parquet" for name in ('lookup', 'hist', 'national', 'mix')}
F |= {name: args.out_cache / f"qc_{name}{sfx}.parquet" for name in ('lookup_full', 'hist_full', 'national_full', 'extremes')}
MIN = args.min_people


def stale(out, deps):
    return args.force or not out.exists() or any(out.stat().st_mtime < d.stat().st_mtime for d in deps)


def build(out, deps, sql, options='format parquet'):
    """Write `sql` to `out` (a file, or a folder for partitioned writes) unless it is newer than all `deps`."""
    if not stale(out, deps):
        log(f"Cached {out.name}")
        return
    part = out.with_name(out.name + '.part')
    shutil.rmtree(part, ignore_errors=True) if part.is_dir() else part.unlink(missing_ok=True)
    con.sql(f"copy ({sql}) to '{part}' ({options})")
    if out.is_dir():
        shutil.rmtree(out)
    os.replace(part, out)
    log(f"Built {out.name}")


# --- Input columns (engine or release spelling), cleaned: "NA"/'' -> null
src = f"(select * from read_parquet('{args.input}')" + (f" limit {TEST_ROWS})" if args.test else ')')
desc = {r[0]: r[1] for r in con.sql(f"describe select * from read_parquet('{args.input}')").fetchall()}
OPTIONAL = {'unit_pre_inj', 'rowid'}  # left null when the input lacks them (release files have no pre-injection unit)


def raw(name):
    c = next((c for c in COLUMNS[name] if c and c in desc), None)
    return c


def s(name):
    c = raw(name)
    if c is None:
        if name in OPTIONAL:
            return 'null'
        raise SystemExit(f"{args.input}: missing column {COLUMNS[name]}")
    return f"nullif(nullif(trim(cast(\"{c}\" as varchar)), ''), 'NA')"


num = lambda name: f"try_cast(replace({s(name)}, ',', '.') as double)"
key = lambda name: f"case when {s(name)} is not null then hash({s(name)}) end"
vr = s('v_raw')
vr_clean = f"regexp_replace(replace({vr}, ',', '.'), '\\.?0+$', '')" if desc[raw('v_raw')] in ('DOUBLE', 'FLOAT') else f"replace({vr}, ',', '.')"
unit_missing = f"({s('unit_pre_inj')} is null)" if raw('unit_pre_inj') else 'null'

# --- Base and lookup (the two passes over the input)
if stale(BASE, [args.input]):
    log("Building base (one pass over the input) ...")
build(BASE, [args.input], f"""
    select try_cast({s('rowid')} as bigint) rowid, try_cast({s('omop')} as bigint) omop, {key('test_name')} k_test, {key('s')} k_sp,
           case when {s('f')} is not null then hash({s('f')}) end f,
           try_cast(substr({s('dt')}, 1, 4) as smallint) as year, try_cast(substr({s('dt')}, 6, 2) as tinyint) as month,
           case lower({s('sex')}) when 'male' then 1 when 'm' then 1 when 'female' then 0 when 'f' then 0 end::tinyint sex,
           try_cast({num('age')} as float) age, {num('v')} v, {num('v_raw')} v_raw,
           case lower({s('extracted')}) when '1' then true when 'true' then true when '0' then false when 'false' then false end extracted,
           {key('unit')} k_unit, {unit_missing} unit_missing,
           case when try_cast({vr_clean} as double) is null then null when strpos({vr_clean}, '.') > 0
                then length({vr_clean}) - strpos({vr_clean}, '.') else 0 end::tinyint decimals,
           try_cast({s('qc_pass')} as tinyint) qc_pass, {key('qc_notes')} k_note, {s('outcome')} outcome,
           {key('ref_text')} k_ref, {num('ref_low')} ref_low, {num('ref_high')} ref_high
    from {src} where {num('v')} is not null or {num('v_raw')} is not null""", 'format parquet, partition_by (omop)')
build(F['lookup_full'], [args.input], ' union all '.join(
    f"select '{kind}' as kind, hash(x) as key, any_value(x) as value from (select {s(col)} x from {src}) where x is not null group by all"
    for kind, col in (('test', 'test_name'), ('sp', 's'), ('unit', 'unit'), ('note', 'qc_notes'), ('ref', 'ref_text'))))

B = f"read_parquet('{BASE}/*/*.parquet', hive_partitioning = true)"
deps_base = [BASE]

# --- Unmasked aggregates (cache): histogram -> national limits -> extremes
build(F['hist_full'], deps_base, f"""
    select omop, sign(v)::tinyint as sign, case when v <> 0 then floor(100 * log10(abs(v)))::smallint end as bin, count(*) n
    from {B} where v is not null group by all order by omop""")

ord_ = "sign, sign * coalesce(bin, 0)"
mid = "case sign when 0 then 0 else sign * pow(10, (bin + 0.5) / 100) end"
qcols = ', '.join(f"min({mid}) filter (where cum >= {q} * tot) {name}" for name, q in QUANTILES.items())
build(F['national_full'], [F['hist_full']], f"""
    with c as (select *, sum(n) over (partition by omop order by {ord_} rows unbounded preceding) cum,
                         sum(n) over (partition by omop) tot from read_parquet('{F['hist_full']}')),
         q as (select omop, any_value(tot)::bigint n, {qcols} from c group by omop)
    select *, case when p25 > 0 then pow(10, log10(p25) - {args.fence_k} * (log10(p75) - log10(p25))) end fence_lo,
              case when p25 > 0 then pow(10, log10(p75) + {args.fence_k} * (log10(p75) - log10(p25))) end fence_hi
    from q order by omop""")

N = f"read_parquet('{F['national_full']}')"
out_fence = "(b.v < nat.fence_lo or b.v > nat.fence_hi)"
build(F['extremes'], [BASE, F['national_full']], f"""
    select b.*, b.v < nat.p001 below_p001, b.v > nat.p999 above_p999, coalesce({out_fence}, false) out_fence,
           nat.p001, nat.p50, nat.p999, nat.fence_lo, nat.fence_hi
    from {B} b join {N} nat using (omop)
    where b.v < nat.p001 or b.v > nat.p999 or {out_fence} order by omop""")

# --- Exported aggregates: every row has >= MIN people
build(F['lookup'], [BASE, F['lookup_full']], f"""
    with used as ({' union all '.join(f"select '{kind}' as kind, {col} as key from {B} where {col} is not null group by all having count(distinct f) >= {MIN}"
                                      for kind, col in (('test', 'k_test'), ('sp', 'k_sp'), ('unit', 'k_unit'), ('note', 'k_note'), ('ref', 'k_ref')))})
    select l.* from read_parquet('{F['lookup_full']}') l semi join used using (kind, key)""")

build(F['hist'], deps_base, f"""
    select omop, k_test, k_sp, year, extracted, sign(v)::tinyint as sign,
           case when v <> 0 then floor(100 * log10(abs(v)))::smallint end as bin, count(*) n, count(distinct f) people
    from {B} where v is not null group by all having people >= {MIN} order by omop""")

tail = ', '.join(f"case when n * {min(q, 1 - q)} >= {MIN} then {name} end {name}" for name, q in QUANTILES.items())
build(F['national'], [BASE, F['national_full']], f"""
    with p as (select omop, count(distinct f) people from {B} where v is not null group by all having people >= {MIN})
    select omop, n, people, {tail}, fence_lo, fence_hi from {N} join p using (omop) order by omop""")

K = 'omop, k_test, k_sp, year, extracted'
DIMS = {'unit': 'cast(k_unit as varchar)', 'qc_note': 'cast(k_note as varchar)', 'outcome': 'outcome',
        'decimals': 'cast(decimals as varchar)', 'has_value': 'cast(v is not null as varchar)',
        'qc_fail': 'cast(coalesce(qc_pass = 0, false) as varchar)', 'unit_missing': 'cast(unit_missing as varchar)',
        'has_ref_text': 'cast(k_ref is not null as varchar)', 'has_ref_num': 'cast(ref_low is not null or ref_high is not null as varchar)'}
build(F['mix'], deps_base, f"""
    with x as (select {K}, {', '.join(f"{e} as d_{d}" for d, e in DIMS.items())}, f from {B}),
         g as (select {K}, {', '.join(f'd_{d}' for d in DIMS)}, {', '.join(f'grouping(d_{d}) as g_{d}' for d in DIMS)},
                      count(*) n, count(distinct f) people
               from x group by grouping sets ({', '.join(f'({K}, d_{d})' for d in DIMS)}) having people >= {MIN})
    select {K}, case {' '.join(f"when g_{d} = 0 then '{d}'" for d in DIMS)} end dim,
           case {' '.join(f"when g_{d} = 0 then d_{d}" for d in DIMS)} end cat, n, people
    from g order by omop""")

for name, path in [('base', BASE), *F.items()]:
    size = sum(p.stat().st_size for p in path.rglob('*') if p.is_file()) if path.is_dir() else path.stat().st_size
    print(f"  {path.name:28s} {size / 2**20:9.1f} MB")
n_base, n_omop = con.sql(f"select count(*), count(distinct omop) from {B}").fetchone()
log(f"Done: base has {n_base:,} rows over {n_omop:,} OMOP ids")
