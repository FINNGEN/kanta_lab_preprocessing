#!/usr/bin/env python3
"""Learn each unit's reference range per test name x OMOP, and flag the rows whose range belongs to another unit.

Modes:
  cache   the global build, every test; rebuilt only when missing or older than --input / --limits, or with --force
  sets    cache (if not up to date), then learn each pair's core and unit ranges -> its folder (skipped when the folder
          is up to date with the cache and the parameters; --force to relearn)
  flag    cache and sets (each only if not up to date), then flag each pair's rows
So one flag run does everything a pair needs, and a second one reuses it all. sets/flag rebuild the cache only when it is
missing or stale (--rebuild-cache to force), under a lock so parallel runs build it once.

--out (or OUT, its shorthand) is the results folder, or one pair's folder in it, told apart by whether it holds the
pair's metadata. The pairs, at most one of:
  --test TEST_NAME --omop OMOP_ID   one pair, into <out>/<test>/<OMOP id>/
  --top N, --random N               the N largest / N random pairs of those with >= --min-count rows (default 1000)
  --pairs FILE                      the pairs listed in FILE (TEST_NAME, OMOP_CONCEPT_ID), whatever their rows
  (none)                            every pair with >= --min-count rows; or, when --out is a pair's folder, that pair:
                                    its test, OMOP id, and unless given --input, --limits and --sample come from its metadata
e.g. flag results --top 2; flag results --test b-hkr --omop 3009542; flag results/b-hkr/3009542 (rerun).
One pair runs in this process and exits with code 3 when it has no rows with a harmonized value (nothing to learn or
flag). Several pairs: the cache, then each pair in its own process, --jobs at a time (each gets --memory-limit / --jobs
and --threads / --jobs), with a progress bar; --dry-run stops after the cache and the pair lists. Per-pair logs in
--out/logs/, the pair lists in --out/all_pairs.tsv and pairs.tsv, the run log in ./run_ref_units.log, and for flag one
line per pair in --out/flag_summary.tsv (test, omop, rows_checked, flagged, pct_flagged, unit_mismatch, unit_missing).
Exit code 1 when a pair failed (3 = no rows, counts as skipped).

Assumptions: (1) the OMOP mapping is right, so a row whose harmonized value sits in the core of its test's
distribution has the right MEASUREMENT_UNIT_CLEANED; (2) a row's reference range is on the scale of its source value,
even when the declared unit is wrong (in the unit and in the reference-range unit alike).

cache (in --tmp, named after the input file; with --sample the first 10M input rows and a _sample suffix):
1. inject   A slim copy of the input rows (every test), as if the engine had already filled the reference-range columns: rows
            without numeric limits get lower/upper from --limits (*_LIMITS.tsv from ref_text_limits.py, statuses
            range/upper/lower), matched on test name + REFERENCE_RANGE_TEXT (else REFERENCE_RANGE_GROUP), as in
            ref_text_limits.py. Release column names: ROWID, TEST_NAME, OMOP_CONCEPT_ID (null when unmapped or 0),
            OMOP_QUANTITY, SEX, EVENT_AGE, MEASUREMENT_VALUE_CLEANED, MEASUREMENT_UNIT_CLEANED, MEASUREMENT_VALUE_HARMONIZED,
            MEASUREMENT_UNIT_HARMONIZED, REFERENCE_RANGE_LOW_VALUE, REFERENCE_RANGE_HIGH_VALUE, REFERENCE_RANGE_SOURCE
            (num / text / null). A folder split into 64 buckets by test name (bucket=hash(TEST_NAME) % 64), so a pair
            reads only its test's bucket.
2. hist     test x OMOP x sex x age group x unit x lower x upper x 0.002-log10 bin of the harmonized value -> rows, in
            the same buckets. A test name can map to several OMOP concepts (e.g. % and count), with harmonized values of
            different quantities, so everything from here on is per test x OMOP.
3. info     rows per test x OMOP x range source, with quantity and harmonized unit; and ref_unit_pairs_<input>.tsv: every
            test x OMOP pair (OMOP id not 0) by rows, TEST_NAME, OMOP_CONCEPT_ID, n.

sets (per pair):
4. core     Per stratum (F/M x minor/adult, and all/all pooled over every row): the rows whose log10 harmonized value is
            within median +- --core-sd x 1.4826 x MAD (a robust 1 SD by default), from the bins.
5. sets     Per stratum x unit, over the core rows with a range: weighted median of the lower limits (> 0) and of the
            upper limits (> 0), with 5th/95th percentiles. Used (use = true) when it has >= --min-rows rows and >= --min-pct %
            of the stratum's core rows with a range, and neither limit spans >= 10x between its percentiles (mixed_scale:
            the core let rows of another scale in).

flag (per pair): rows checked are the test's rows mapped to that OMOP id, and its unmapped ones (no OMOP id: no unit, or
a unit outside the harmonization, which a match can bring back in); rows mapped to the test's other OMOP ids are left
out, and so are rows without a numeric MEASUREMENT_VALUE_CLEANED. The verdict comes from the range alone; the row's own
value then confirms it. For each row with a range, in its stratum (all/all when sex or age is unknown; per unit, the
stratum's learned range, else the pooled all/all one):
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
The verdict is worked out once per signature (stratum x unit x lower x upper), then joined back to the rows.
Confirmation, per row with a suggested unit, with the unit factors (median harmonized / source value over the ok rows
of each unit) and the pair's core (all/all; its --core-sd is read from the metadata): corrected = source value x the
suggested unit's factor. The suggestion stands when |log10(corrected / median)| < |log10(harmonized now / median)|, i.e.
the change brings the value closer to the median; for a unit not harmonized now, when corrected is within --far robust
SDs of the median. Otherwise the status becomes value_range_conflict (the range points to another unit, the value
doesn't: e.g. a 41 % hematocrit with a 0.35-0.46 range), and suggested_unit is left empty (range_suggested keeps it).

Outputs, one folder per pair: --out/<test name>/<OMOP id>/ ('/' in a name becomes '_'), each file named
<test>_<OMOP id>_<file> (aggregates only, groups of >= --min-n rows, except the row-level parquet; with --sample
the names end in _sample):
from sets
- ref_unit_core.tsv            test, omop, sex, age, rows, median, core_lo, core_hi (harmonized), core_rows, core_rows_with_range
- ref_unit_sets.tsv            test, omop, sex, age, unit, rows, pct_of_core, n_lower, lower, lower_p05, lower_p95, n_upper,
                               upper, upper_p05, upper_p95, use, note
- metadata.json                the pair, quantity and harmonized unit, the cache it was learned from (input, limits,
                               modification times), parameters, strata, rows by range source (of the OMOP, and of the
                               test's unmapped rows), the test's other OMOP ids, learned units; flag adds its run under "flags"
from flag
- ref_unit_flags_rows.parquet  row level (stays on the data side): ROWID, TEST_NAME, OMOP_CONCEPT_ID, sex, age, unit, lower,
                               upper, REFERENCE_RANGE_SOURCE, status, suggested_unit, range_suggested, x_claimed, x_suggested
                               (distances as factors, e.g. 100 = a hundred times off), stratum_used; only the rows with a
                               confirmed suggested unit (unit_mismatch, unit_missing), unless --all-rows
- ref_unit_flags.tsv           per signature: rows, the range's verdict (range_status, range_suggested) and how many of
                               its rows were confirmed / value_range_conflict
- ref_unit_flags_summary.tsv   rows per unit x status x suggested_unit, with % of the unit's rows
- ref_unit_flag_line.tsv       one line for the run's flag_summary.tsv: test, omop, rows_checked, flagged,
                               pct_flagged, unit_mismatch, unit_missing
- ref_unit_check_ranges.tsv    per group (ok rows per unit; every other status per unit; flagged and value_range_conflict
                               ones per unit -> the unit their range points to): rows; their ranges (lower/upper 5th/50th/95th
                               percentiles, top 3 ranges); the factors; median source value, harmonized as is and corrected;
                               % as is / corrected inside the core (as is empty when the unit isn't harmonized)
- ref_unit_check_hist.tsv      rows per 0.02-log10 bin of the harmonized value: the ok rows (reference), each other group
                               as harmonized now (as_is), and the flagged ones corrected
- ref_unit_check.png           one row per flagged group (at most 12) with harmonized values both now and corrected (the
                               others, e.g. units not harmonized now, are listed at the bottom): the group now and corrected
                               against the reference, and its ranges next to the ok rows' in the claimed and the suggested
                               unit (needs matplotlib; skipped without)
"""
import argparse, json, math, os, shutil, subprocess, sys, time
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
NBUCKETS = 64  # injected rows and hist: folders bucket=0..63 by hash(TEST_NAME)
BIN = 0.002  # log10 width of the harmonized-value bins (0.5%: fine enough for narrow tests like hematocrit)
TEST_ROWS = 10_000_000
NO_CORE = 3  # exit code: the pair has no rows with a harmonized value
SEX = "case when lower(SEX) in ('male', 'm') then 'M' when lower(SEX) in ('female', 'f') then 'F' end"
AGE = "case when EVENT_AGE < 18 then 'minor' when EVENT_AGE >= 18 then 'adult' end"
FLAG = "('unit_mismatch', 'unit_missing')"
RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
D = Path('/mnt/disks/data/kanta/ref')
INPUT = Path('/mnt/disks/data/kanta/engine/kanta_dev_2026_10_15_RELEASE.parquet')
LIMITS = D / 'limits' / 'ref_text_table_LIMITS.tsv'
t0 = time.time()
log = lambda msg: print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)
lit = lambda s: "'" + str(s).replace("'", "''") + "'"  # SQL string literal
mtime = lambda p: datetime.fromtimestamp(p.stat().st_mtime).isoformat(timespec='seconds')


def connect(args):
    (args.tmp / 'duckdb').mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.sql(f"set temp_directory = '{args.tmp / 'duckdb'}'")
    con.sql("set preserve_insertion_order = false")
    con.sql(f"set memory_limit = '{args.memory_limit}'")
    con.sql(f"set threads = {args.threads}")
    log(f"duckdb: memory limit {args.memory_limit} (machine has {RAM_GB:.0f}GB), {args.threads} threads, spilling to {args.tmp / 'duckdb'}")
    return con


def cache_files(args):
    tag = f"{args.input.stem}{'_sample' if args.sample else ''}"
    return {'inj': args.tmp / f"ref_injected_{tag}",  # folder of bucket=<n>/ parquet files
            'hist': args.tmp / f"ref_unit_hist_{tag}",  # same
            'info': args.tmp / f"ref_unit_info_{tag}.parquet",
            'pairs': args.tmp / f"ref_unit_pairs_{tag}.tsv",
            'meta': args.tmp / f"ref_unit_cache_{tag}.json"}


def pair_dir(args, test, omop):
    name = test.replace('/', '_')
    return args.out / name / str(omop), f"{name}_{omop}_", '_sample' if args.sample else ''


def bucket_of(con, test):
    return con.sql(f"select hash({lit(test)}) % {NBUCKETS}").fetchone()[0]


# ---------------------------------------------------------------- cache
def cache_stale(args, force=False):
    """True when any cached file must be rebuilt: force, missing, or older than what it is built from."""
    F = cache_files(args)
    deps = {'inj': [args.input, args.limits], 'hist': [F['inj']], 'info': [F['inj']], 'meta': [F['inj']]}
    return force or not all(F[k].exists() and all(F[k].stat().st_mtime >= d.stat().st_mtime for d in ds) for k, ds in deps.items())


def ensure_cache(args, con, force=False):
    """Build the cache if it is stale, holding a lock so parallel runs wait for one build instead of racing."""
    import fcntl
    if not cache_stale(args, force):
        return
    args.tmp.mkdir(parents=True, exist_ok=True)
    with open(cache_files(args)['meta'].with_suffix('.lock'), 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if cache_stale(args, force):  # another run may have built it while we waited
            cache(args, con, force)


def cache(args, con, force=False):
    F = cache_files(args)

    def stale(out, deps):
        return force or not out.exists() or any(out.stat().st_mtime < d.stat().st_mtime for d in deps)

    def build(out, deps, sql, partition=None):
        """Write `sql` to parquet `out` (a folder split by `partition`, if given) unless it is newer than all `deps`
        (make-like); written under a temporary name, then renamed."""
        if not stale(out, deps):
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

    src = f"(select * from read_parquet('{args.input}')" + (f" limit {TEST_ROWS})" if args.sample else ')')
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
    # --- 1. Inject the parsed text limits into the rows without numeric limits (read like ref_text_limits.py wrote
    # them, so the texts compare equal to the input's)
    lim = pd.read_csv(args.limits, sep='\t', dtype=str, keep_default_na=False, quoting=3, escapechar='\\')
    lim = lim[lim['status'].isin(['range', 'upper', 'lower'])]
    lim = pd.DataFrame({'test_name': lim['test_name'], 'ref_text': lim['ref_text'],
                        'lower': pd.to_numeric(lim['lower'], errors='coerce'), 'upper': pd.to_numeric(lim['upper'], errors='coerce')})
    con.register('lim_df', lim)
    log(f"{len(lim):,} test name x text pairs with limits in {args.limits.name}")
    has_num = f"({num('ref_low')} is not null or {num('ref_high')} is not null)"
    build(F['inj'], [args.input, args.limits], f"""
        with r as (
            select try_cast({col('rowid')} as bigint) as ROWID, coalesce({col('test_name')}, 'NA') as TEST_NAME,
                   nullif({col('omop')}, '0') as OMOP_CONCEPT_ID, {col('quantity')} as OMOP_QUANTITY, {col('sex')} as SEX,
                   try_cast({col('age')} as double) as EVENT_AGE, {num('v_raw')} as MEASUREMENT_VALUE_CLEANED,
                   {col('unit')} as MEASUREMENT_UNIT_CLEANED, {num('v')} as MEASUREMENT_VALUE_HARMONIZED,
                   {col('h_unit')} as MEASUREMENT_UNIT_HARMONIZED, {num('ref_low')} as lo, {num('ref_high')} as hi, {has_num} as has_num,
                   regexp_replace(coalesce({col('ref_text')}, {col('ref_group')}), '[\\t\\r\\n]+', ' ', 'g') as txt
            from {src})
        select hash(r.TEST_NAME) % {NBUCKETS} as bucket, r.ROWID, r.TEST_NAME, r.OMOP_CONCEPT_ID, r.OMOP_QUANTITY, r.SEX, r.EVENT_AGE,
               r.MEASUREMENT_VALUE_CLEANED, r.MEASUREMENT_UNIT_CLEANED, r.MEASUREMENT_VALUE_HARMONIZED, r.MEASUREMENT_UNIT_HARMONIZED,
               case when r.has_num then r.lo else l.lower end as REFERENCE_RANGE_LOW_VALUE,
               case when r.has_num then r.hi else l.upper end as REFERENCE_RANGE_HIGH_VALUE,
               case when r.has_num then 'num' when l.lower is not null or l.upper is not null then 'text' end as REFERENCE_RANGE_SOURCE
        from r left join lim_df l on l.test_name = r.TEST_NAME and l.ref_text = r.txt""", partition='bucket')
    I = f"read_parquet('{F['inj']}/*/*.parquet', hive_partitioning = true)"
    # --- 2. Histogram of the harmonized values by stratum, unit and range, in the same buckets
    build(F['hist'], [F['inj']], f"""
        select bucket, TEST_NAME test, OMOP_CONCEPT_ID omop, {SEX} sex, {AGE} age, coalesce(MEASUREMENT_UNIT_CLEANED, 'NA') unit,
               REFERENCE_RANGE_LOW_VALUE lower, REFERENCE_RANGE_HIGH_VALUE upper,
               floor(log10(MEASUREMENT_VALUE_HARMONIZED) / {BIN})::integer hbin, count(*) n
        from {I} where MEASUREMENT_VALUE_HARMONIZED > 0 and OMOP_CONCEPT_ID is not null group by all""", partition='bucket')
    # --- 3. Rows per test x OMOP x range source; the pair list
    build(F['info'], [F['inj']], f"""select TEST_NAME, OMOP_CONCEPT_ID omop, coalesce(REFERENCE_RANGE_SOURCE, 'none') src, count(*) n,
                                         mode(OMOP_QUANTITY) quantity, mode(MEASUREMENT_UNIT_HARMONIZED) harmonized_unit
                                  from {I} group by all""")
    con.sql(f"""copy (select TEST_NAME, omop OMOP_CONCEPT_ID, sum(n)::bigint n from read_parquet('{F['info']}')
                      where omop is not null and omop not in ('0', 'NA') group by all order by n desc, TEST_NAME, OMOP_CONCEPT_ID
                ) to '{F['pairs']}' (header, delimiter '\t')""")
    for s, n, pct in con.sql(f"""select src, sum(n), 100 * sum(n) / sum(sum(n)) over () from read_parquet('{F['info']}')
                                 group by all order by 2 desc""").fetchall():
        log(f"  range from {s}: {n:,} rows ({pct:.1f}%)")
    meta = {'input': str(args.input.resolve()), 'input_modified': mtime(args.input), 'limits': str(args.limits.resolve()),
            'limits_modified': mtime(args.limits), 'sample_rows_only': args.sample, 'injected_rows': str(F['inj'].resolve()),
            'injected_modified': mtime(F['inj']), 'hist': str(F['hist'].resolve()), 'buckets': NBUCKETS}
    F['meta'].write_text(json.dumps(meta, indent=1) + '\n')
    n = con.sql(f"select count(*), sum(n) from read_csv('{F['pairs']}', delim='\t', header=true)").fetchone()
    log(f"Cache ready ({F['meta']}); {n[0]:,} test x OMOP pairs, {n[1]:,} rows -> {F['pairs']}")


def read_cache(args):
    F = cache_files(args)
    if not F['meta'].exists() or not F['hist'].is_dir():
        raise SystemExit(f"{F['meta']}: no cache for {args.input.name}; run '{Path(sys.argv[0]).name} cache' first")
    return F, json.loads(F['meta'].read_text())


# ---------------------------------------------------------------- sets
def wquant(table, keys, val, qs, where='true'):
    """Weighted quantiles of `val` (weights n) per `keys` from `table`: columns <val>_q<qq> for each q in `qs`."""
    sel = ', '.join(f"min({val}) filter (where cum >= {q} * tot) {val.split('.')[-1]}_q{round(q * 100):02d}" for q in qs)
    return f"""select {keys}, {sel} from (
        select {keys}, {val}, sum(n) over (partition by {keys} order by {val} rows unbounded preceding) cum,
               sum(n) over (partition by {keys}) tot
        from (select {keys}, {val}, sum(n) n from {table} where {where} group by all)) group by all"""


def learn(args, con, test, omop):
    """The pair's core and learned unit ranges -> its folder. False when it has no rows with a harmonized value."""
    F, cmeta = read_cache(args)
    d, pre, sfx = pair_dir(args, test, omop)
    META = d / f"{pre}metadata{sfx}.json"
    params = {'core_sd': args.core_sd, 'min_rows': args.min_rows, 'min_pct': args.min_pct, 'min_n': args.min_n,
              'bin_log10': BIN, 'mixed_scale_factor': 10}
    if META.exists() and not args.force:
        old = json.loads(META.read_text())
        if old.get('cache', {}).get('injected_modified') == cmeta['injected_modified'] and old.get('parameters') == params:
            log(f"{test} / {omop}: {d} is up to date with the cache and parameters, kept (--force to redo)")
            return True
    b = bucket_of(con, test)
    if not (F['hist'] / f"bucket={b}").is_dir():  # no test of this bucket has a harmonized value
        log(f"{test} / {omop}: no rows with a harmonized value, nothing learned")
        return False
    H = f"(select * from read_parquet('{F['hist']}/bucket={b}/*.parquet') where test = {lit(test)} and omop = {lit(omop)})"
    con.sql(f"""create or replace temp table s as
        select test, omop, sex, age, unit, lower, upper, hbin, n from {H} where sex is not null and age is not null
        union all
        select test, omop, 'all', 'all', unit, lower, upper, hbin, sum(n) from {H} group by all""")
    if not con.sql("select count(*) from s").fetchone()[0]:
        log(f"{test} / {omop}: no rows with a harmonized value, nothing learned")
        return False

    # --- 4. Core per stratum, plus all/all pooled over every row
    K = 'test, omop, sex, age'
    con.sql(f"create or replace temp table med as {wquant('s', K, 'hbin', [0.5])}")
    con.sql(f"""create or replace temp table mad as {wquant(f"(select s.*, abs(s.hbin - m.hbin_q50) dev from s join med m using ({K}))", K, 'dev', [0.5])}""")
    con.sql(f"""create or replace temp table core as
        select m.test, m.omop, m.sex, m.age, m.hbin_q50 med, {args.core_sd} * 1.4826 * greatest(d.dev_q50, 1) halfw
        from med m join mad d using ({K})""")
    con.sql(f"create or replace temp table srow as select s.*, abs(s.hbin - c.med) <= c.halfw in_core from s join core c using ({K})")
    core_out = con.sql(f"""
        select test, omop, sex, age, sum(n) as rows,
               round(pow(10, (any_value(c.med) + 0.5) * {BIN}), 4) median,
               round(pow(10, (any_value(c.med) + 0.5 - any_value(c.halfw)) * {BIN}), 4) core_lo,
               round(pow(10, (any_value(c.med) + 0.5 + any_value(c.halfw)) * {BIN}), 4) core_hi,
               coalesce(sum(n) filter (where in_core), 0) core_rows,
               coalesce(sum(n) filter (where in_core and (lower is not null or upper is not null)), 0) core_rows_with_range
        from srow join core c using ({K}) group by test, omop, sex, age
        having sum(n) >= {args.min_n} order by sex = 'all', sex, age""").df()

    # --- 5. Learned range per stratum x unit, over the core rows with a range
    con.sql("""create or replace temp table cr as select test, omop, sex, age, unit, lower, upper, sum(n) n from srow
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
        where rows >= {args.min_n} order by sex = 'all', sex, age, rows desc""").df()
    mixed = (sets['upper_p95'] / sets['upper_p05'] >= 10) | (sets['lower_p95'] / sets['lower_p05'] >= 10)
    low = (sets['rows'] < args.min_rows) | (sets['pct_of_core'] < args.min_pct)
    sets['note'] = [';'.join(n for n, f in (('low_support', a), ('mixed_scale', b)) if f) for a, b in zip(low, mixed)]
    sets['use'] = ~low & ~mixed
    for df in (core_out, sets):
        for c in {'rows', 'core_rows', 'core_rows_with_range', 'n_lower', 'n_upper'} & set(df.columns):
            df[c] = df[c].fillna(0).astype('int64')

    info = con.sql(f"select * from read_parquet('{F['info']}') where TEST_NAME = {lit(test)}").df()
    oi = info[info['omop'] == omop].sort_values('n', ascending=False)
    by_src = lambda df: {k: int(v) for k, v in df.groupby('src')['n'].sum().items()}
    d.mkdir(parents=True, exist_ok=True)
    core_out.to_csv(d / f"{pre}ref_unit_core{sfx}.tsv", sep='\t', index=False)
    sets.to_csv(d / f"{pre}ref_unit_sets{sfx}.tsv", sep='\t', index=False)
    meta = {'test_name': test, 'omop_id': omop,
            'omop_quantity': oi['quantity'].iloc[0] if len(oi) else None, 'harmonized_unit': oi['harmonized_unit'].iloc[0] if len(oi) else None,
            'created': datetime.now().isoformat(timespec='seconds'), 'script': str(Path(__file__).resolve()),
            'cache': cmeta, 'parameters': params,
            'strata': {'sex': 'F / M from SEX', 'age': 'minor (EVENT_AGE < 18) / adult (>= 18)', 'pooled': 'all/all, every row'},
            'rows_by_range_source': by_src(info[info['omop'] == omop]),
            'unmapped_rows_by_range_source': by_src(info[info['omop'].isna()]),
            'other_omop_ids': {o: int(n) for o, n in info[info['omop'].notna() & (info['omop'] != omop)].groupby('omop')['n'].sum().items()},
            'learned_units': sorted(sets.loc[sets['use'], 'unit'].unique().tolist()),
            'unused_units': sorted(set(sets['unit']) - set(sets.loc[sets['use'], 'unit']))}
    META.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str) + '\n')
    log(f"Wrote {d}/ (core {len(core_out)} rows, sets {len(sets)} rows, metadata)")
    pd.set_option('display.width', 250)
    pd.set_option('display.max_rows', 200)
    print("\nCore per stratum:\n" + core_out.to_string(index=False))
    print("\nLearned ranges per stratum x unit:\n" + sets.to_string(index=False))
    return True


# ---------------------------------------------------------------- flag
def dist(lo, hi, rlo, rhi):
    """Larger log10 distance over the limits both sides have (> 0); None when none can be compared."""
    d = [abs(math.log10(a / b)) for a, b in ((lo, rlo), (hi, rhi)) if pd.notna(a) and pd.notna(b) and a > 0 and b > 0]
    return max(d) if d else None


def flag(args, con, test, omop):
    F, _ = read_cache(args)
    d, pre, sfx = pair_dir(args, test, omop)
    META = d / f"{pre}metadata{sfx}.json"
    SETS = d / f"{pre}ref_unit_sets{sfx}.tsv"
    if not META.exists() or not SETS.exists():
        raise SystemExit(f"{d}: no metadata / ref_unit_sets; run sets for {test} / {omop} first")
    meta = json.loads(META.read_text())
    core_sd = meta['parameters']['core_sd']
    b = bucket_of(con, test)
    SRC = f"read_parquet('{F['inj']}/bucket={b}/*.parquet')"
    THIS = f"TEST_NAME = {lit(test)} and (OMOP_CONCEPT_ID = {lit(omop)} or OMOP_CONCEPT_ID is null)"
    I = f"(select * from {SRC} where {THIS} and MEASUREMENT_VALUE_CLEANED is not null)"
    no_value = con.sql(f"select count(*) from {SRC} where {THIS} and MEASUREMENT_VALUE_CLEANED is null").fetchone()[0]
    other = con.sql(f"""select OMOP_CONCEPT_ID, count(*) from {SRC}
                         where TEST_NAME = {lit(test)} and OMOP_CONCEPT_ID != {lit(omop)} group by all""").fetchall()
    log(f"{test} / OMOP {omop}: rows from {F['inj']}; left out: {no_value:,} without a value"
        + (f", mapped to other OMOP ids: {dict(other)}" if other else ''))
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
    ref = {(r.sex, r.age, r.unit): (r.lower, r.upper) for r in sets.itertuples()}
    units = sorted(set(sets['unit']))
    tol = math.log10(args.margin) + 1e-9

    def verdict(s):
        """(status, suggested_unit, x_claimed, x_suggested, stratum_used) for one signature."""
        if pd.isna(s.lower) and pd.isna(s.upper):
            return 'no_range', None, None, None, None
        strat = (s.sex, s.age) if pd.notna(s.sex) and pd.notna(s.age) else ('all', 'all')
        cand, used = {}, set()
        for u in units:  # per unit: the stratum's learned range, else the pooled one
            for st, label in ((strat, 'stratum' if strat != ('all', 'all') else 'pooled'), (('all', 'all'), 'pooled')):
                r = ref.get((*st, u))
                if r is not None:
                    cand[u] = dist(s.lower, s.upper, *r)
                    used.add(label)
                    break
        used = '+'.join(sorted(used)) or None
        fx = lambda x: None if x is None else round(10 ** x, 2)
        claimed = cand.get(s.unit) if pd.notna(s.unit) else None
        if pd.notna(s.unit) and claimed is not None and claimed <= tol:
            return 'ok', None, fx(claimed), None, used
        match = sorted((x, u) for u, x in cand.items() if u != s.unit and x is not None and x <= tol)
        if len(match) == 1:
            return ('unit_missing' if pd.isna(s.unit) else 'unit_mismatch'), match[0][1], fx(claimed), fx(match[0][0]), used
        if len(match) > 1:
            return 'ambiguous', '|'.join(u for _, u in match), fx(claimed), None, used
        return ('range_outlier' if s.unit in cand and claimed is not None else 'no_reference'), None, fx(claimed), None, used

    vcols = ['status', 'suggested_unit', 'x_claimed', 'x_suggested', 'stratum_used']
    sig[vcols] = pd.DataFrame([verdict(s) for s in sig.itertuples()], index=sig.index, columns=vcols)
    for c in ('status', 'suggested_unit', 'stratum_used'):  # text even when all empty (else duckdb reads an all-null column as a number)
        sig[c] = sig[c].astype('string')
    for c in ('x_claimed', 'x_suggested'):
        sig[c] = sig[c].astype('float64')
    con.register('verdict_df', sig[['k', *vcols]])

    # --- 3. Confirm each suggestion with the row's own value (pass 2): the corrected harmonized value must be closer to
    # the core's median than the current one (log10); without a current one (unit not harmonized), within --far robust SDs
    fac = dict(con.sql(f"""select r.unit, median(r.h / r.v) from {R} r join verdict_df v using (k)
                           where v.status = 'ok' and r.v > 0 and r.h > 0 group by all""").fetchall())
    con.register('fac_df', pd.DataFrame({'unit': pd.Series(list(fac), dtype='string'), 'f': pd.Series(list(fac.values()), dtype='float64')}))
    core = pd.read_csv(d / f"{pre}ref_unit_core{sfx}.tsv", sep='\t', keep_default_na=False, na_values=[''])
    core = core[(core['sex'] == 'all') & (core['age'] == 'all')].iloc[0]
    med_c, lo_c, hi_c = float(core['median']), float(core['core_lo']), float(core['core_hi'])
    m, sd = math.log10(med_c), math.log10(hi_c / med_c) / core_sd  # median and one robust SD, log10
    con.sql(f"""create or replace temp table rv as
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
    con.sql(f"create or replace temp table rv2 as select *, case when status in {FLAG} then range_suggested end suggested_unit from rv")
    by_status = dict(con.sql("select status, count(*) from rv2 group by all").fetchall())
    log("Rows by status: " + ', '.join(f"{k} {v:,}" for k, v in sorted(by_status.items(), key=lambda kv: -kv[1])))

    # --- 4. Row-level flags
    path = d / f"{pre}ref_unit_flags_rows{sfx}.parquet"
    part = path.with_name(path.name + '.part')
    keep = '' if args.all_rows else f"where status in {FLAG}"
    con.sql(f"""copy (
        select ROWID, test TEST_NAME, OMOP_CONCEPT_ID, sex, age, unit, lower, upper, REFERENCE_RANGE_SOURCE,
               status, suggested_unit, range_suggested, x_claimed, x_suggested, stratum_used
        from rv2 {keep}) to '{part}' (format parquet)""")
    os.replace(part, path)
    log(f"Wrote {path} ({con.sql(f"select count(*) from read_parquet('{path}')").fetchone()[0]:,} rows)")

    # --- 5. Aggregates
    conf = con.sql(f"""select k, count(*) filter (where status in {FLAG}) n_confirmed,
                              count(*) filter (where status = 'value_range_conflict') n_value_range_conflict
                       from rv2 group by all""").df()
    out = (sig.merge(conf, on='k', how='left').rename(columns={'status': 'range_status', 'suggested_unit': 'range_suggested'})
           .drop(columns='k').sort_values(['n', 'sex', 'age', 'unit', 'lower', 'upper'], ascending=[False, True, True, True, True, True]))
    p = d / f"{pre}ref_unit_flags{sfx}.tsv"
    out[out['n'] >= args.min_n].to_csv(p, sep='\t', index=False)
    log(f"Wrote {p} ({(out['n'] >= args.min_n).sum():,} signatures)")
    summ = con.sql("""select test, coalesce(unit, 'NA') unit, status, coalesce(suggested_unit, coalesce(range_suggested, '')) suggested_unit,
                             count(*) n from rv2 group by all""").df()
    summ['pct'] = (100 * summ['n'] / summ.groupby(['test', 'unit'])['n'].transform('sum')).round(2)
    summ = summ[summ['n'] >= args.min_n].sort_values(['unit', 'n'], ascending=[True, False])
    p = d / f"{pre}ref_unit_flags_summary{sfx}.tsv"
    summ.to_csv(p, sep='\t', index=False)
    log(f"Wrote {p} ({len(summ):,} rows)")
    tot, mis, miss = sum(by_status.values()), by_status.get('unit_mismatch', 0), by_status.get('unit_missing', 0)
    pd.DataFrame([{'test': test, 'omop': omop, 'rows_checked': tot, 'flagged': mis + miss,
                   'pct_flagged': round(100 * (mis + miss) / tot, 3) if tot else 0, 'unit_mismatch': mis, 'unit_missing': miss}]
                 ).to_csv(d / f"{pre}ref_unit_flag_line{sfx}.tsv", sep='\t', index=False)

    # --- 6. Check of the suggestions against the values, per group (value_range_conflict rows with the unit their range
    # pointed to, and where they would have landed)
    con.sql("""create or replace temp table chk2 as
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
    p = d / f"{pre}ref_unit_check_ranges{sfx}.tsv"
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
    p = d / f"{pre}ref_unit_check_hist{sfx}.tsv"
    hist.to_csv(p, sep='\t', index=False, float_format='%.4g')
    log(f"Wrote {p} ({len(hist):,} bins)")
    plot(d / f"{pre}ref_unit_check{sfx}.png", rng, hist, meta, test, omop, lo_c, hi_c)
    meta['flags'] = {'created': datetime.now().isoformat(timespec='seconds'), 'script': str(Path(__file__).resolve()),
                     'margin': args.margin, 'all_rows': args.all_rows, 'min_n': args.min_n, 'far': args.far,
                     'rows_by_status': {k: int(v) for k, v in by_status.items()},
                     'unit_factors': fac, 'core_all': {'median': med_c, 'lo': lo_c, 'hi': hi_c}}
    META.write_text(json.dumps(meta, indent=2, ensure_ascii=False, default=str) + '\n')
    log(f"Added the run to {META}")

    pd.set_option('display.width', 250)
    pd.set_option('display.max_rows', 300)
    print("\nRows per unit x status:\n" + summ.to_string(index=False))
    flagged = out[~out['range_status'].isin(['ok', 'no_range']) & (out['n'] >= args.min_n)]
    print("\nFlagged signatures (largest first):\n" + flagged.head(40).to_string(index=False))
    print("\nCheck against the values (ok rows per unit; every other status per unit, flagged ones per unit -> suggested unit):")
    print(rng.drop(columns=['top_ranges']).to_string(index=False))


def plot(path, rng, hist, meta, test, omop, lo_c, hi_c):
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
                      cellLoc='left', colLoc='left', colWidths=[0.2, 0.12, 0.08, 0.08, 0.52])
        tb.auto_set_font_size(False)
        tb.set_fontsize(7.5)
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


# ---------------------------------------------------------------- every pair
def run_batch(args, con):
    """sets/flag without a pair: the cache, the pair list, then the pair's mode for each pair, --jobs at a time, each in its
    own process (a crash or out-of-memory kill loses that pair only); per-pair logs in --out/logs/, a progress bar on the
    screen, the run log in ./run_ref_units.log, and for flag --out/flag_summary.tsv."""
    ensure_cache(args, con, args.rebuild_cache)
    con.close()
    F = cache_files(args)
    allp = pd.read_csv(F['pairs'], sep='\t', dtype={'TEST_NAME': str, 'OMOP_CONCEPT_ID': str, 'n': 'int64'}, keep_default_na=False)
    n_cache = len(allp)
    allp = allp[allp['n'] >= args.min_count]
    print(f"{len(allp):,} of {n_cache:,} test x OMOP pairs have >= {args.min_count:,} rows -> {args.out / 'all_pairs.tsv'}", flush=True)
    if args.pairs:
        sel = pd.read_csv(args.pairs, sep='\t', dtype=str, keep_default_na=False)[['TEST_NAME', 'OMOP_CONCEPT_ID']]
        sel = sel.merge(allp, how='left', on=['TEST_NAME', 'OMOP_CONCEPT_ID']).astype({'n': 'Int64'})  # listed pairs run even below --min-count
        which = f"{len(sel)} from {args.pairs}"
    elif args.random:
        sel, which = allp.sample(n=min(args.random, len(allp))), f"{args.random} random"
    elif args.top:
        sel, which = allp.head(args.top), f"top {args.top}"
    else:
        sel, which = allp, 'all'
    LOGS = args.out / 'logs'
    LOGS.mkdir(parents=True, exist_ok=True)
    allp.to_csv(args.out / 'all_pairs.tsv', sep='\t', index=False)
    sel.to_csv(args.out / 'pairs.tsv', sep='\t', index=False)
    jobs = max(1, args.jobs)
    total_gb = float(args.memory_limit.upper().removesuffix('GB')) if args.memory_limit.upper().endswith('GB') \
        else float(args.memory_limit.upper().removesuffix('MB')) / 1024
    mem, threads = max(1, int(total_gb / jobs)), max(1, args.threads // jobs)
    cmd = [sys.executable, str(Path(__file__).resolve()), args.mode, '--input', str(args.input), '--limits', str(args.limits),
           '--tmp', str(args.tmp), '--out', str(args.out), '--min-n', str(args.min_n), '--memory-limit', f"{mem}GB",
           '--threads', str(threads), '--core-sd', str(args.core_sd), '--min-rows', str(args.min_rows), '--min-pct', str(args.min_pct)]
    cmd += ['--sample'] * args.sample + ['--force'] * args.force
    if args.mode == 'flag':
        cmd += ['--margin', str(args.margin), '--far', str(args.far)] + ['--all-rows'] * args.all_rows
    N = len(sel)
    if args.dry_run:
        pd.set_option('display.width', 200)
        print(f"Dry run: {which} of {len(allp):,} pairs -> {N:,} would run ({jobs} at a time); lists in {args.out / 'all_pairs.tsv'} "
              f"and {args.out / 'pairs.tsv'}\n" + sel.head(30).to_string(index=False) + (f"\n... ({N - 30:,} more)" if N > 30 else ''))
        return 0
    print(f"ref_units.py {args.mode}  {datetime.now():%F %T}\n  input  {args.input}\n  out    {args.out}\n"
          f"  pairs  {which} of {len(allp):,}: {N:,}, {jobs} at a time ({mem}GB, {threads} threads each)\n  logs   {LOGS}", flush=True)
    name = lambda t, o: f"{t.replace('/', '_')}_{o}"
    line_file = lambda t, o: args.out / t.replace('/', '_') / o / f"{name(t, o)}_ref_unit_flag_line{'_sample' if args.sample else ''}.tsv"
    order = list(sel[['TEST_NAME', 'OMOP_CONCEPT_ID']].itertuples(index=False, name=None))
    todo, running, rc, flagged, started = list(order), {}, {}, 0, datetime.now().isoformat(timespec='seconds')
    why = lambda code: 'killed: out of memory? try fewer --jobs' if code == -9 else f"exit {code}"
    hms = lambda x: f"{int(x) // 3600}:{int(x) % 3600 // 60:02d}:{int(x) % 60:02d}"
    t1 = time.time()

    def bar(label):
        i, el = len(rc), time.time() - t1
        ok, skip = sum(v == 0 for v in rc.values()), sum(v == NO_CORE for v in rc.values())
        fill = i * 30 // N if N else 30
        eta = hms(el * (N - i) / i) if i else '--:--:--'
        sys.stderr.write(f"\r  [{'#' * fill}{'.' * (30 - fill)}] {i}/{N} {100 * i // N if N else 100:3d}%  elapsed {hms(el)}  eta {eta}  "
                         f"flagged {flagged:,} rows (ok {ok}, skip {skip}, fail {i - ok - skip})  {label[:30]}\033[K")
        sys.stderr.flush()

    bar('')
    while todo or running:
        while todo and len(running) < jobs:
            t, o = todo.pop(0)
            fh = open(LOGS / f"{name(t, o)}.log", 'w')
            running[subprocess.Popen(cmd + ['--test', t, '--omop', o], stdout=fh, stderr=subprocess.STDOUT)] = (t, o, fh)
        for p in [p for p in running if p.poll() is not None]:
            t, o, fh = running.pop(p)
            fh.close()
            rc[(t, o)] = p.returncode
            if p.returncode == 0 and args.mode == 'flag' and line_file(t, o).exists():
                flagged += int(pd.read_csv(line_file(t, o), sep='\t')['flagged'].iloc[0])
            bar(f"{t} / {o}")
        time.sleep(0.2)
    bar('done')
    sys.stderr.write('\n')

    # the run log (per-pair logs in pair order) and the summary
    lines = []
    with open(Path.cwd() / 'run_ref_units.log', 'a') as L:
        L.write(f"=== {started} ref_units.py {args.mode}: input {args.input}, out {args.out}, "
                f"pairs: {which}, {jobs} jobs ({mem}GB, {threads} threads each)\n")
        for t, o in order:
            code = rc[(t, o)]
            L.write(f"--- {t} / {o}" + {0: '\n', NO_CORE: ': no rows with a harmonized value (no core), skipped\n'}.get(code, f": FAILED ({why(code)})\n"))
            L.write((LOGS / f"{name(t, o)}.log").read_text())
            if code == 0 and args.mode == 'flag' and line_file(t, o).exists():
                lines.append(pd.read_csv(line_file(t, o), sep='\t', dtype={'omop': str}))
        ok, skip = sum(v == 0 for v in rc.values()), sum(v == NO_CORE for v in rc.values())
        done = f"done in {hms(time.time() - t1)}: {ok} pairs ok, {skip} skipped (no core), {N - ok - skip} failed"
        L.write(f"=== {datetime.now().isoformat(timespec='seconds')} {done}\n")
    print(done + (f"; {flagged:,} rows with a suggested unit" if args.mode == 'flag' else ''))
    failed = [f"{t} / {o} ({why(code)})" for (t, o), code in rc.items() if code not in (0, NO_CORE)]
    if failed:
        print(f"  failed: {', '.join(failed[:10])}{' ...' if len(failed) > 10 else ''} (logs in {LOGS})")
    if lines:
        summ = pd.concat(lines)
        summ.to_csv(args.out / 'flag_summary.tsv', sep='\t', index=False)
        print(f"  per pair: {args.out / 'flag_summary.tsv'}\n  most flagged (% of rows checked):")
        for r in summ.sort_values('pct_flagged', ascending=False).head(5).itertuples():
            print(f"      {r.test:30} {r.omop:10} {r.pct_flagged:6.2f}%  {r.flagged:,} of {r.rows_checked:,}")
    print(f"  run log: {Path.cwd() / 'run_ref_units.log'}")
    return 0 if not failed else 1


# ---------------------------------------------------------------- command line
if __name__ == '__main__':
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('--input', type=Path, help=f"Engine output (or release) parquet; also names the cache (default: the pair folder's, else {INPUT})")
    common.add_argument('--tmp', type=Path, default=Path('/mnt/disks/data/kanta/tmp/ref_unit'),
                        help="Cache and duckdb spill folder (default: /mnt/disks/data/kanta/tmp/ref_unit)")
    common.add_argument('--limits', type=Path, help=f"*_LIMITS.tsv from ref_text_limits.py, injected by the cache (default: the pair folder's, else {LIMITS})")
    common.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                        help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
    common.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads; fewer threads lower peak memory (default: min(8, cores))")
    common.add_argument('--sample', action='store_true', default=None,
                        help=f"The cache reads only the first {TEST_ROWS:,} input rows; every file gets a _sample suffix")
    pairp = argparse.ArgumentParser(add_help=False)
    pairp.add_argument('out_pos', metavar='OUT', type=Path, nargs='?', help="Shorthand for --out")
    pairp.add_argument('--out', type=Path, help=f"The results folder (one folder per test x OMOP in it), or one pair's folder in it "
                                                f"(has its metadata: that pair, rerun) (default: {D / 'results'})")
    which = pairp.add_mutually_exclusive_group()
    which.add_argument('--test', dest='test_name', help="One pair: TEST_NAME (with --omop)")
    which.add_argument('--top', type=int, help="The N largest pairs (of those with >= --min-count rows)")
    which.add_argument('--random', type=int, help="N random pairs (of those with >= --min-count rows; a quick test run)")
    which.add_argument('--pairs', type=Path, help="The pairs in this TSV (TEST_NAME, OMOP_CONCEPT_ID columns), whatever their rows")
    pairp.add_argument('--omop', help="One pair: OMOP_CONCEPT_ID (with --test)")
    pairp.add_argument('--min-n', type=int, default=5, help="Groups with fewer rows are left out of the outputs (default: 5)")
    pairp.add_argument('--force', action='store_true', help="Relearn the pair even when its folder is up to date")
    pairp.add_argument('--rebuild-cache', action='store_true', help="Rebuild the global cache first, even when up to date")
    pairp.add_argument('--jobs', type=int, help="Several pairs: pairs run at a time; --memory-limit and --threads are split between them; more than 1 can run out of memory on large pairs (default: 1)")
    pairp.add_argument('--min-count', type=int, help="Several pairs: only pairs with at least this many rows, in all_pairs.tsv (default: 1000)")
    pairp.add_argument('--dry-run', action='store_true', help="Several pairs: build the cache and the pair lists, show what would run, run nothing")
    setsp = argparse.ArgumentParser(add_help=False)
    setsp.add_argument('--core-sd', type=float, default=1.0, help="Core = median +- this many robust SDs (1.4826 x MAD) of log10 harmonized value (default: 1)")
    setsp.add_argument('--min-rows', type=int, default=100, help="Rows a unit's learned range needs, per stratum (default: 100)")
    setsp.add_argument('--min-pct', type=float, default=1.0, help="%% of the stratum's core rows with a range a unit needs (default: 1)")
    flagp = argparse.ArgumentParser(add_help=False)
    flagp.add_argument('--margin', type=float, default=1.5, help="A unit matches when both limits are within this factor of its learned range (default: 1.5)")
    flagp.add_argument('--far', type=float, default=3.0, help="Unit not harmonized now: the corrected value must be within this many robust SDs of the median (default: 3)")
    flagp.add_argument('--all-rows', action='store_true', help="Write every row to the row-level file, not only the ones with a suggested unit")

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0], formatter_class=argparse.RawDescriptionHelpFormatter,
                                 epilog='\n'.join(__doc__.splitlines()[2:10]))
    sub = ap.add_subparsers(dest='mode', required=True)
    c = sub.add_parser('cache', parents=[common], help="Build the global cache (every test)")
    c.add_argument('--force', action='store_true', help="Rebuild every cached file")
    sub.add_parser('sets', parents=[common, pairp, setsp], help="cache if needed, then learn the pairs' cores and unit ranges",
                   description=__doc__.split('Assumptions:')[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    sub.add_parser('flag', parents=[common, pairp, setsp, flagp], help="cache and sets if needed, then flag the pairs' rows",
                   description=__doc__.split('Assumptions:')[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    args = ap.parse_args()
    if args.mode != 'cache':
        if args.out_pos and args.out:
            ap.error("give the folder once: OUT or --out")
        args.out = args.out_pos or args.out or D / 'results'
        several = [o for o, v in (('--jobs', args.jobs), ('--min-count', args.min_count), ('--dry-run', args.dry_run)) if v]
        found = sorted(args.out.glob('*metadata*.json'))
        if found:  # one pair's folder: that pair
            if args.test_name or args.omop or args.top or args.random or args.pairs or several:
                ap.error(f"{args.out} is one pair's folder: give the results folder for --test/--omop/--top/--random/--pairs"
                         + (f"/{'/'.join(several)}" if several else ''))
            m = json.loads(found[0].read_text())
            c = m.get('cache', m)  # folders from ref_unit_sets.py kept input/limits at the top level
            args.test_name, args.omop, args.out = m['test_name'], str(m['omop_id']), args.out.parent.parent
            args.input = args.input or (Path(c['input']) if c.get('input') else None)
            args.limits = args.limits or (Path(c['limits']) if c.get('limits') else None)
            if args.sample is None:
                args.sample = bool(c.get('sample_rows_only', c.get('test_rows_only', False)))
            log(f"{found[0].parent}: {args.test_name} / {args.omop}, input {args.input}")
        elif bool(args.test_name) != bool(args.omop):
            ap.error("--test and --omop go together (one pair)")
        elif args.test_name and several:
            ap.error(f"{'/'.join(several)} apply to several pairs, not to --test/--omop")
        args.jobs, args.min_count = args.jobs or 1, args.min_count or 1000
    args.input, args.limits, args.sample = args.input or INPUT, args.limits or LIMITS, bool(args.sample)

    con = connect(args)
    if args.mode == 'cache':
        ensure_cache(args, con, args.force)
        log(f"Cache up to date: {cache_files(args)['meta']}")
        sys.exit(0)
    if not args.test_name:
        sys.exit(run_batch(args, con))
    ensure_cache(args, con, args.rebuild_cache)
    if not learn(args, con, args.test_name, args.omop):
        sys.exit(NO_CORE)
    if args.mode == 'flag':
        flag(args, con, args.test_name, args.omop)
