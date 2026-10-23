#!/usr/bin/env python3
"""Dump the reference-range texts (REFERENCE_RANGE_TEXT, or REFERENCE_RANGE_GROUP with --column) to look for patterns.

Built as a chain of cached parquet files in --tmp, so memory stays bounded (every heavy step is a plain or distinct
count on 8-byte hashes, which duckdb can spill to disk) and a crash or a change only redoes what depends on it:

  input parquet -> ref_text_rows   rows with a text: test name, SP, text, FINNGENID, sex, age (+ ref_coverage .txt)
                   -> ref_rows     the same rows as hashes: k_sp = hash(test name, SP, text), k_test = hash(test name,
                   |               text), k_txt = hash(text), f = hash(FINNGENID), sex (1 male, 0 female), age_y (years)
                   |               -> ref_n_sp / ref_n_test / ref_n_txt   rows and distinct people per key
                   |               -> ref_hist                            rows per k_sp x sex x age year
                   -> ref_keys     each key's test name, SP and text

A file is rebuilt when it is missing or older than any file it is built from (or with --force), so deleting a
file rebuilds it and everything downstream. Files are written under a temporary name and renamed when complete.
File names carry the input name and --column; --test builds the chain from the first 10M input rows, as *_test files.

The outputs are always rewritten (cheap) to --out (aggregates only; groups seen by fewer than --min-people people
are dropped; tabs/newlines inside a text become spaces; with --test the names end in _test). Each has pct_female
(% of the rows with a known sex that are female) and age quantiles age_p05/p25/p50/p75/p95 (whole years, over rows) summed up from ref_hist:
- ref_text_unique.tsv   per text: rows, people, test names, SPs, top 3 test names (% of the text's rows); by rows
- ref_text_by_test.tsv  per TEST_NAME x text: rows, people, SPs; by rows
- ref_text_by_sp.tsv    per TEST_NAME x SP x text: rows, people, SP region/org from sp_region.tsv; by rows
"""
import argparse, os, time
from pathlib import Path
import duckdb

# short name -> (engine output column, release column)
COLUMNS = {
    'f': ('FINNGENID', 'FINNGENID'),
    's': ('SERVICE_PROVIDER', 'SERVICE_PROVIDER_ID'),
    'test_name': ('TEST_NAME_ABBREVIATION', 'TEST_NAME'),
    'ref_text': ('REFERENCE_RANGE_TEXT', 'REFERENCE_RANGE_TEXT'),
    'ref_group': ('REFERENCE_RANGE_GROUP', 'REFERENCE_RANGE_GROUP'),
    'sex': ('SEX', 'SEX'),
    'age': ('EVENT_AGE', 'EVENT_AGE'),
    'ref_low': ('REFERENCE_RANGE_LOWER_VALUE', 'REFERENCE_RANGE_LOW_VALUE'),
    'ref_high': ('REFERENCE_RANGE_UPPER_VALUE', 'REFERENCE_RANGE_HIGH_VALUE'),
}
QUANTILES = (0.05, 0.25, 0.5, 0.75, 0.95)
TEST_ROWS = 10_000_000

RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', required=True, type=Path, help="Engine output (or release) parquet")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/service-provider/ref_text'), help="Output directory")
ap.add_argument('--sp-region', type=Path, default=Path('/mnt/disks/data/kanta/service-provider/region/sp_region.tsv'),
                help="sp_region.tsv from sp_region_cs.py (default: /mnt/disks/data/kanta/service-provider/region/sp_region.tsv)")
ap.add_argument('--column', choices=['ref_text', 'ref_group'], default='ref_text',
                help="ref_text = REFERENCE_RANGE_TEXT (default), ref_group = REFERENCE_RANGE_GROUP")
ap.add_argument('--min-people', type=int, default=5, help="Drop groups seen by fewer people than this (default: 5)")
ap.add_argument('--tmp', type=Path, default=Path('/mnt/disks/data/kanta/tmp/ref_text'),
                help="Cached parquet chain and duckdb spill folder (default: /mnt/disks/data/kanta/tmp/ref_text)")
ap.add_argument('--force', action='store_true', help="Rebuild every cached file")
ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads; fewer threads lower peak memory (default: min(8, cores))")
ap.add_argument('--test', action='store_true', help=f"Build the chain from the first {TEST_ROWS:,} input rows; files and outputs get a _test suffix")
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

sfx = '_test' if args.test else ''
tag = f"{args.input.stem}_{args.column}{sfx}"
F = {name: args.tmp / f"ref_{name}_{tag}.parquet" for name in ('text_rows', 'rows', 'keys', 'n_sp', 'n_test', 'n_txt', 'hist')}
COVERAGE = args.tmp / f"ref_coverage_{tag}.txt"


def build(out, deps, sql):
    """Write `sql` to parquet `out` unless it is newer than all `deps` (make-like); temp name, then rename."""
    if not args.force and out.exists() and all(out.stat().st_mtime >= d.stat().st_mtime for d in deps):
        log(f"Cached {out.name}")
        return
    part = out.with_name(out.name + '.part')
    con.sql(f"copy ({sql}) to '{part}' (format parquet)")
    os.replace(part, out)
    log(f"Built {out.name}")


# --- 1. Text rows from the input, plus the coverage line (rebuilt together)
if args.force or not (F['text_rows'].exists() and COVERAGE.exists()) or F['text_rows'].stat().st_mtime < args.input.stat().st_mtime:
    src = f"(select * from read_parquet('{args.input}')" + (f" limit {TEST_ROWS})" if args.test else ')')
    cols = set(con.sql(f"select * from read_parquet('{args.input}') limit 0").columns)

    def col(name):
        """The input's column for `name` (engine or release spelling), as a cleaned string ("NA"/'' -> null)."""
        c = next((c for c in COLUMNS[name] if c in cols), None)
        if c is None:
            raise SystemExit(f"{args.input}: missing column {COLUMNS[name][0]} / {COLUMNS[name][1]}")
        return f"nullif(nullif(trim(cast(\"{c}\" as varchar)), ''), 'NA')"

    low, high = col('ref_low'), col('ref_high')
    tot = con.sql(f"""select count(*), count({col(args.column)}), count(case when ({low} is not null or {high} is not null) then 1 end)
                      from {src}""").fetchone()
    F['text_rows'].unlink(missing_ok=True)
    build(F['text_rows'], [], f"""
        select coalesce({col('test_name')}, 'NA') as test_name, coalesce({col('s')}, 'NA') as s, {col('f')} as f,
               regexp_replace({col(args.column)}, '[\\t\\r\\n]+', ' ', 'g') as txt, lower({col('sex')}) as sex,
               try_cast({col('age')} as double) as age
        from {src} where {col(args.column)} is not null""")
    COVERAGE.write_text(f"{tot[0]:,} rows: {tot[1]:,} ({100 * tot[1] / max(tot[0], 1):.1f}%) with a {args.column}; {tot[2]:,} with numeric limits\n")
else:
    log(f"Cached {F['text_rows'].name}")
print(COVERAGE.read_text(), end='')

# --- 2. Hashed rows and keys, from the text rows
T = f"read_parquet('{F['text_rows']}')"
build(F['rows'], [F['text_rows']], f"""
    select hash(test_name, s, txt) k_sp, hash(test_name, txt) k_test, hash(txt) k_txt, case when f is not null then hash(f) end f,
           case when sex in ('male', 'm') then 1 when sex in ('female', 'f') then 0 end::tinyint sex,
           try_cast(floor(age) as smallint) age_y
    from {T}""")
build(F['keys'], [F['text_rows']], f"select distinct hash(test_name, s, txt) k_sp, hash(test_name, txt) k_test, hash(txt) k_txt, test_name, s, txt from {T}")

# --- 3. Counts from the hashed rows
R = f"read_parquet('{F['rows']}')"
for key in ('sp', 'test', 'txt'):
    build(F[f'n_{key}'], [F['rows']], f"select k_{key}, count(*) n, count(distinct f) people from {R} group by all")
build(F['hist'], [F['rows']], f"select k_sp, sex, age_y, count(*) n from {R} group by all")

# --- 4. Outputs (always rewritten)
con.sql(f"create temp table keys as select * from read_parquet('{F['keys']}')")
for key in ('sp', 'test', 'txt'):
    con.sql(f"create temp table n_{key} as select * from read_parquet('{F[f'n_{key}']}')")
con.sql(f"create temp table hist_k as select h.*, k.k_test, k.k_txt from read_parquet('{F['hist']}') h join keys k using (k_sp)")


def demo(key):
    """Per `key`: % female among rows with a known sex, and age quantiles (whole years), summed up from hist_k."""
    qs = ', '.join(f"min(age_y) filter (where cum >= {q} * tot) age_p{int(q * 100):02d}" for q in QUANTILES)
    con.sql(f"""create temp table demo_{key} as
        with sx as (select {key}, round(100 * coalesce(sum(n) filter (where sex = 0), 0) / nullif(sum(n) filter (where sex is not null), 0), 1) pct_female
                    from hist_k group by all),
             a as (select {key}, age_y, sum(n) n from hist_k where age_y is not null group by all),
             c as (select *, sum(n) over (partition by {key} order by age_y rows unbounded preceding) cum,
                             sum(n) over (partition by {key}) tot from a),
             q as (select {key}, {qs} from c group by all)
        select * from sx left join q using ({key})""")


for key in ('k_sp', 'k_test', 'k_txt'):
    demo(key)
DEMO_COLS = 'pct_female, ' + ', '.join(f"age_p{int(q * 100):02d}" for q in QUANTILES)

if args.sp_region.exists():
    con.sql(f"""create temp table reg as select s, hva as region, pct_region_of_total, top_csm as org
                from read_csv('{args.sp_region}', delim='\t', header=true, all_varchar=true)""")
else:
    print(f"Note: {args.sp_region} not found; region columns left empty")
    con.sql("create temp table reg (s varchar, region varchar, pct_region_of_total varchar, org varchar)")


def copy(sql, name):
    path = args.out / f"{name}{sfx}.tsv"
    con.sql(f"copy ({sql}) to '{path}' (header, delimiter '\t', quote '')")
    n = con.sql(f"select count(*) from read_csv('{path}', delim='\t', header=true, all_varchar=true, quote='')").fetchone()[0]
    log(f"Wrote {path} ({n:,} rows)")


copy(f"""with kt as (select distinct k_test, k_txt, test_name from keys),
              ks as (select k_txt, any_value(txt) txt, count(distinct test_name) test_names, count(distinct s) sps from keys group by all),
              top as (
                select k_txt, string_agg(test_name || ' (' || round(100 * n / n_txt, 1) || '%)', ', ' order by n desc) top3_test_names
                from (select kt.k_txt, kt.test_name, nt.n, x.n n_txt, row_number() over (partition by kt.k_txt order by nt.n desc) rk
                      from kt join n_test nt using (k_test) join n_txt x using (k_txt))
                where rk <= 3 group by all)
         select ks.txt as {args.column}, x.n, x.people, ks.test_names, ks.sps, top.top3_test_names, {DEMO_COLS}
         from n_txt x join ks using (k_txt) join top using (k_txt) join demo_k_txt using (k_txt)
         where x.people >= {args.min_people} order by x.n desc""",
     'ref_text_unique')

copy(f"""with kt as (select k_test, any_value(test_name) test_name, any_value(txt) txt, count(distinct s) sps from keys group by all)
         select kt.test_name, kt.txt as {args.column}, nt.n, nt.people, kt.sps, {DEMO_COLS}
         from n_test nt join kt using (k_test) join demo_k_test using (k_test)
         where nt.people >= {args.min_people} order by nt.n desc""",
     'ref_text_by_test')

copy(f"""select k.test_name, left(k.s, 5) sp, reg.region, reg.pct_region_of_total, reg.org, k.txt as {args.column}, ns.n, ns.people, {DEMO_COLS}
         from n_sp ns join keys k using (k_sp) join demo_k_sp using (k_sp) left join reg on reg.s = k.s
         where ns.people >= {args.min_people} order by ns.n desc""",
     'ref_text_by_sp')
