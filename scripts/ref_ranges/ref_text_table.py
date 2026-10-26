#!/usr/bin/env python3
"""Count rows per TEST_NAME x reference-range text, to see which texts could fill the numeric reference-range columns.

The text is REFERENCE_RANGE_TEXT, else REFERENCE_RANGE_GROUP (the same text with its spaces removed; it is filled
on some rows without a text). One pass over the input, rows with a text only. Writes --out/ref_text_table.tsv (aggregates only, sorted by n; tabs/newlines inside a
text become spaces; pairs with fewer than --min-n rows or --min-people people are dropped) with columns:
  test_name, ref_text, n (rows), pct (% of all rows with a text, before the filters),
  cum_pct (running sum of pct down the file), pct_group (% of the pair's rows whose text came from the group)
Distinct people are counted in two plain group-bys (pair x person, then pair), which duckdb can spill to disk.
"""
import argparse, os, time
from pathlib import Path
import duckdb

# short name -> (engine output column, release column)
COLUMNS = {
    'f': ('FINNGENID', 'FINNGENID'),
    'test_name': ('TEST_NAME_ABBREVIATION', 'TEST_NAME'),
    'ref_text': ('REFERENCE_RANGE_TEXT', 'REFERENCE_RANGE_TEXT'),
    'ref_group': ('REFERENCE_RANGE_GROUP', 'REFERENCE_RANGE_GROUP'),
}
TEST_ROWS = 10_000_000

RAM_GB = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30
ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
ap.add_argument('--input', required=True, type=Path, help="Engine output (or release) parquet")
ap.add_argument('--out', type=Path, default=Path('/mnt/disks/data/kanta/service-provider/ref_text'),
                help="Output directory (default: /mnt/disks/data/kanta/service-provider/ref_text)")
ap.add_argument('--min-n', type=int, default=1000, help="Drop pairs with fewer rows than this (default: 1000)")
ap.add_argument('--min-people', type=int, default=5, help="Drop pairs seen by fewer people than this (default: 5)")
ap.add_argument('--tmp', type=Path, default=Path('/mnt/disks/data/kanta/tmp/ref_text'),
                help="duckdb spill folder (default: /mnt/disks/data/kanta/tmp/ref_text)")
ap.add_argument('--memory-limit', default=f"{int(RAM_GB * 0.5)}GB",
                help=f"duckdb memory limit; it spills to --tmp beyond it (default: 50%% of RAM, here {int(RAM_GB * 0.5)}GB)")
ap.add_argument('--threads', type=int, default=min(8, os.cpu_count() or 1), help="duckdb threads; fewer threads lower peak memory (default: min(8, cores))")
ap.add_argument('--test', action='store_true', help=f"Read only the first {TEST_ROWS:,} input rows; the output gets a _test suffix")
args = ap.parse_args()
args.out.mkdir(parents=True, exist_ok=True)
OUT = args.out / f"ref_text_table{'_test' if args.test else ''}.tsv"
(args.tmp / 'duckdb').mkdir(parents=True, exist_ok=True)
t0 = time.time()
log = lambda msg: print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)

con = duckdb.connect()
con.sql(f"set temp_directory = '{args.tmp / 'duckdb'}'")
con.sql("set preserve_insertion_order = false")
con.sql(f"set memory_limit = '{args.memory_limit}'")
con.sql(f"set threads = {args.threads}")
log(f"duckdb: memory limit {args.memory_limit} (machine has {RAM_GB:.0f}GB), {args.threads} threads, spilling to {args.tmp / 'duckdb'}")

src = f"(select * from read_parquet('{args.input}')" + (f" limit {TEST_ROWS})" if args.test else ')')
cols = set(con.sql(f"select * from read_parquet('{args.input}') limit 0").columns)


def col(name):
    """The input's column for `name` (engine or release spelling), as a cleaned string ("NA"/'' -> null)."""
    c = next((c for c in COLUMNS[name] if c in cols), None)
    if c is None:
        raise SystemExit(f"{args.input}: missing column {COLUMNS[name][0]} / {COLUMNS[name][1]}")
    return f"nullif(nullif(trim(cast(\"{c}\" as varchar)), ''), 'NA')"


part = OUT.with_name(OUT.name + '.part')
con.sql(f"""copy (
    with per_person as (
        select coalesce({col('test_name')}, 'NA') as test_name,
               regexp_replace(coalesce({col('ref_text')}, {col('ref_group')}), '[\\t\\r\\n]+', ' ', 'g') as ref_text,
               {col('f')} as f, count(*) n, count(*) filter (where {col('ref_text')} is null) n_group
        from {src} where coalesce({col('ref_text')}, {col('ref_group')}) is not null group by all)
    , pairs as (select test_name, ref_text, sum(n)::bigint n, sum(n_group) n_group, count(f) people from per_person group by all)
    , kept as (select test_name, ref_text, n, n_group, 100 * n / sum(n) over () pct from pairs
               qualify n >= {args.min_n} and people >= {args.min_people})
    select test_name, ref_text, n, round(pct, 3) pct,
           round(sum(pct) over (order by n desc, test_name, ref_text rows unbounded preceding), 3) cum_pct,
           round(100 * n_group / n, 1) pct_group
    from kept order by n desc, test_name, ref_text
) to '{part}' (header, delimiter '\t', quote '')""")
os.replace(part, OUT)
n, rows, pct, grp = con.sql(f"""select count(*), sum(n), sum(pct), sum(n * pct_group / 100)
                              from read_csv('{OUT}', delim='\t', header=true, quote='')""").fetchone()
log(f"Wrote {OUT}: {n:,} pairs, {rows:,} rows ({pct:.1f}% of all rows with a text), ~{grp:,.0f} of them from the group")
