#!/usr/bin/env python3
"""
build_intervals.py

Build per-patient hospitalization interval tables from the FinnGen detailed
longitudinal register file. Each output row is a merged (non-overlapping)
episode of hospital care, derived from a single register SOURCE (INPAT by
default).

A "visit" is one (FINNGENID, INDEX) group in the source file -- the source
file carries several rows per visit (one per diagnosis code), all sharing the
same EVENT_AGE and CODE4 (duration of stay in days). Visit intervals are
[EVENT_AGE, EVENT_AGE + CODE4 / 365.25], then overlapping/touching visits for
the same patient are merged into episodes.

Usage:
  python3 build_intervals.py [--input PATH] [--out PATH] [--source INPAT] [--chunksize N]
  python3 build_intervals.py --test   # run against the synthetic fixture in test_data/
"""

import argparse
import gzip
from pathlib import Path

import pandas as pd
from tqdm import tqdm

DEFAULT_INPUT = Path("/mnt/disks/data/kanta/hospital/finngen_R14_detailed_longitudinal_2.0.txt.gz")
DEFAULT_OUT_DIR = Path("/mnt/disks/data/kanta/hospital/results/")

TEST_DATA_DIR = Path(__file__).parent / "test_data"
TEST_INPUT = TEST_DATA_DIR / "detailed_longitudinal_test.txt.gz"

USECOLS = ["FINNGENID", "SOURCE", "EVENT_AGE", "CODE4", "INDEX"]
DTYPES = {
    "FINNGENID": "string",
    "SOURCE": "string",
    "EVENT_AGE": "float64",
    "CODE4": "float64",
    "INDEX": "string",
}

DAYS_PER_YEAR = 365.25


def filtered_path_for(input_path: Path, source: str) -> Path:
    """Path of the cached SOURCE-only extract, alongside the original file."""
    name = input_path.name
    base = name[: -len(".txt.gz")] if name.endswith(".txt.gz") else input_path.stem
    return input_path.with_name(f"{base}_{source}_ONLY.txt.gz")


def ensure_filtered_file(input_path: Path, source: str) -> Path:
    """Return a SOURCE-only extract of `input_path`, building and caching it
    (alongside the original file) on first use. Subsequent runs -- with the
    same input and source -- skip straight to the cached, much smaller file
    instead of re-scanning the full register on every run."""
    out_path = filtered_path_for(input_path, source)
    if out_path.exists():
        print(f"using cached {source}-only extract: {out_path}")
        return out_path

    print(f"no cached {source}-only extract found -- building {out_path} (one-time, reused on later runs)")
    tmp_path = out_path.with_suffix(out_path.suffix + ".tmp")
    with open(input_path, "rb") as raw_fh:
        with tqdm.wrapattr(raw_fh, "read", total=input_path.stat().st_size,
                            desc=f"building {source}-only extract", unit="B", unit_scale=True) as progressed_fh:
            with gzip.open(progressed_fh, "rt") as in_fh, gzip.open(tmp_path, "wt") as out_fh:
                header = next(in_fh)
                out_fh.write(header)
                source_col = header.rstrip("\n").split("\t").index("SOURCE")
                n_scanned = n_matched = 0
                for line in in_fh:
                    n_scanned += 1
                    if line.split("\t", source_col + 1)[source_col] == source:
                        out_fh.write(line)
                        n_matched += 1
    tmp_path.rename(out_path)  # atomic: a run stopped mid-build never leaves a bad cache at out_path
    print(f"built {source}-only extract: {n_scanned:,} lines scanned, {n_matched:,} {source} rows -> {out_path}")
    return out_path


def stream_visits(input_path: Path, chunksize: int, source: str) -> pd.DataFrame:
    """Stream the longitudinal file in chunks, keep only `source` rows, and
    dedupe to one row per visit (FINNGENID, INDEX).

    Streaming keeps peak memory low regardless of the source file's total
    size, since only the (small) filtered subset is accumulated. Progress is
    tracked against compressed bytes read, since row count isn't known
    upfront without a separate full pass.
    """
    per_chunk = []
    with open(input_path, "rb") as raw_fh:
        with tqdm.wrapattr(raw_fh, "read", total=input_path.stat().st_size,
                            desc=f"scanning for {source}", unit="B", unit_scale=True) as fh:
            reader = pd.read_csv(
                fh,
                sep="\t",
                usecols=USECOLS,
                dtype=DTYPES,
                na_values="NA",
                chunksize=chunksize,
                compression="gzip",
            )
            for chunk in reader:
                sub = chunk.loc[chunk["SOURCE"] == source]
                if sub.empty:
                    continue
                per_chunk.append(
                    sub.groupby(["FINNGENID", "INDEX"], as_index=False).agg(
                        EVENT_AGE=("EVENT_AGE", "first"),
                        CODE4=("CODE4", "first"),
                        N_ROWS=("EVENT_AGE", "size"),
                    )
                )

    if not per_chunk:
        return pd.DataFrame(columns=["FINNGENID", "INDEX", "EVENT_AGE", "CODE4", "N_ROWS"])

    visits = pd.concat(per_chunk, ignore_index=True)
    # A visit's rows can straddle a chunk boundary, producing duplicate
    # (FINNGENID, INDEX) partial groups across chunks; collapse those too.
    visits = visits.groupby(["FINNGENID", "INDEX"], as_index=False).agg(
        EVENT_AGE=("EVENT_AGE", "first"),
        CODE4=("CODE4", "first"),
        N_ROWS=("N_ROWS", "sum"),
    )
    return visits


def merge_intervals(visits: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collapse each patient's per-visit (start, end) intervals into
    non-overlapping episodes of hospital care.

    Returns (episodes, visits) -- visits gains START_AGE/END_AGE/EPISODE_ID
    columns so callers can trace which visits fed which episode.
    """
    visits = visits.assign(
        START_AGE=visits["EVENT_AGE"],
        END_AGE=visits["EVENT_AGE"] + visits["CODE4"].fillna(0) / DAYS_PER_YEAR,
    ).sort_values(["FINNGENID", "START_AGE"]).reset_index(drop=True)

    episodes = []
    episode_ids = []
    cur_fid = cur_start = cur_end = None
    for fid, start, end in tqdm(
        visits[["FINNGENID", "START_AGE", "END_AGE"]].itertuples(index=False),
        total=len(visits), desc="merging visits into episodes",
    ):
        if cur_fid is None or fid != cur_fid or start > cur_end:
            if cur_fid is not None:
                episodes.append((cur_fid, cur_start, cur_end))
            cur_fid, cur_start, cur_end = fid, start, end
        else:
            cur_end = max(cur_end, end)
        episode_ids.append(len(episodes))
    if cur_fid is not None:
        episodes.append((cur_fid, cur_start, cur_end))

    visits = visits.assign(EPISODE_ID=episode_ids)
    episodes_df = pd.DataFrame(episodes, columns=["FINNGENID", "START_AGE", "END_AGE"])
    episodes_df.index.name = "EPISODE_ID"
    return episodes_df.reset_index(), visits


def read_raw_rows(input_path: Path, source: str) -> pd.DataFrame:
    """Read the full file (small --test fixtures only) and return the raw,
    unaggregated rows for `source`, sorted by (FINNGENID, EVENT_AGE)."""
    df = pd.read_csv(input_path, sep="\t", usecols=USECOLS, dtype=DTYPES, na_values="NA")
    return (
        df.loc[df["SOURCE"] == source, ["FINNGENID", "INDEX", "EVENT_AGE", "CODE4"]]
        .sort_values(["FINNGENID", "EVENT_AGE"])
    )


def print_trace(raw: pd.DataFrame, visits: pd.DataFrame, episodes: pd.DataFrame) -> None:
    """Print the raw input rows and the logic applied to turn them into
    episodes: dedupe (FINNGENID, INDEX) -> visit, visit -> interval, then
    overlapping/touching visits -> merged episode."""
    visits = visits.sort_values(["FINNGENID", "EVENT_AGE"])

    print(f"raw rows (SOURCE, sorted by FINNGENID, EVENT_AGE):")
    print(raw.to_string(index=False))

    print("\nstep 1 -- dedupe rows sharing (FINNGENID, INDEX) into one visit:")
    any_dedupe = False
    for fid, index, age, code4, n_rows in visits[
        ["FINNGENID", "INDEX", "EVENT_AGE", "CODE4", "N_ROWS"]
    ].itertuples(index=False):
        if n_rows > 1:
            any_dedupe = True
            print(f"  {fid} INDEX={index}: {n_rows} raw rows -> 1 visit (EVENT_AGE={age:.3f}, CODE4={code4:g})")
    if not any_dedupe:
        print("  (no visit had more than one raw row)")

    print("\nstep 2 -- visit -> interval [EVENT_AGE, EVENT_AGE + CODE4 / 365.25]:")
    for fid, index, age, code4 in visits[["FINNGENID", "INDEX", "EVENT_AGE", "CODE4"]].itertuples(index=False):
        duration = 0 if pd.isna(code4) else code4
        end = age + duration / DAYS_PER_YEAR
        code4_repr = "NA->0" if pd.isna(code4) else f"{code4:g}"
        print(f"  {fid} INDEX={index}: EVENT_AGE={age:.3f} CODE4={code4_repr} -> [{age:.3f}, {end:.3f}]")

    print("\nstep 3 -- merge overlapping/touching visit intervals per patient:")
    for _, ep_visits in visits.groupby("EPISODE_ID"):
        fid = ep_visits["FINNGENID"].iloc[0]
        ep = episodes.loc[
            (episodes["FINNGENID"] == fid)
            & (episodes["START_AGE"] == ep_visits["START_AGE"].min())
        ].iloc[0]
        if len(ep_visits) > 1:
            spans = ", ".join(f"[{s:.3f}, {e:.3f}]" for s, e in
                               ep_visits[["START_AGE", "END_AGE"]].itertuples(index=False))
            print(f"  {fid}: visits {spans} overlap/touch -> merged into [{ep.START_AGE:.3f}, {ep.END_AGE:.3f}]")
        else:
            print(f"  {fid}: visit [{ep.START_AGE:.3f}, {ep.END_AGE:.3f}] has no neighbor to merge with")

    print(f"\n{len(visits)} visits -> {len(episodes)} episodes")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=None,
                         help=f"Detailed longitudinal file (.txt.gz). Default: {DEFAULT_INPUT} "
                              f"(or {TEST_INPUT} with --test)")
    parser.add_argument("--out", type=Path, default=None,
                         help=f"Output directory (ignored with --test, which prints to screen instead). "
                              f"Default: {DEFAULT_OUT_DIR}")
    parser.add_argument("--source", default="INPAT", help="Register SOURCE value to treat as hospitalization")
    parser.add_argument("--chunksize", type=int, default=2_000_000, help="Rows per read chunk")
    parser.add_argument("--test", action="store_true",
                         help="Run against the synthetic fixture in test_data/ instead of real data")
    args = parser.parse_args()

    input_path = args.input or (TEST_INPUT if args.test else DEFAULT_INPUT)

    if args.test and not input_path.exists():
        raise SystemExit(
            f"test fixture not found at {input_path} -- run generate_test_data.py first"
        )

    if args.test:
        # --test never writes a file, and the fixture is tiny -- read it directly,
        # no need for a cached SOURCE-only extract.
        visits = stream_visits(input_path, args.chunksize, args.source)
        episodes, visits = merge_intervals(visits)
        raw = read_raw_rows(input_path, args.source)
        print_trace(raw, visits, episodes)
        return

    scan_path = ensure_filtered_file(input_path, args.source)
    visits = stream_visits(scan_path, args.chunksize, args.source)
    episodes, visits = merge_intervals(visits)

    out_dir = args.out or DEFAULT_OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"hospitalization_intervals_{args.source.lower()}.tsv"
    episodes[["FINNGENID", "START_AGE", "END_AGE"]].to_csv(out_path, sep="\t", index=False)
    print(f"{len(visits)} visits -> {len(episodes)} episodes -> {out_path}")


if __name__ == "__main__":
    main()
