#!/usr/bin/env python3
"""
prevalence_analysis.py

For each of a set of Kanta lab columns (e.g. TEST_NAME), compare how often
each category value appears among lab rows drawn during a hospitalization
episode vs. outside one. For every category value: an odds ratio, a
Fisher's-exact p-value (Benjamini-Hochberg FDR-corrected across all values
tested for that column), and a saved TSV + a histogram of log2(OR) across
all values.

The Kanta release file is scanned in row-group batches (never fully loaded),
each batch immediately collapsed to running per-category counts -- only
those small running totals are held in memory regardless of how much of the
release file is scanned.

Usage:
  python3 prevalence_analysis.py [--kanta-input PATH] [--intervals-input PATH]
                                  [--columns TEST_NAME,OMOP_CONCEPT_ID,CODING_SYSTEM_ORG]
                                  [--min-count 30] [--chunksize 2000000] [--out PATH]
  python3 prevalence_analysis.py --test [--test-chunks 10]   # stop early, for fast iteration
"""

import argparse
import tempfile
import urllib.request
from collections import Counter
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import pyarrow.parquet as pq
from scipy.stats import fisher_exact
from tqdm import tqdm

DEFAULT_ENGINE_DIR = Path("/mnt/disks/data/kanta/engine/")
DEFAULT_INTERVALS_INPUT = Path("/mnt/disks/data/kanta/hospital/results/hospitalization_intervals_inpat.tsv")
DEFAULT_OUT_DIR = Path("/mnt/disks/data/kanta/hospital/results/")
DEFAULT_COLUMNS = ["TEST_NAME", "OMOP_CONCEPT_ID", "CODING_SYSTEM_ORG"]
DEFAULT_CHUNKSIZE = 2_000_000

# Full Usagi export (conceptId/conceptName) -- unlike the engine's own trimmed
# LABfi.tsv (MAPPINGS/), this one is only used here to label OMOP_CONCEPT_ID
# results for readability, so it's fetched to a temp file and discarded rather
# than treated as a real reference-data dependency of the engine.
OMOP_CONCEPT_NAMES_URL = (
    "https://raw.githubusercontent.com/FINNGEN/kanta_lab_harmonisation_public/"
    "refs/heads/kanta_v4/VOCABULARIES/LABfi_ALL/LABfi_ALL.usagi.csv"
)


def fetch_omop_concept_names() -> dict[str, str]:
    """Download the full Usagi export to a temp file, build a conceptId ->
    conceptName lookup, then discard the file."""
    with tempfile.NamedTemporaryFile(suffix=".csv") as tmp:
        urllib.request.urlretrieve(OMOP_CONCEPT_NAMES_URL, tmp.name)
        df = pd.read_csv(tmp.name, dtype=str, usecols=["conceptId", "conceptName"])
    return dict(zip(df["conceptId"], df["conceptName"]))


def resolve_kanta_input() -> Path:
    """Default to the most recently modified *_RELEASE.parquet in the engine dir,
    since the dated filename changes with every new engine run."""
    candidates = sorted(DEFAULT_ENGINE_DIR.glob("*_RELEASE.parquet"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise SystemExit(f"no *_RELEASE.parquet found under {DEFAULT_ENGINE_DIR} -- pass --kanta-input")
    return candidates[-1]


def load_intervals(intervals_path: Path) -> pd.DataFrame:
    intervals = pd.read_csv(intervals_path, sep="\t")
    # normalize to plain object-dtype strings -- CSV- and parquet-derived
    # FINNGENID columns otherwise land on different pandas string backends
    # (StringDtype variants), which merge_asof refuses to join on.
    intervals["FINNGENID"] = intervals["FINNGENID"].astype(str)
    # merge_asof requires the "on" column sorted overall (not just per "by"-group)
    return intervals.sort_values("START_AGE").reset_index(drop=True)


def tag_in_hospital(batch: pd.DataFrame, intervals: pd.DataFrame) -> pd.DataFrame:
    """Vectorized interval-containment join: for each (FINNGENID, EVENT_AGE) row,
    find the last episode (by START_AGE) that started at or before EVENT_AGE for
    that patient, then check EVENT_AGE still falls within it. merge_asof requires
    both sides sorted by the join key and works per-group via `by` -- intervals
    are already non-overlapping (build_intervals.py's merge step guarantees that),
    which is exactly what makes "nearest preceding start" a correct containment
    check rather than an approximation."""
    # merge_asof requires the "on" column (EVENT_AGE) sorted overall, not just
    # within each FINNGENID group -- `by` handles the per-patient grouping itself.
    batch = batch.assign(
        FINNGENID=batch["FINNGENID"].astype(str),
        EVENT_AGE=batch["EVENT_AGE"].astype("float64"),
    ).sort_values("EVENT_AGE")
    merged = pd.merge_asof(
        batch, intervals[["FINNGENID", "START_AGE", "END_AGE"]],
        left_on="EVENT_AGE", right_on="START_AGE", by="FINNGENID", direction="backward",
    )
    merged["IN_HOSPITAL"] = merged["END_AGE"].notna() & (merged["EVENT_AGE"] <= merged["END_AGE"])
    return merged


def benjamini_hochberg(p_values: pd.Series) -> pd.Series:
    """Benjamini-Hochberg FDR correction (no statsmodels dependency for one function)."""
    n = len(p_values)
    order = p_values.argsort()
    ranks = pd.Series(range(1, n + 1), index=p_values.index[order])
    adjusted = (p_values.iloc[order].values * n / ranks.values)
    # enforce monotonicity (BH step-up), then clip to [0, 1]
    adjusted = pd.Series(adjusted, index=p_values.index[order]).iloc[::-1].cummin().iloc[::-1]
    return adjusted.clip(upper=1.0).reindex(p_values.index)


def summarize_column(counts_in: Counter, counts_out: Counter, n_in: int, n_out: int, min_count: int) -> pd.DataFrame:
    values = set(counts_in) | set(counts_out)
    rows = []
    for v in values:
        c_in = counts_in.get(v, 0)
        c_out = counts_out.get(v, 0)
        if c_in + c_out < min_count:
            continue
        table = [[c_in, n_in - c_in], [c_out, n_out - c_out]]
        odds_ratio, p_value = fisher_exact(table)
        rows.append((v, c_in, c_out, c_in + c_out, c_in / n_in, c_out / n_out, odds_ratio, p_value))

    result = pd.DataFrame(
        rows, columns=["VALUE", "N_IN", "N_OUT", "N_TOTAL", "PREV_IN", "PREV_OUT", "ODDS_RATIO", "P_VALUE"]
    )
    if result.empty:
        return result
    result["P_ADJ_FDR"] = benjamini_hochberg(result["P_VALUE"])
    result["LOG2_OR"] = pd.Series(
        [float("inf") if pd.isna(o) or o in (float("inf"),) else
         float("-inf") if o == 0 else
         __import__("math").log2(o) for o in result["ODDS_RATIO"]]
    )
    return result.sort_values("ODDS_RATIO", ascending=False).reset_index(drop=True)


def plot_histogram(result: pd.DataFrame, column: str, out_path: Path) -> None:
    finite = result.loc[result["LOG2_OR"].replace([float("inf"), float("-inf")], pd.NA).notna(), "LOG2_OR"]
    n_clipped = len(result) - len(finite)

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.hist(finite, bins=40, color="#4C72B0", edgecolor="white", linewidth=0.5)
    ax.axvline(0, color="#555555", linewidth=1, linestyle="--")
    ax.set_xlabel("log2(odds ratio), in-hospital vs. outside")
    ax.set_ylabel(f"number of {column} values")
    title = f"{column}: prevalence shift, in-hospital vs. outside"
    if n_clipped:
        title += f"  ({n_clipped} infinite-OR values omitted from plot)"
    ax.set_title(title, fontsize=10)
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#DDDDDD", linewidth=0.5)
    ax.set_axisbelow(True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kanta-input", type=Path, default=None,
                         help="Kanta RELEASE parquet. Default: most recent *_RELEASE.parquet "
                              f"under {DEFAULT_ENGINE_DIR}")
    parser.add_argument("--intervals-input", type=Path, default=DEFAULT_INTERVALS_INPUT,
                         help=f"Hospitalization intervals TSV (build_intervals.py output). Default: {DEFAULT_INTERVALS_INPUT}")
    parser.add_argument("--columns", default=",".join(DEFAULT_COLUMNS),
                         help=f"Comma-separated Kanta columns to analyze. Default: {','.join(DEFAULT_COLUMNS)}")
    parser.add_argument("--min-count", type=int, default=100,
                         help="Ignore category values with fewer than this many total rows (in+out)")
    parser.add_argument("--chunksize", type=int, default=DEFAULT_CHUNKSIZE, help="Rows per read batch")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT_DIR, help="Output directory")
    parser.add_argument("--test", action="store_true",
                         help="Stop after --test-chunks batches, for fast iteration on real data")
    parser.add_argument("--test-chunks", type=int, default=10,
                         help="With --test: number of batches to read before stopping (default 10, "
                              f"i.e. {10 * DEFAULT_CHUNKSIZE:,} rows at the default chunksize)")
    args = parser.parse_args()

    columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    kanta_input = args.kanta_input or resolve_kanta_input()

    if not args.intervals_input.exists():
        raise SystemExit(
            f"intervals file not found at {args.intervals_input} -- run build_intervals.py first"
        )

    print(f"kanta input: {kanta_input}")
    print(f"intervals input: {args.intervals_input}")
    print(f"columns: {columns}")

    intervals = load_intervals(args.intervals_input)

    needed_cols = ["FINNGENID", "EVENT_AGE"] + columns
    pf = pq.ParquetFile(kanta_input)
    total_rows = pf.metadata.num_rows
    n_batches = args.test_chunks if args.test else None

    counts_in = {c: Counter() for c in columns}
    counts_out = {c: Counter() for c in columns}
    n_in = n_out = 0

    with tqdm(total=total_rows, desc="scanning Kanta release", unit="rows", unit_scale=True) as pbar:
        for i, record_batch in enumerate(pf.iter_batches(batch_size=args.chunksize, columns=needed_cols)):
            batch = record_batch.to_pandas()
            tagged = tag_in_hospital(batch, intervals)

            in_mask = tagged["IN_HOSPITAL"]
            n_in += int(in_mask.sum())
            n_out += int((~in_mask).sum())
            for c in columns:
                vc = tagged.loc[in_mask, c].value_counts()
                counts_in[c].update(dict(zip(vc.index, vc.values)))
                vc = tagged.loc[~in_mask, c].value_counts()
                counts_out[c].update(dict(zip(vc.index, vc.values)))

            pbar.update(len(batch))
            if n_batches is not None and i + 1 >= n_batches:
                break

    print(f"\n{n_in:,} rows in-hospital, {n_out:,} rows outside ({100 * n_in / (n_in + n_out):.1f}% in-hospital)")

    concept_names = fetch_omop_concept_names() if "OMOP_CONCEPT_ID" in columns else {}

    args.out.mkdir(parents=True, exist_ok=True)
    for c in columns:
        result = summarize_column(counts_in[c], counts_out[c], n_in, n_out, args.min_count)
        if result.empty:
            print(f"{c}: no category value reached --min-count={args.min_count}, skipping")
            continue
        if c == "OMOP_CONCEPT_ID":
            result.insert(1, "OMOP_CONCEPT_NAME", result["VALUE"].map(concept_names))
        out_tsv = args.out / f"prevalence_{c.lower()}.tsv"
        # scientific notation for every float column (N_IN/N_OUT/N_TOTAL stay
        # plain integers -- they're counts, not measured quantities)
        sci_cols = ["PREV_IN", "PREV_OUT", "ODDS_RATIO", "P_VALUE", "P_ADJ_FDR", "LOG2_OR"]
        formatted = result.assign(**{col: result[col].map(lambda x: f"{x:.3e}") for col in sci_cols})
        formatted.to_csv(out_tsv, sep="\t", index=False)
        out_png = args.out / f"prevalence_{c.lower()}_hist.png"
        plot_histogram(result, c, out_png)
        n_sig = int((result["P_ADJ_FDR"] < 0.05).sum())
        print(f"{c}: {len(result)} values (n_total>={args.min_count}), {n_sig} significant at FDR<0.05 "
              f"-> {out_tsv}, {out_png}")


if __name__ == "__main__":
    main()
