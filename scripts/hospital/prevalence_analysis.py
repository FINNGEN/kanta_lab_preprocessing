#!/usr/bin/env python3
"""
prevalence_analysis.py

For each of a set of Kanta lab columns (e.g. TEST_NAME), compare how often
each category value appears among lab rows drawn during a hospitalization
episode vs. outside one. For every category value: the fraction of its rows
drawn in-hospital, an odds ratio with a 95% CI (Woolf, +0.5 on every cell so
zero counts give finite ORs), a G-test log10 p-value (Benjamini-Hochberg
FDR-corrected across all values tested for that column), and a saved TSV +
a histogram of log2(OR) across all values.

p-values are kept on the log10 scale throughout: with hundreds of millions of
rows they routinely fall below float64's ~1e-308 and would underflow to 0.
Even so, rows from the same patient are not independent, so treat them as a
coarse gate only -- rank and filter on the OR / its CI / FRAC_IN instead.

The Kanta release file is scanned in row-group batches (never fully loaded),
each batch immediately collapsed to running per-category counts -- only
those small running totals are held in memory regardless of how much of the
release file is scanned.

Usage:
  python3 prevalence_analysis.py [--kanta-input PATH] [--intervals-input PATH]
                                  [--columns TEST_NAME,OMOP_CONCEPT_ID,CODING_SYSTEM_ORG]
                                  [--min-count 30] [--chunksize 2000000] [--out PATH]
                                  [--provider-orgs PATH]
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
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from scipy.stats import norm
from tqdm import tqdm

DEFAULT_ENGINE_DIR = Path("/mnt/disks/data/kanta/engine/")
DEFAULT_INTERVALS_INPUT = Path("/mnt/disks/data/kanta/hospital/results/hospitalization_intervals_inpat.tsv")
DEFAULT_OUT_DIR = Path("/mnt/disks/data/kanta/hospital/results/")
DEFAULT_COLUMNS = ["TEST_NAME", "OMOP_CONCEPT_ID", "CODING_SYSTEM_ORG","SERVICE_PROVIDER_ID"]
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


def load_provider_orgs(path: Path) -> dict[str, str]:
    """SERVICE_PROVIDER_ID -> best-matching organization name, from a TSV with at
    least columns `s` (provider id) and `orgname`. Only used to label results."""
    orgs = pd.read_csv(path, sep="\t", dtype=str, usecols=["s", "orgname"])
    return dict(zip(orgs["s"], orgs["orgname"]))


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


def benjamini_hochberg_log10(log10_p: pd.Series) -> pd.Series:
    """Benjamini-Hochberg FDR correction done in log10 space, so p-values far
    below float64's range stay distinguishable (no statsmodels dependency)."""
    n = len(log10_p)
    order = log10_p.argsort()
    ranks = np.arange(1, n + 1)
    adjusted = log10_p.iloc[order].values + np.log10(n) - np.log10(ranks)
    # enforce monotonicity (BH step-up), then clip to p <= 1
    adjusted = pd.Series(adjusted, index=log10_p.index[order]).iloc[::-1].cummin().iloc[::-1]
    return adjusted.clip(upper=0.0).reindex(log10_p.index)


def g_test_log10_p(a, b, c, d) -> np.ndarray:
    """Log-likelihood-ratio (G) test on 2x2 tables [[a, b], [c, d]], vectorized,
    returning log10 p directly so it never underflows. At these sample sizes it
    agrees with Fisher's exact test and is far cheaper."""
    obs = np.stack([a, b, c, d]).astype("float64")
    row1, row2 = a + b, c + d
    col1, col2 = a + c, b + d
    total = row1 + row2
    exp = np.stack([row1 * col1, row1 * col2, row2 * col1, row2 * col2]) / total
    with np.errstate(divide="ignore", invalid="ignore"):
        terms = np.where(obs > 0, obs * np.log(obs / exp), 0.0)  # 0 * log 0 := 0
    g = np.clip(2 * terms.sum(axis=0), 0, None)
    # chi2(df=1).sf(g) == 2 * norm.sf(sqrt(g)); unlike chi2.logsf, norm.logsf
    # uses an asymptotic expansion in the far tail and stays finite
    return (np.log(2) + norm.logsf(np.sqrt(g))) / np.log(10)


def summarize_column(counts_in: Counter, counts_out: Counter, n_in: int, n_out: int, min_count: int) -> pd.DataFrame:
    values = set(counts_in) | set(counts_out)
    rows = [(v, counts_in.get(v, 0), counts_out.get(v, 0)) for v in values]
    result = pd.DataFrame(rows, columns=["VALUE", "N_IN", "N_OUT"])
    result["N_TOTAL"] = result["N_IN"] + result["N_OUT"]
    result = result[result["N_TOTAL"] >= min_count].reset_index(drop=True)
    if result.empty:
        return result

    a = result["N_IN"].to_numpy("float64")
    c = result["N_OUT"].to_numpy("float64")
    b, d = n_in - a, n_out - c

    result["FRAC_IN"] = a / (a + c)
    result["PREV_IN"] = a / n_in
    result["PREV_OUT"] = c / n_out
    # +0.5 on every cell (Haldane-Anscombe) keeps the OR and its SE finite when a
    # value never occurs on one side; negligible for anything with real counts.
    ln_or = np.log((a + 0.5) * (d + 0.5) / ((b + 0.5) * (c + 0.5)))
    se = np.sqrt(1 / (a + 0.5) + 1 / (b + 0.5) + 1 / (c + 0.5) + 1 / (d + 0.5))
    z = norm.ppf(0.975)
    result["ODDS_RATIO"] = np.exp(ln_or)
    result["OR_CI_LOW"] = np.exp(ln_or - z * se)
    result["OR_CI_HIGH"] = np.exp(ln_or + z * se)
    result["LOG2_OR"] = ln_or / np.log(2)
    result["LOG10_P"] = g_test_log10_p(a, b, c, d)
    result["LOG10_P_ADJ_FDR"] = benjamini_hochberg_log10(result["LOG10_P"])
    # rank by the conservative end of the CI: a huge point OR built on a handful
    # of rows has a wide CI and sinks, a large well-supported OR rises
    return result.sort_values("OR_CI_LOW", ascending=False).reset_index(drop=True)


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
    parser.add_argument("--provider-orgs", type=Path, default=None,
                         help="Optional TSV mapping SERVICE_PROVIDER_ID (column `s`) to an organization "
                              "name (column `orgname`); appended as a final BEST_ORG column to the "
                              "SERVICE_PROVIDER_ID results")
    parser.add_argument("--test", action="store_true",
                         help="Stop after --test-chunks batches, for fast iteration on real data")
    parser.add_argument("--test-chunks", type=int, default=10,
                         help="With --test: number of batches to read before stopping (default 10, "
                              f"i.e. {10 * DEFAULT_CHUNKSIZE:,} rows at the default chunksize)")
    args = parser.parse_args()

    columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    kanta_input = args.kanta_input or resolve_kanta_input()

    if args.provider_orgs is not None and not args.provider_orgs.exists():
        raise SystemExit(f"provider orgs file not found at {args.provider_orgs}")
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
    # iter_batches silently skips unknown columns, which would only surface later as a
    # KeyError mid-scan -- check up front (e.g. engine-internal names vs. release names)
    missing = [c for c in needed_cols if c not in pf.schema_arrow.names]
    if missing:
        raise SystemExit(f"columns not in {kanta_input.name}: {missing}\n"
                         f"available: {pf.schema_arrow.names}")
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
    provider_orgs = load_provider_orgs(args.provider_orgs) if args.provider_orgs else None

    args.out.mkdir(parents=True, exist_ok=True)
    for c in columns:
        result = summarize_column(counts_in[c], counts_out[c], n_in, n_out, args.min_count)
        if result.empty:
            print(f"{c}: no category value reached --min-count={args.min_count}, skipping")
            continue
        if c == "OMOP_CONCEPT_ID":
            result.insert(1, "OMOP_CONCEPT_NAME", result["VALUE"].map(concept_names))
        if c == "SERVICE_PROVIDER_ID" and provider_orgs is not None:
            # last column on purpose: org names contain spaces, so keeping them at
            # the end leaves every numeric column's position unchanged
            result["BEST_ORG"] = result["VALUE"].map(provider_orgs)
        out_tsv = args.out / f"prevalence_{c.lower()}.tsv"
        # scientific notation for every float column (N_IN/N_OUT/N_TOTAL stay
        # plain integers -- they're counts, not measured quantities); FRAC_IN and
        # the log-scale columns are already human-scale, so plain decimals
        sci_cols = ["PREV_IN", "PREV_OUT", "ODDS_RATIO", "OR_CI_LOW", "OR_CI_HIGH"]
        fixed_cols = ["FRAC_IN", "LOG2_OR", "LOG10_P", "LOG10_P_ADJ_FDR"]
        formatted = result.assign(
            **{col: result[col].map(lambda x: f"{x:.3e}") for col in sci_cols},
            **{col: result[col].map(lambda x: f"{x:.3f}") for col in fixed_cols},
        )
        formatted.to_csv(out_tsv, sep="\t", index=False)
        out_png = args.out / f"prevalence_{c.lower()}_hist.png"
        plot_histogram(result, c, out_png)
        n_sig = int((result["LOG10_P_ADJ_FDR"] < np.log10(0.05)).sum())
        print(f"{c}: {len(result)} values (n_total>={args.min_count}), {n_sig} significant at FDR<0.05 "
              f"-> {out_tsv}, {out_png}")


if __name__ == "__main__":
    main()
