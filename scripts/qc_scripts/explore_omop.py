#!/usr/bin/env python3
"""
explore_omop.py

Consistency report for a single OMOP concept: how does its value distribution look at three
points in the pipeline, for the rows the engine currently maps to it (ground truth —
harmonization_omop::OMOP_ID == OMOP_ID, not an inference)?

  SOURCE       MEASUREMENT_VALUE, grouped by cleaned-pre-inj::MEASUREMENT_UNIT — a snapshot
               before either injection pass ran.
  POST-INJ     MEASUREMENT_VALUE, grouped by MEASUREMENT_UNIT (final, after both injection
               passes — treated as one overarching process here, not split into primary/
               secondary) — the direct input to OMOP mapping/harmonization.
  HARMONIZED   harmonization_omop::MEASUREMENT_VALUE, grouped by
               harmonization_omop::MEASUREMENT_UNIT.

Two rows of the same three stages: the top row overlays one KDE per unit (top --top-n-units by
row count; everything else pooled into "other"), the bottom row the same but grouped by
TEST_NAME_ABBREVIATION instead (top --top-n-tests). The harmonized panels in both rows also
overlay a dimmed "unharmonized" curve -- the post-inj value of rows that had one but failed to
harmonize -- so the harmonized total lines up with SOURCE/post-inj.

Everything needed to redraw the report is cached under --dump-dir, keyed by OMOP_ID + parquet
filename + --top-n-units: the per-unit value arrays feeding each panel (report_data_*.pkl) and
the fitted KDE curves (kde_*.npz). A second run for the same OMOP_ID/parquet/top-n combo skips
the parquet query entirely and goes straight to plotting from cache. The underlying per-
(OMOP_ID, TEST_NAME_ABBREVIATION, stage, unit) value arrays are also cached separately
(dist_*.npz), building up a reusable library of raw distributions across runs.

The full Usagi export (for concept names) is fetched from --usagi-url by default, localized to
--dump-dir, and falls back to whatever's already there if offline (same fetch-with-local-fallback
pattern as reference_data.py's other Usagi tables). The derived OMOP_ID -> concept name lookup is
then built once and cached separately, so repeat runs neither re-download nor re-parse the ~23k
row CSV.

Usage
-----
  python3 explore_omop.py PARQUET OMOP_ID [--out-dir .] [--dump-dir dump_omop]
  python3 explore_omop.py PARQUET OMOP_ID --usagi-url file:///path/to/local/copy.csv

Mismatched-tests check (always runs)
-------------------------------------
After the report is built, for every TEST_NAME_ABBREVIATION mapped to this OMOP_ID with at
least --ks-min-n harmonized values, leave-one-out KS-test its HARM_VALUE distribution against
the pooled rest of the harmonized values. A test whose own values look like they don't belong in
this concept's harmonized pool at all (wrong specimen/scale/quantity, not just an outlier tail)
shows up here. Flags on the KS D statistic (effect size: max gap between the two empirical
CDFs) directly, D >= --ks-d-threshold (default 0.3) -- not the p-value, which saturates near 0
at real row counts (tens of thousands+) even for two tests that are merely comparable rather
than identical (e.g. serum vs. plasma of the same analyte), so it can't tell "comparable" apart
from "wrong specimen/scale entirely" the way D can. Rendered as a table baked into the bottom of
the report PNG itself (flagged rows tinted), and printed as Markdown to stdout at the very end
of the run -- both ordered by entry count (n), most-represented test first. Each row also
carries that test's own min/Q1/median/Q3/max of HARM_VALUE, so a flag comes with the numbers
behind it rather than just a verdict.

Real example, OMOP_ID 3035350 (Ketones [Presence] in Urine by Test strip) -- this is exactly how
u-keto-de was caught as a mismapped test (HUS-only, zero-confidence unreviewed auto-mapping,
values on a completely different scale from the rest of the concept's harmonized pool):

  | TEST_NAME_ABBREVIATION | n       | D (KS) | p-value   | flagged | min | Q1    | median | Q3    | max   |
  |-------------------------|--------:|-------:|----------:|:-------:|----:|------:|-------:|------:|------:|
  | u-keto-o                | 189,897 |  0.008 | 2.120e-01 | no      | 0   | 0     | 0      | 0     | 50    |
  | u-asetoniaineet(kval)   |  16,850 |  0.003 | 9.953e-01 | no      | 0   | 0     | 0      | 0     | 1.02  |
  | asetoniaineet(kval)     |   1,981 |  0.001 | 1.000e+00 | no      | 0   | 0     | 0      | 0     | 3     |
  | u-keto-de               |     153 |  1.000 | 0.000e+00 | yes     | 723 | 1,296 | 1,383  | 1,494 | 2,353 |
  | u-keto                  |      26 |  0.381 | 6.574e-04 | yes     | 0   | 0     | 0      | 1     | 1.03  |
  | u-keto-o.               |       9 |  0.003 | 1.000e+00 | no      | 0   | 0     | 0      | 0     | 0     |

  (flagged when D >= 0.3 across 6 tests compared)
"""

import argparse
import pickle
import sys
import time
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy import stats

_REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO_ROOT / "scripts/injection"))
sys.path.insert(0, str(_REPO_ROOT / "src"))
from kanta.engine.reference_data import _refresh_from_remote  # noqa: E402 -- same fetch-with-fallback pattern as the engine's own Usagi tables

_PLOT_MAX_N = 100_000  # downsample to this many points for KDE fits (own cap -- not injection_engine's 50k)


def _plot_sample(arr, rng):
    if len(arr) > _PLOT_MAX_N:
        return arr[np.sort(rng.choice(len(arr), size=_PLOT_MAX_N, replace=False))]
    return arr

_DEFAULT_USAGI_URL = (
    "https://raw.githubusercontent.com/FINNGEN/kanta_lab_harmonisation_public/"
    "refs/heads/development/VOCABULARIES/LABfi_ALL/LABfi_ALL.usagi.csv"
)
_TOP_N_DEFAULT = 6
_KDE_POINTS = 400
_CLIP_PERCENTILES = (1, 99)  # per-unit outlier clip before KDE fit/plot -- real data has
                                  # extreme mis-entered values (e.g. OMOP 3004410 mmol/mol spans
                                  # -6236..7693) that would otherwise stretch the x-axis until
                                  # the actual clinical-range bulk is an invisible sliver
_UNIT_PALETTE = [
    "#2E7D6B", "#B5652E", "#3B6FB5", "#9A4FB5", "#B5A32E", "#5FA84C",
    "#C0392B", "#7F8C8D",
]
_OTHER_COLOR = "#B0B0B0"
_UNHARMONIZED_COLOR = "#9A9A9A"


# ---------------------------------------------------------------------------
# Concept name lookup
# ---------------------------------------------------------------------------

def load_concept_name_map(dump_dir, usagi_url):
    """OMOP_ID -> concept name, built from the full Usagi export. The CSV itself is localized to
    dump_dir (fetched from usagi_url, falling back to whatever's already there if offline); the
    much smaller derived (OMOP_ID -> name) dict is then cached separately so repeat runs skip
    both the download and the ~23k-row parse."""
    dump_dir = Path(dump_dir)
    dump_dir.mkdir(parents=True, exist_ok=True)
    local_csv = dump_dir / "LABfi_ALL.usagi.csv"
    name_map_cache = dump_dir / "omop_concept_names.pkl"

    if name_map_cache.exists():
        return pickle.loads(name_map_cache.read_bytes())

    print(f"Fetching Usagi export from {usagi_url} -> {local_csv} ...", flush=True)
    t0 = time.time()
    _refresh_from_remote(usagi_url, local_csv)
    if not local_csv.exists():
        print(f"WARNING: could not fetch or find a local copy of the Usagi export at {local_csv} "
              f"-- concept names will be unavailable", file=sys.stderr)
        return {}
    print(f"  done in {time.time() - t0:.1f}s, parsing concept names...", flush=True)

    df = pd.read_csv(local_csv, dtype=str, usecols=["conceptId", "conceptName"])
    df = df.dropna(subset=["conceptId"])
    name_map = df.drop_duplicates("conceptId").set_index("conceptId")["conceptName"].to_dict()
    name_map_cache.write_bytes(pickle.dumps(name_map))
    print(f"  cached {len(name_map):,} concept names -> {name_map_cache}", flush=True)
    return name_map


# ---------------------------------------------------------------------------
# Row query (cached)
# ---------------------------------------------------------------------------

def _duckdb_connect():
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='11GB'")
    con.execute("PRAGMA threads=8")
    con.execute("PRAGMA enable_progress_bar=false")
    return con


def query_omop_rows(parquet, omop_id, dump_dir):
    """All rows the engine currently maps to omop_id (ground truth), with the columns needed for
    all three panels. Not cached -- only the derived plot data (KDE curves) is cached to
    dump_dir; the raw rows are re-queried fresh every run."""
    print(f"Querying OMOP_ID={omop_id} rows from {parquet} "
          f"(full scan, no index -- can take a while on a large file)...", flush=True)
    t0 = time.time()
    con = _duckdb_connect()
    df = con.execute(f"""
        SELECT
            TEST_NAME_ABBREVIATION,
            MEASUREMENT_VALUE,
            MEASUREMENT_UNIT,
            "cleaned-pre-inj::MEASUREMENT_UNIT" AS SOURCE_UNIT,
            "harmonization_omop::MEASUREMENT_VALUE" AS HARM_VALUE,
            "harmonization_omop::MEASUREMENT_UNIT" AS HARM_UNIT
        FROM read_parquet('{parquet}')
        WHERE "harmonization_omop::OMOP_ID" = ?
    """, [str(omop_id)]).df()

    print(f"  query done in {time.time() - t0:.1f}s  ({len(df):,} rows)", flush=True)
    return df


# ---------------------------------------------------------------------------
# Per-stage unit grouping + KDE (cached)
# ---------------------------------------------------------------------------

def top_units_column(df, unit_col, value_col, top_n):
    """Rows with a real value in value_col, plus a PLOT_UNIT column: unit_col's own value if
    it's one of the top_n by row count, else "other". Returns (df_valued, counts) where counts
    is a Series indexed by PLOT_UNIT, sorted descending."""
    valued = df[df[value_col] != "NA"].copy()
    counts_all = valued[unit_col].value_counts()
    keep = set(counts_all.head(top_n).index)
    valued["PLOT_UNIT"] = np.where(valued[unit_col].isin(keep), valued[unit_col], "other")
    counts = valued["PLOT_UNIT"].value_counts()
    # keep insertion order matching magnitude, "other" last regardless of its own size
    order = [u for u in counts_all.head(top_n).index if u in counts.index]
    if "other" in counts.index:
        order.append("other")
    counts = counts.reindex(order)
    return valued, counts


def clip_for_plotting(arr, label=""):
    """Drop values outside _CLIP_PERCENTILES before a value array is handed to KDE fitting --
    real data has extreme mis-entered values (e.g. OMOP 3004410 mmol/mol spans -6236..7693) that
    would otherwise stretch the x-axis until the actual clinical-range bulk is an invisible
    sliver. This is purely a display decision, applied here at the plotting layer -- the cached
    raw distributions (dist_*.npz) keep every value, unclipped."""
    arr = arr[np.isfinite(arr)]
    if len(arr) < 2 or np.std(arr) == 0:
        return arr
    lo, hi = np.percentile(arr, _CLIP_PERCENTILES)
    clipped = arr[(arr >= lo) & (arr <= hi)]
    n_dropped = len(arr) - len(clipped)
    if n_dropped and len(clipped) >= 2 and np.std(clipped) > 0:
        print(f"      [{label}] clipped {n_dropped:,}/{len(arr):,} outliers outside "
              f"[{lo:.3g}, {hi:.3g}] ({_CLIP_PERCENTILES[0]}-{_CLIP_PERCENTILES[1]} pct) "
              f"before fit/plot", flush=True)
        return clipped
    return arr


def _compute_kde(arr, rng):
    if len(arr) < 2 or np.std(arr) == 0:
        return None
    arr = _plot_sample(arr, rng)
    kde = stats.gaussian_kde(arr)
    xs = np.linspace(arr.min(), arr.max(), _KDE_POINTS)
    ys = kde(xs)
    return xs, ys


def get_or_compute_kde(dump_dir, key, arr, rng, label, clip=True):
    cache = Path(dump_dir) / f"kde_{key}_{'clip' if clip else 'noclip'}.npz"
    if cache.exists():
        print(f"    [{label}] N={len(arr):,} -- cached KDE, skipping fit", flush=True)
        data = np.load(cache)
        return data["xs"], data["ys"]
    n_total = len(arr)
    if clip:
        arr = clip_for_plotting(arr, label)
    n_fit = min(len(arr), _PLOT_MAX_N)
    print(f"    [{label}] N={n_total:,} (fitting KDE on {n_fit:,} points)...", end="  ", flush=True)
    t0 = time.time()
    result = _compute_kde(arr, rng)
    print(f"{time.time() - t0:.1f}s", flush=True)
    if result is None:
        return None
    xs, ys = result
    np.savez(cache, xs=xs, ys=ys)
    return xs, ys


def _safe_tag(*parts):
    tag = "_".join(str(p) for p in parts)
    return "".join(c if c.isalnum() or c in "-." else "_" for c in tag)


def cache_distributions(dump_dir, omop_id, stage_name, valued, value_col):
    """Persist the raw (float) value array for every (TEST_NAME_ABBREVIATION, PLOT_UNIT) group
    in this stage, keyed by omop_id + stage + test + unit. Builds up a reusable library of raw
    distributions across runs/OMOP_IDs/tests -- separate from the fitted KDE cache, which only
    stores the pooled-per-unit curve actually plotted."""
    dump_dir = Path(dump_dir)
    written, skipped = 0, 0
    for (test, unit), group in valued.groupby(["TEST_NAME_ABBREVIATION", "PLOT_UNIT"]):
        key = _safe_tag(omop_id, stage_name, test, unit)
        cache = dump_dir / f"dist_{key}.npz"
        if cache.exists():
            skipped += 1
            continue
        arr = _to_float(group[value_col])
        np.savez(cache, values=arr)
        written += 1
    print(f"    distribution cache: {written} written, {skipped} already cached", flush=True)


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _to_float(series):
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)


def build_unit_color_map(stage_counts):
    """One color per unit, shared across all panels -- so e.g. mmol/mol is the same color in
    SOURCE and post-inj (and harmonized, where it overlaps) rather than each panel assigning
    colors independently by its own rank order."""
    totals = {}
    for _, counts in stage_counts:
        for unit, n in counts.items():
            if unit == "other":
                continue
            totals[unit] = totals.get(unit, 0) + n
    ordered = sorted(totals, key=totals.get, reverse=True)
    return {unit: _UNIT_PALETTE[i % len(_UNIT_PALETTE)] for i, unit in enumerate(ordered)}


_LOG_SCALE_RATIO = 10  # switch a panel's x-axis to log scale once its plotted groups' medians
                        # span at least this ratio -- otherwise the smaller-magnitude unit's
                        # bulk is squashed to an invisible sliver next to the larger one


def _group_medians(arrays):
    medians = []
    for arr in arrays:
        arr = arr[np.isfinite(arr)]
        arr = arr[arr > 0]
        if len(arr):
            medians.append(np.median(arr))
    return medians


def should_use_log_scale(*arrays_groups):
    medians = []
    for arrays in arrays_groups:
        medians.extend(_group_medians(arrays))
    if len(medians) < 2:
        return False
    return (max(medians) / min(medians)) >= _LOG_SCALE_RATIO


def plot_stage(ax, dump_dir, omop_id, cache_stage, title, units, counts, unit_colors, rng,
              unharmonized_arr=None, clip=True):
    print(f"  [{cache_stage}]", flush=True)
    curves = []  # (key, xs, ys, color, label, linestyle, line_alpha, fill_alpha)
    for unit in counts.index:
        color = _OTHER_COLOR if unit == "other" else unit_colors[unit]
        arr = units[unit]
        key = _safe_tag(omop_id, cache_stage, "unit", unit)
        result = get_or_compute_kde(dump_dir, key, arr, rng, f"unit={unit}", clip=clip)
        if result is None:
            continue
        xs, ys = result
        label = f"{unit} (N={counts[unit]:,})"
        curves.append((unit, xs, ys, color, label, "-", 0.85, 0.15))

    if unharmonized_arr is not None and len(unharmonized_arr) > 0:
        key = _safe_tag(omop_id, cache_stage, "unharmonized")
        result = get_or_compute_kde(dump_dir, key, unharmonized_arr, rng, "unharmonized", clip=clip)
        if result is not None:
            xs, ys = result
            label = f"unharmonized (N={len(unharmonized_arr):,})"
            curves.append(("__unharmonized__", xs, ys, _UNHARMONIZED_COLOR, label, "--", 0.5, 0.08))

    plotted_arrays = [units[u] for u in counts.index if u in units]
    if unharmonized_arr is not None:
        plotted_arrays.append(unharmonized_arr)
    if should_use_log_scale(plotted_arrays):
        print(f"    x-axis: log scale (plotted groups' medians span >= {_LOG_SCALE_RATIO}x)", flush=True)
        ax.set_xscale("log")

    # peak-normalize each curve to its own max so units/tests with very different sample sizes
    # or concentration are all visible on one shared y-axis -- trades away absolute density
    # (how concentrated one group is vs another) for guaranteed visibility of every curve's shape
    for key, xs, ys, color, label, linestyle, line_alpha, fill_alpha in curves:
        peak = ys.max()
        ys_norm = ys / peak if peak > 0 else ys
        ax.plot(xs, ys_norm, color=color, label=label, alpha=line_alpha, linestyle=linestyle)
        ax.fill_between(xs, ys_norm, color=color, alpha=fill_alpha)

    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.set_xlabel("value")
    ax.set_ylabel("density (normalized to peak)")
    ax.legend(fontsize=7, loc="upper right", framealpha=0.9)


def plot_unit_shift_bars(ax, source_ct, postinj_ct, unit_colors):
    """Paired stacked bars per test: unit-count composition of SOURCE (left bar) vs post-inj
    (right bar), colored to match the by-unit row -- shows how many rows moved between units
    (typically NA -> a real unit) during injection, per test."""
    if source_ct.empty:
        ax.set_title("unit composition: SOURCE -> post-inj", fontsize=12, fontweight="bold")
        ax.text(0.5, 0.5, "no rows with a value", ha="center", va="center", transform=ax.transAxes)
        return

    tests = list(source_ct.index)
    postinj_ct = postinj_ct.reindex(index=tests, fill_value=0)
    all_units = set(source_ct.columns) | set(postinj_ct.columns)
    units_order = [u for u in unit_colors if u in all_units and u != "NA"]
    if "other" in all_units:
        units_order.append("other")
    if "NA" in all_units:
        units_order.append("NA")  # always drawn last -- stacked at the top, never buried at the bottom

    x = np.arange(len(tests))
    width = 0.38
    for ct, xpos, side_label in [(source_ct, x - width / 2 - 0.02, "SOURCE"),
                                  (postinj_ct, x + width / 2 + 0.02, "post-inj")]:
        bottoms = np.zeros(len(tests))
        for unit in units_order:
            if unit not in ct.columns:
                continue
            vals = ct[unit].to_numpy(dtype=float)
            color = _OTHER_COLOR if unit == "other" else unit_colors[unit]
            label = unit if side_label == "SOURCE" else None  # avoid duplicate legend entries
            ax.bar(xpos, vals, width=width, bottom=bottoms, color=color, label=label)
            bottoms += vals

    ax.set_xticks(x)
    ax.set_xticklabels(tests, rotation=30, ha="right", fontsize=7)
    ax.set_title("unit composition: SOURCE (left) -> post-inj (right)", fontsize=12, fontweight="bold")
    ax.set_ylabel("count")
    ax.legend(fontsize=6, loc="upper right", framealpha=0.9)


def build_counts_table(stage_counts):
    lines = []
    for stage_name, counts, unharmonized_n in stage_counts:
        total = counts.sum() + (unharmonized_n or 0)
        lines.append(f"{stage_name}  (total={total:,})")
        for unit, n in counts.items():
            lines.append(f"  {unit:<16} {n:>8,}")
        if unharmonized_n:
            lines.append(f"  {'unharmonized':<16} {unharmonized_n:>8,}")
    return "\n".join(lines)


_QUANTILES = [0, 25, 50, 75, 100]  # min, Q1, median, Q3, max


def ks_test_each_vs_rest(df, value_col, min_n=5):
    """Leave-one-out anomaly check: for every TEST_NAME_ABBREVIATION with >= min_n valid values
    in value_col, KS-test its own value distribution against the pool of every OTHER test's
    values (same value_col, i.e. normally run against HARM_VALUE -- the harmonized pool this
    OMOP_ID's rules are supposed to have made comparable). A test that's actually the wrong
    specimen/scale/quantity silently sharing this OMOP_ID (e.g. u-keto-de under OMOP 3035350)
    shows up as a low p-value here even without knowing what it's "supposed" to look like --
    this is the automated version of the by-hand comparison that caught that case. Also reports
    each test's own _QUANTILES of value_col (min/Q1/median/Q3/max) -- the numbers behind a flag,
    so the flag doesn't have to be taken on faith.
    Returns a DataFrame (TEST_NAME_ABBREVIATION, n, n_rest, ks_stat, p_value, q00, q25, q50, q75,
    q100), sorted by n descending (most-represented test first)."""
    valued = df[df[value_col] != "NA"].copy()
    valued["_v"] = _to_float(valued[value_col])
    valued = valued.dropna(subset=["_v"])
    q_cols = [f"q{q:02d}" for q in _QUANTILES]
    cols = ["TEST_NAME_ABBREVIATION", "n", "n_rest", "ks_stat", "p_value"] + q_cols
    if valued.empty:
        return pd.DataFrame(columns=cols)

    counts = valued["TEST_NAME_ABBREVIATION"].value_counts()
    rows = []
    for test in counts[counts >= min_n].index:
        is_this = valued["TEST_NAME_ABBREVIATION"] == test
        this_arr = valued.loc[is_this, "_v"].to_numpy()
        rest_arr = valued.loc[~is_this, "_v"].to_numpy()
        if len(rest_arr) < 2:
            continue
        stat, p = stats.ks_2samp(this_arr, rest_arr)
        quantiles = np.percentile(this_arr, _QUANTILES)
        rows.append((test, len(this_arr), len(rest_arr), stat, p, *quantiles))
    result = pd.DataFrame(rows, columns=cols)
    return result.sort_values("n", ascending=False).reset_index(drop=True)


def _fmt_num(x):
    """Compact display for a quantile value -- integers print bare, everything else to 3 sig figs."""
    if float(x).is_integer() and abs(x) < 1e6:
        return f"{x:,.0f}"
    return f"{x:,.3g}"


def build_mismatched_table_rows(ks_table, d_threshold=0.3):
    """Shared row-building for the mismatched-tests table (stdout Markdown + PNG). Flags on the
    KS D statistic (max gap between the two empirical CDFs) directly, not the p-value: at real
    row counts (tens of thousands+) the p-value saturates to ~0 for even mild, expected
    differences between comparable tests (e.g. serum vs. plasma of the same analyte), so it
    can't distinguish "comparable but not identical" from "wrong specimen/scale entirely". D is
    the actual effect size and doesn't inflate with N -- D=1.0 means the two distributions don't
    overlap at all (e.g. u-keto-de vs. the rest of OMOP 3035350's pool); D=0.2 means a real but
    modest shift, which two legitimately-related tests can show without being a mapping error.
    Returns (header_cells, [row_cells, ...], d_threshold) already sorted by n descending
    (ks_table is pre-sorted by ks_test_each_vs_rest); empty rows list if ks_table is empty."""
    header = ["TEST_NAME_ABBREVIATION", "n", "D (KS)", "p-value", "flagged",
              "min", "Q1", "median", "Q3", "max"]
    if ks_table.empty:
        return header, [], d_threshold
    rows = []
    for _, row in ks_table.iterrows():
        flagged = "yes" if row["ks_stat"] >= d_threshold else "no"
        rows.append([
            row["TEST_NAME_ABBREVIATION"], f"{row['n']:,}", f"{row['ks_stat']:.3f}",
            f"{row['p_value']:.3e}", flagged,
            _fmt_num(row["q00"]), _fmt_num(row["q25"]), _fmt_num(row["q50"]),
            _fmt_num(row["q75"]), _fmt_num(row["q100"]),
        ])
    return header, rows, d_threshold


def print_mismatched_tests(ks_table, value_col, d_threshold=0.3):
    """Print ks_test_each_vs_rest's result as a Markdown table (rows in ks_table's order, i.e.
    by entry count descending), flagging tests whose KS D statistic (effect size, not p-value --
    see build_mismatched_table_rows) meets d_threshold. Quantile columns (min/Q1/median/Q3/max
    of value_col) are the harmonized-value distribution backing each row's flag."""
    header = (f"Leave-one-out KS test on {value_col}, each TEST_NAME_ABBREVIATION vs. the "
              f"pooled rest of the harmonized pool (rows ordered by entry count)")
    cols, rows, _ = build_mismatched_table_rows(ks_table, d_threshold=d_threshold)
    if not rows:
        print(f"\n{header}: no tests with enough rows to compare", flush=True)
        return
    lines = [
        f"\n{header}",
        f"(flagged when D >= {d_threshold} across {len(rows)} tests compared)",
        "",
        "| " + " | ".join(cols) + " |",
        "|" + "|".join("---:" if i > 0 else "---" for i in range(len(cols))) + "|",
    ]
    for row_cells in rows:
        lines.append("| " + " | ".join(row_cells) + " |")
    print("\n".join(lines), flush=True)


def render_mismatched_table(ax, ks_table, value_col, d_threshold=0.3):
    """Render ks_test_each_vs_rest's result as an actual matplotlib table on ax (rows already
    ordered by entry count -- see build_mismatched_table_rows), flagged rows tinted red."""
    ax.axis("off")
    header, rows, _ = build_mismatched_table_rows(ks_table, d_threshold=d_threshold)
    if not rows:
        ax.text(0.5, 0.5, "mismatched-tests check: no tests with enough rows to compare",
                ha="center", va="center", transform=ax.transAxes, fontsize=10)
        return
    ax.set_title(f"Mismatched-tests check -- leave-one-out KS test on {value_col}, each "
                f"TEST_NAME_ABBREVIATION vs. pooled rest (rows by entry count); "
                f"flagged when D >= {d_threshold} across {len(rows)} tests",
                fontsize=10, loc="left")
    tbl = ax.table(cellText=rows, colLabels=header, loc="center", cellLoc="center")
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(8)
    tbl.auto_set_column_width(col=list(range(len(header))))
    tbl.scale(1, 1.5)
    for i, row_cells in enumerate(rows, start=1):  # +1: row 0 in the table is the header
        if row_cells[4] == "yes":
            for j in range(len(header)):
                tbl[i, j].set_facecolor("#f6cccc")


_REPORT_DATA_VERSION = "v4"  # bump when the cached stage_data tuple shape changes


def _report_data_cache_path(dump_dir, omop_id, parquet, top_n, top_n_tests):
    key = _safe_tag(omop_id, Path(parquet).stem, f"top{top_n}", f"toptests{top_n_tests}",
                     _REPORT_DATA_VERSION)
    return Path(dump_dir) / f"report_data_{key}.pkl"


_KS_FLAGS_VERSION = "v2"  # bump when ks_test_each_vs_rest's returned columns change


def _ks_flags_cache_path(dump_dir, omop_id, parquet, ks_min_n):
    key = _safe_tag(omop_id, Path(parquet).stem, f"ksminn{ks_min_n}", _KS_FLAGS_VERSION)
    return Path(dump_dir) / f"ks_flags_{key}.pkl"


def _group_arrays(df, group_col, value_col, top_n):
    """top_units_column is generic over its grouping column -- reused here for both the by-unit
    row (group_col=a unit column) and the by-test row (group_col=TEST_NAME_ABBREVIATION).
    Returns (group -> float value array, counts)."""
    valued, counts = top_units_column(df, group_col, value_col, top_n)
    groups = {g: _to_float(valued.loc[valued["PLOT_UNIT"] == g, value_col]) for g in counts.index}
    return valued, groups, counts


def test_unit_crosstab(valued_by_unit, top_n_tests):
    """valued_by_unit already has PLOT_UNIT (top-N units, "other") and the raw
    TEST_NAME_ABBREVIATION column. Buckets tests the same way (top-N by row count, else
    "other") and returns a test x unit count table, test rows ordered by count descending
    ("other" last) -- used to compare unit composition per test across stages (e.g. SOURCE vs
    post-inj) with a shared, aligned test axis."""
    if valued_by_unit.empty:
        return pd.DataFrame()
    counts_all = valued_by_unit["TEST_NAME_ABBREVIATION"].value_counts()
    keep = set(counts_all.head(top_n_tests).index)
    test_bucket = np.where(valued_by_unit["TEST_NAME_ABBREVIATION"].isin(keep),
                            valued_by_unit["TEST_NAME_ABBREVIATION"], "other")
    ct = pd.crosstab(test_bucket, valued_by_unit["PLOT_UNIT"])
    order = [t for t in counts_all.head(top_n_tests).index if t in ct.index]
    if "other" in ct.index:
        order.append("other")
    return ct.reindex(order)


def load_or_build_stage_data(parquet, omop_id, dump_dir, top_n, top_n_tests, ks_min_n=5):
    """Returns (n_total_rows, [(stage_name, units, unit_counts, tests, test_counts,
    unharmonized_arr, crosstab), ...], ks_table). units/tests map group label -> float value
    array (by unit, by TEST_NAME_ABBREVIATION, respectively). tests/test_counts are only
    populated for SOURCE and harmonized -- post-inj's would be identical to SOURCE's (both group
    the same MEASUREMENT_VALUE rows by the same TEST_NAME_ABBREVIATION, unaffected by which unit
    column is used), so it's skipped as redundant. crosstab (test x unit count table) is only
    populated for SOURCE and post-inj -- it feeds the middle bottom-row panel, which shows how
    unit composition per test shifts between those two stages instead of duplicating SOURCE's
    by-test distribution. unharmonized_arr is only non-None for the "harmonized" stage: the
    post-inj MEASUREMENT_VALUE of rows that had a post-inj value but failed to harmonize
    (HARM_VALUE == 'NA') -- plotted as a separate dimmed reference curve so the harmonized
    panel's total lines up with SOURCE/post-inj. Cached as a whole (report_data_*.pkl) so a
    repeat run for the same OMOP_ID/parquet/top-n/top-n-tests skips the parquet query entirely.

    ks_table (see ks_test_each_vs_rest) is always computed, against HARM_VALUE, and cached
    separately (ks_flags_*.pkl, keyed on ks_min_n only -- independent of top_n/top_n_tests since
    it runs over every eligible test, not just the top-N plotted ones). If the report_data cache
    already has a hit but the ks cache doesn't, the parquet is queried again just for this."""
    cache = _report_data_cache_path(dump_dir, omop_id, parquet, top_n, top_n_tests)
    stage_data = None
    n_total = None
    if cache.exists():
        print(f"{cache} already exists -- skipping parquet query, loading cached stage data.", flush=True)
        n_total, stage_data = pickle.loads(cache.read_bytes())

    ks_cache = _ks_flags_cache_path(dump_dir, omop_id, parquet, ks_min_n)
    ks_table = None
    if ks_cache.exists():
        print(f"{ks_cache} already exists -- loading cached KS flags.", flush=True)
        ks_table = pickle.loads(ks_cache.read_bytes())

    need_df = stage_data is None or ks_table is None
    if not need_df:
        return n_total, stage_data, ks_table

    df = query_omop_rows(parquet, omop_id, dump_dir)

    if stage_data is None:
        stages = [
            ("SOURCE",     "SOURCE_UNIT",      "MEASUREMENT_VALUE"),
            ("post-inj",   "MEASUREMENT_UNIT", "MEASUREMENT_VALUE"),
            ("harmonized", "HARM_UNIT",        "HARM_VALUE"),
        ]
        stage_data = []
        for stage_name, unit_col, value_col in stages:
            valued, units, unit_counts = _group_arrays(df, unit_col, value_col, top_n)
            if not valued.empty:
                cache_distributions(dump_dir, omop_id, stage_name, valued, value_col)

            tests, test_counts = {}, pd.Series(dtype=int)
            if stage_name != "post-inj":
                _, tests, test_counts = _group_arrays(df, "TEST_NAME_ABBREVIATION", value_col, top_n_tests)

            crosstab = None
            if stage_name in ("SOURCE", "post-inj"):
                crosstab = test_unit_crosstab(valued, top_n_tests)

            unharmonized_arr = None
            if stage_name == "harmonized":
                unharmonized = df[(df["HARM_VALUE"] == "NA") & (df["MEASUREMENT_VALUE"] != "NA")]
                unharmonized_arr = _to_float(unharmonized["MEASUREMENT_VALUE"])
                print(f"    unharmonized (post-inj value, no harmonized value): "
                      f"N={len(unharmonized_arr):,}", flush=True)

            stage_data.append((stage_name, units, unit_counts, tests, test_counts, unharmonized_arr, crosstab))

        n_total = len(df)
        cache.write_bytes(pickle.dumps((n_total, stage_data)))

    if ks_table is None:
        ks_table = ks_test_each_vs_rest(df, "HARM_VALUE", min_n=ks_min_n)
        ks_cache.write_bytes(pickle.dumps(ks_table))

    return n_total, stage_data, ks_table


def make_report(parquet, omop_id, out_dir, dump_dir, top_n, top_n_tests,
                usagi_url=_DEFAULT_USAGI_URL, seed=0, clip=True,
                ks_min_n=5, ks_d_threshold=0.3):
    Path(dump_dir).mkdir(parents=True, exist_ok=True)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)

    concept_name = load_concept_name_map(dump_dir, usagi_url).get(str(omop_id))
    n_total, stage_data, ks_table = load_or_build_stage_data(
        parquet, omop_id, dump_dir, top_n, top_n_tests, ks_min_n=ks_min_n)
    print(f"OMOP_ID={omop_id}  ({concept_name or 'name not found in Usagi export'})")
    print(f"  {n_total:,} rows currently mapped to this concept")

    unit_colors = build_unit_color_map([(s, uc) for s, _, uc, _, _, _, _ in stage_data])
    test_colors = build_unit_color_map([(s, tc) for s, _, _, _, tc, _, _ in stage_data])
    by_stage = {s: (units, unit_counts, tests, test_counts, unharmonized_arr, crosstab)
                for s, units, unit_counts, tests, test_counts, unharmonized_arr, crosstab in stage_data}

    # reserve a dedicated right-hand margin for the counts table so it never overlaps a panel's
    # own legend (top-right corner of a panel is exactly where matplotlib puts legends), and a
    # third row spanning the full width for the mismatched-tests table -- sized by row count so
    # it doesn't get squashed when a lot of tests clear --ks-min-n.
    n_ks_rows = max(len(ks_table), 1)
    fig = plt.figure(figsize=(22, 11 + 0.35 * n_ks_rows), constrained_layout=True)
    gs = fig.add_gridspec(3, 4, width_ratios=[1, 1, 1, 0.38],
                          height_ratios=[1, 1, 0.15 + 0.09 * n_ks_rows])
    unit_axes = [fig.add_subplot(gs[0, i]) for i in range(3)]
    test_axes = [fig.add_subplot(gs[1, i]) for i in range(3)]
    table_ax = fig.add_subplot(gs[:2, 3])
    table_ax.axis("off")
    ks_ax = fig.add_subplot(gs[2, :])

    stage_counts = []
    for ax, (stage_name, units, unit_counts, tests, test_counts, unharmonized_arr, crosstab) in zip(unit_axes, stage_data):
        unharmonized_n = len(unharmonized_arr) if unharmonized_arr is not None else 0
        stage_counts.append((stage_name, unit_counts, unharmonized_n))
        if not units and not unharmonized_n:
            ax.set_title(f"{stage_name} (by unit)", fontsize=12, fontweight="bold")
            ax.text(0.5, 0.5, "no rows with a value", ha="center", va="center", transform=ax.transAxes)
            continue
        plot_stage(ax, dump_dir, omop_id, stage_name, f"{stage_name} (by unit)", units, unit_counts,
                  unit_colors, rng, unharmonized_arr=unharmonized_arr, clip=clip)

    # bottom row: SOURCE-by-test, unit-composition shift (SOURCE vs post-inj), harmonized-by-test.
    # post-inj-by-test is skipped -- it's identical to SOURCE-by-test (same MEASUREMENT_VALUE
    # rows grouped by the same TEST_NAME_ABBREVIATION regardless of unit), so the middle slot
    # shows something new instead: how unit composition per test shifted during injection.
    source_tests, source_test_counts = by_stage["SOURCE"][2], by_stage["SOURCE"][3]
    harm_tests, harm_test_counts, harm_unharm = by_stage["harmonized"][2], by_stage["harmonized"][3], by_stage["harmonized"][4]
    source_crosstab, postinj_crosstab = by_stage["SOURCE"][5], by_stage["post-inj"][5]

    ax = test_axes[0]
    if not source_tests:
        ax.set_title("SOURCE (by test)", fontsize=12, fontweight="bold")
        ax.text(0.5, 0.5, "no rows with a value", ha="center", va="center", transform=ax.transAxes)
    else:
        plot_stage(ax, dump_dir, omop_id, "SOURCE-by-test", "SOURCE (by test)", source_tests,
                  source_test_counts, test_colors, rng, clip=clip)

    plot_unit_shift_bars(test_axes[1], source_crosstab, postinj_crosstab, unit_colors)

    ax = test_axes[2]
    harm_unharm_n = len(harm_unharm) if harm_unharm is not None else 0
    if not harm_tests and not harm_unharm_n:
        ax.set_title("harmonized (by test)", fontsize=12, fontweight="bold")
        ax.text(0.5, 0.5, "no rows with a value", ha="center", va="center", transform=ax.transAxes)
    else:
        plot_stage(ax, dump_dir, omop_id, "harmonized-by-test", "harmonized (by test)", harm_tests,
                  harm_test_counts, test_colors, rng, unharmonized_arr=harm_unharm, clip=clip)

    title = f"OMOP_ID {omop_id}"
    if concept_name:
        title += f" — {concept_name}"
    if clip:
        title += (f"\nper-group outliers outside the {_CLIP_PERCENTILES[0]}-{_CLIP_PERCENTILES[1]} "
                  f"percentile range are excluded from these plots (KDE fit + display)")
    fig.suptitle(title, fontsize=14, fontweight="bold")

    table_text = build_counts_table(stage_counts)
    table_ax.text(0.02, 0.98, table_text, ha="left", va="top", fontsize=8,
                 family="monospace", transform=table_ax.transAxes,
                 bbox=dict(boxstyle="round", facecolor="white", edgecolor="0.7", alpha=0.9))

    render_mismatched_table(ks_ax, ks_table, "HARM_VALUE", d_threshold=ks_d_threshold)

    out_path = Path(out_dir) / f"omop_{omop_id}_report.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\nWrote {out_path}")

    print_mismatched_tests(ks_table, "HARM_VALUE", d_threshold=ks_d_threshold)

    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="Two-row, three-panel consistency report (SOURCE / post-injection / "
                    "harmonized, by unit and by test) for one OMOP concept's currently-mapped rows.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("parquet", help="Engine output parquet (e.g. kanta_dev_YYYY_MM_DD.parquet)")
    p.add_argument("omop_id", help="OMOP_ID to report on")
    p.add_argument("--out-dir", default=".", metavar="PATH", help="Where to save the report PNG")
    p.add_argument("--dump-dir", default="dump_omop", metavar="PATH",
                   help="Cache directory for the per-stage plot data (skips the parquet query "
                        "on repeat runs), computed KDE curves, and the localized Usagi export "
                        "+ derived concept-name lookup")
    p.add_argument("--usagi-url", default=_DEFAULT_USAGI_URL, metavar="URL",
                   help="Full Usagi export to localize for concept names (http(s):// or "
                        "file:// for a local copy); falls back to whatever's already cached "
                        "in --dump-dir if unreachable")
    p.add_argument("--top-n-units", type=int, default=_TOP_N_DEFAULT, metavar="INT",
                   help="Max distinct units plotted per panel before bucketing the rest as 'other'")
    p.add_argument("--top-n-tests", type=int, default=_TOP_N_DEFAULT, metavar="INT",
                   help="Max distinct TEST_NAME_ABBREVIATIONs plotted per bottom-row panel "
                        "before bucketing the rest as 'other'")
    p.add_argument("--no-clip", action="store_true",
                   help=f"Disable per-group outlier clipping (default: clip to the "
                        f"{_CLIP_PERCENTILES[0]}-{_CLIP_PERCENTILES[1]} percentile range before "
                        f"fitting/plotting). Without clipping, extreme mis-entered values can "
                        f"squash the real distribution to an invisible sliver.")
    p.add_argument("--ks-min-n", type=int, default=5, metavar="INT",
                   help="Minimum harmonized-value rows a TEST_NAME_ABBREVIATION needs to be "
                        "included in the mismatched-tests leave-one-out KS check (always run, "
                        "against HARM_VALUE, independent of --top-n-tests -- flags a test whose "
                        "values look like they don't belong in this concept's harmonized pool "
                        "at all, e.g. wrong specimen/scale/quantity). Rendered into the report "
                        "PNG and printed to stdout at the end of the run.")
    p.add_argument("--ks-d-threshold", type=float, default=0.3, metavar="FLOAT",
                   help="Flag a test in the mismatched-tests check when its KS D statistic "
                        "(max gap between its own CDF and the pooled rest's) is >= this value. "
                        "Effect-size threshold, not a p-value: at real row counts the p-value "
                        "saturates near 0 even for two comparable-but-not-identical tests (e.g. "
                        "serum vs. plasma of the same analyte), so D is what actually "
                        "distinguishes 'comparable' from 'wrong specimen/scale entirely'.")
    return p


def main():
    args = build_parser().parse_args()
    make_report(args.parquet, args.omop_id, args.out_dir, args.dump_dir,
               args.top_n_units, args.top_n_tests, usagi_url=args.usagi_url, clip=not args.no_clip,
               ks_min_n=args.ks_min_n, ks_d_threshold=args.ks_d_threshold)


if __name__ == "__main__":
    main()
