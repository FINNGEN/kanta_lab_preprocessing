#!/usr/bin/env python3
"""
explore_omop.py

Consistency report for a single OMOP concept: how does its value distribution look at three
points in the pipeline, for the rows the engine currently maps to it (ground truth —
harmonization_omop::OMOP_ID == OMOP_ID, not an inference)?

  PRE-INJ      MEASUREMENT_VALUE, grouped by cleaned-pre-inj::MEASUREMENT_UNIT — a snapshot
               before either injection pass ran.
  POST-INJ     MEASUREMENT_VALUE, grouped by MEASUREMENT_UNIT (final, after both injection
               passes — treated as one overarching process here, not split into primary/
               secondary) — the direct input to OMOP mapping/harmonization.
  HARMONIZED   harmonization_omop::MEASUREMENT_VALUE, grouped by
               harmonization_omop::MEASUREMENT_UNIT.

Each panel overlays one KDE per unit (top --top-n-units by row count; everything else pooled
into "other"), plus two dashed reference lines pooling *all* units in that panel together, split
by IS_VALUE_EXTRACTED — a sanity check that free-text-extracted values aren't systematically
different from structured ones.

Fitting KDEs is expensive, so the plot data (curves only, not the raw rows) is cached under
--dump-dir, keyed by OMOP_ID + parquet filename + stage/unit/extracted-split — a second run
for the same OMOP_ID against the same parquet skips re-fitting. The row query itself always
re-runs against the parquet (rows aren't cached).

The full Usagi export (for concept names) is fetched from --usagi-url by default, localized to
--dump-dir, and falls back to whatever's already there if offline (same fetch-with-local-fallback
pattern as reference_data.py's other Usagi tables). The derived OMOP_ID -> concept name lookup is
then built once and cached separately, so repeat runs neither re-download nor re-parse the ~23k
row CSV.

Usage
-----
  python3 explore_omop.py PARQUET OMOP_ID [--out-dir .] [--dump-dir dump_omop]
  python3 explore_omop.py PARQUET OMOP_ID --usagi-url file:///path/to/local/copy.csv
"""

import argparse
import pickle
import subprocess
import sys
import time
from pathlib import Path

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
_UNIT_PALETTE = [
    "#2E7D6B", "#B5652E", "#3B6FB5", "#9A4FB5", "#B5A32E", "#5FA84C",
    "#C0392B", "#7F8C8D",
]
_OTHER_COLOR = "#B0B0B0"
_EXTRACTED_COLOR = "#C0392B"
_NOT_EXTRACTED_COLOR = "#1A2420"


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

def clickhouse(query, **params):
    cmd = ["clickhouse", "-q", query]
    for k, v in params.items():
        cmd.append(f"--param_{k}={v}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise RuntimeError("ClickHouse query failed")
    return proc.stdout


def query_omop_rows(parquet, omop_id, dump_dir):
    """All rows the engine currently maps to omop_id (ground truth), with the columns needed for
    all three panels. Not cached -- only the derived plot data (KDE curves) is cached to
    dump_dir; the raw rows are re-queried fresh every run."""
    print(f"Querying OMOP_ID={omop_id} rows from {parquet} "
          f"(full scan, no index -- can take a while on a large file)...", flush=True)
    t0 = time.time()
    out = clickhouse(f"""
        SELECT
            MEASUREMENT_VALUE,
            MEASUREMENT_UNIT,
            `cleaned-pre-inj::MEASUREMENT_UNIT` AS PRE_INJ_UNIT,
            `harmonization_omop::MEASUREMENT_VALUE` AS HARM_VALUE,
            `harmonization_omop::MEASUREMENT_UNIT` AS HARM_UNIT,
            IS_VALUE_EXTRACTED
        FROM file('{parquet}')
        WHERE `harmonization_omop::OMOP_ID` = {{omop_id:String}}
        FORMAT TSVWithNames
    """, omop_id=str(omop_id))

    from io import StringIO
    if not out.strip():
        df = pd.DataFrame(columns=["MEASUREMENT_VALUE", "MEASUREMENT_UNIT", "PRE_INJ_UNIT",
                                    "HARM_VALUE", "HARM_UNIT", "IS_VALUE_EXTRACTED"])
    else:
        df = pd.read_csv(StringIO(out), sep="\t", dtype=str, keep_default_na=False, na_values=[""])

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


def _compute_kde(arr, rng):
    arr = arr[np.isfinite(arr)]
    if len(arr) < 2 or np.std(arr) == 0:
        return None
    arr = _plot_sample(arr, rng)
    kde = stats.gaussian_kde(arr)
    xs = np.linspace(arr.min(), arr.max(), _KDE_POINTS)
    ys = kde(xs)
    return xs, ys


def get_or_compute_kde(dump_dir, key, arr, rng, label):
    cache = Path(dump_dir) / f"kde_{key}.npz"
    if cache.exists():
        print(f"    [{label}] N={len(arr):,} -- cached KDE, skipping fit", flush=True)
        data = np.load(cache)
        return data["xs"], data["ys"]
    n_fit = min(len(arr), _PLOT_MAX_N)
    print(f"    [{label}] N={len(arr):,} (fitting KDE on {n_fit:,} points)...", end="  ", flush=True)
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


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def _to_float(series):
    return pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)


def plot_stage(ax, dump_dir, omop_id, stage_name, valued, counts, value_col, rng):
    print(f"  [{stage_name}]", flush=True)
    for i, unit in enumerate(counts.index):
        color = _OTHER_COLOR if unit == "other" else _UNIT_PALETTE[i % len(_UNIT_PALETTE)]
        arr = _to_float(valued.loc[valued["PLOT_UNIT"] == unit, value_col])
        key = _safe_tag(omop_id, stage_name, "unit", unit)
        result = get_or_compute_kde(dump_dir, key, arr, rng, f"unit={unit}")
        if result is None:
            continue
        xs, ys = result
        label = f"{unit} (N={counts[unit]:,})"
        ax.plot(xs, ys, color=color, label=label, alpha=0.85)
        ax.fill_between(xs, ys, color=color, alpha=0.15)

    # dashed overlay: pooled extracted vs non-extracted, regardless of unit
    for is_extracted, label, color in [("1", "extracted", _EXTRACTED_COLOR),
                                       ("0", "structured", _NOT_EXTRACTED_COLOR)]:
        sub = valued[valued["IS_VALUE_EXTRACTED"] == is_extracted]
        arr = _to_float(sub[value_col])
        key = _safe_tag(omop_id, stage_name, "extracted", is_extracted)
        result = get_or_compute_kde(dump_dir, key, arr, rng, label)
        if result is None:
            continue
        xs, ys = result
        ax.plot(xs, ys, color=color, linestyle="--", linewidth=1.4,
                label=f"{label} (N={len(sub):,})", alpha=0.9)

    ax.set_title(stage_name, fontsize=12, fontweight="bold")
    ax.legend(fontsize=7, loc="upper right", framealpha=0.9)
    ax.set_xlabel("value")
    ax.set_ylabel("density")


def build_counts_table(stage_counts):
    lines = []
    for stage_name, counts in stage_counts:
        lines.append(f"{stage_name}")
        for unit, n in counts.items():
            lines.append(f"  {unit:<16} {n:>8,}")
    return "\n".join(lines)


def make_report(parquet, omop_id, out_dir, dump_dir, top_n, usagi_url=_DEFAULT_USAGI_URL, seed=0):
    Path(dump_dir).mkdir(parents=True, exist_ok=True)
    Path(out_dir).mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)

    concept_name = load_concept_name_map(dump_dir, usagi_url).get(str(omop_id))
    df = query_omop_rows(parquet, omop_id, dump_dir)
    print(f"OMOP_ID={omop_id}  ({concept_name or 'name not found in Usagi export'})")
    print(f"  {len(df):,} rows currently mapped to this concept")

    stages = [
        ("pre-inj",    "PRE_INJ_UNIT", "MEASUREMENT_VALUE"),
        ("post-inj",   "MEASUREMENT_UNIT", "MEASUREMENT_VALUE"),
        ("harmonized", "HARM_UNIT", "HARM_VALUE"),
    ]

    # reserve a dedicated right-hand margin for the counts table so it never overlaps a panel's
    # own legend (top-right corner of the third panel is exactly where matplotlib puts legends)
    fig = plt.figure(figsize=(22, 5.5))
    gs = fig.add_gridspec(1, 4, width_ratios=[1, 1, 1, 0.38], wspace=0.28)
    axes = [fig.add_subplot(gs[0, i]) for i in range(3)]
    table_ax = fig.add_subplot(gs[0, 3])
    table_ax.axis("off")

    stage_counts = []
    for ax, (stage_name, unit_col, value_col) in zip(axes, stages):
        valued, counts = top_units_column(df, unit_col, value_col, top_n)
        stage_counts.append((stage_name, counts))
        if valued.empty:
            ax.set_title(stage_name, fontsize=12, fontweight="bold")
            ax.text(0.5, 0.5, "no rows with a value", ha="center", va="center", transform=ax.transAxes)
            continue
        plot_stage(ax, dump_dir, omop_id, stage_name, valued, counts, value_col, rng)

    title = f"OMOP_ID {omop_id}"
    if concept_name:
        title += f" — {concept_name}"
    fig.suptitle(title, fontsize=14, fontweight="bold", y=1.02)

    table_text = build_counts_table(stage_counts)
    table_ax.text(0.02, 0.98, table_text, ha="left", va="top", fontsize=8,
                 family="monospace", transform=table_ax.transAxes,
                 bbox=dict(boxstyle="round", facecolor="white", edgecolor="0.7", alpha=0.9))

    fig.tight_layout()
    out_path = Path(out_dir) / f"omop_{omop_id}_report.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"\nWrote {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        description="Three-panel consistency report (pre-injection / post-injection / "
                    "harmonized) for one OMOP concept's currently-mapped rows.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("parquet", help="Engine output parquet (e.g. kanta_dev_YYYY_MM_DD.parquet)")
    p.add_argument("omop_id", help="OMOP_ID to report on")
    p.add_argument("--out-dir", default=".", metavar="PATH", help="Where to save the report PNG")
    p.add_argument("--dump-dir", default="dump_omop", metavar="PATH",
                   help="Cache directory for the row query, computed KDE curves, and the "
                        "localized Usagi export + derived concept-name lookup")
    p.add_argument("--usagi-url", default=_DEFAULT_USAGI_URL, metavar="URL",
                   help="Full Usagi export to localize for concept names (http(s):// or "
                        "file:// for a local copy); falls back to whatever's already cached "
                        "in --dump-dir if unreachable")
    p.add_argument("--top-n-units", type=int, default=_TOP_N_DEFAULT, metavar="INT",
                   help="Max distinct units plotted per panel before bucketing the rest as 'other'")
    return p


def main():
    args = build_parser().parse_args()
    make_report(args.parquet, args.omop_id, args.out_dir, args.dump_dir,
               args.top_n_units, usagi_url=args.usagi_url)


if __name__ == "__main__":
    main()
