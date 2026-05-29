#!/usr/bin/env python3
"""
split_eval.py

For a single TEST_NAME, evaluate whether splitting the candidate distribution
by a GMM separator yields a better unit fit than treating it as a whole.

Metric: size-weighted average KS statistic
  global_score = best (lowest) KS over qualifying units for c_all
  split_score  = (n_low  * best_KS(c_low,  units)
               +  n_high * best_KS(c_high, units)) / n_total
  improvement  = (global_score − split_score) / global_score

Usage
-----
  python3 split_eval.py TEST_NAME --units "mmol/l:85.2,µmol/l:12.1" [options]

  Assumes candidate and target .npy arrays already exist in --dump-dir
  (produced by explore_test_name.py).
"""

import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats

import injection_engine


def _unit_tag(unit):
    return unit.replace("/", "_").replace(" ", "_").replace("%", "pct")


def _name_tag(name):
    return name.replace("/", "_").replace(" ", "_")


def _parse_units_arg(units_str):
    """Parse 'unit1:pct1,unit2:pct2,...' → list of (unit, pct).  pct defaults to 0.0."""
    pairs = []
    for item in units_str.split(","):
        item = item.strip().strip("{}")
        if not item:
            continue
        if ":" in item:
            u, p = item.rsplit(":", 1)
            pairs.append((u.strip(), float(p)))
        else:
            pairs.append((item.strip(), 0.0))
    return pairs


def load_candidate(name, dump_dir):
    tag  = _name_tag(name)
    path = os.path.join(dump_dir, f"cand_{tag}.npy")
    arr  = np.load(path)
    print(f"  candidate: N={len(arr):,}  ({path})")
    return arr


def load_target(name, unit, dump_dir):
    tag  = _name_tag(name)
    path = os.path.join(dump_dir, f"targ_{tag}_{_unit_tag(unit)}.npy")
    return np.load(path)


# ---------------------------------------------------------------------------
# KS helper (stat only — no p-value needed for scoring)
# ---------------------------------------------------------------------------

def _ks_stat(a, b):
    n1, n2 = len(a), len(b)
    if max(n1, n2) > 500_000:
        lo, hi = min(a.min(), b.min()), max(a.max(), b.max())
        if lo == hi:
            return 0.0
        bins = 100_000
        h1, _ = np.histogram(a, bins=bins, range=(lo, hi))
        h2, _ = np.histogram(b, bins=bins, range=(lo, hi))
        e1 = np.cumsum(h1) / n1
        e2 = np.cumsum(h2) / n2
        return float(np.max(np.abs(e1 - e2)))
    return float(stats.ks_2samp(a, b).statistic)


# ---------------------------------------------------------------------------
# Comparison logic
# ---------------------------------------------------------------------------

def rank_units(c, unit_data):
    """
    For a candidate array c, compute KS against every unit.
    Returns list of (unit, pct, ks_stat) sorted by ks_stat ascending.
    """
    results = []
    for unit, (t_vals, pct) in unit_data.items():
        results.append((unit, pct, _ks_stat(c, t_vals)))
    results.sort(key=lambda x: x[2])
    return results


def split_score(n_low, ks_low, n_high, ks_high):
    """Size-weighted average KS across sub-distributions."""
    return (n_low * ks_low + n_high * ks_high) / (n_low + n_high)


# ---------------------------------------------------------------------------
# Full pipeline for display
# ---------------------------------------------------------------------------

def run_full_pipeline(c, t, label):
    """Run KS+T+MAD and return a one-line summary string + PipelineResult."""
    result = injection_engine.run_pipeline(c, t)
    ks = next(s for s in result.steps if s.name == "KS")
    t_ = next(s for s in result.steps if s.name == "T")
    m  = next(s for s in result.steps if s.name == "MAD")
    summary = (
        f"{label:<40}  "
        f"KS={'P' if ks.passed else 'F'}({ks.details['stat']:.4f})  "
        f"T={'P' if t_.passed else 'F'}  "
        f"MAD={'P' if m.passed else 'F'}  "
        f"→ {result.outcome} at {result.decided_by}"
    )
    return summary, result


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

_PLOT_MAX_N = 50_000


def _sample(arr):
    if len(arr) > _PLOT_MAX_N:
        rng = np.random.default_rng(42)
        return arr[np.sort(rng.choice(len(arr), _PLOT_MAX_N, replace=False))]
    return arr


def _ecdf(arr):
    x = np.sort(arr)
    return x, np.arange(1, len(x) + 1) / len(x)


def _kde(ax, arr, label, color, lw=1.6, fill=True, ls="-"):
    arr = _sample(arr)
    if len(arr) < 2 or np.std(arr) == 0:
        return
    kde = stats.gaussian_kde(arr)
    xs  = np.linspace(arr.min(), arr.max(), 400)
    ys  = kde(xs)
    ax.plot(xs, ys, color=color, label=label, lw=lw, ls=ls, alpha=0.9)
    if fill:
        ax.fill_between(xs, ys, color=color, alpha=0.12)


def _ecdf_plot(ax, arr, label, color):
    x, y = _ecdf(_sample(arr))
    ax.step(x, y, where="post", color=color, label=label, alpha=0.85)


def _ks_marker(ax, c, t, ks_stat):
    cs = _sample(c)
    ts = _sample(t)
    cx, _ = _ecdf(cs)
    tx, _ = _ecdf(ts)
    all_x = np.sort(np.unique(np.concatenate([cx, tx])))
    c_cdf = np.searchsorted(cx, all_x, side="right") / len(cx)
    t_cdf = np.searchsorted(tx, all_x, side="right") / len(tx)
    i     = np.argmax(np.abs(c_cdf - t_cdf))
    y_lo, y_hi = sorted([c_cdf[i], t_cdf[i]])
    ax.plot([all_x[i], all_x[i]], [y_lo, y_hi], color="red", lw=2.5,
            label=f"D={ks_stat:.4f}")


_COLORS = ["steelblue", "darkorange", "seagreen", "mediumpurple", "saddlebrown"]


def make_figure(name, c_vals, unit_data,
                global_ranks, low_ranks, high_ranks,
                c_low, c_high, sep, bim,
                g_score, s_score, improvement,
                out_path):
    """
    2 rows × 3 cols:
      Row 0 — Global:  KDE (all units) | ECDF (best unit) | GMM fit
      Row 1 — Split:   KDE low vs best | KDE high vs best | Score summary
    """
    unit_colors = {u: _COLORS[i % len(_COLORS)] for i, u in enumerate(unit_data)}

    best_g_unit, _, best_g_ks = global_ranks[0]
    best_l_unit, _, best_l_ks = low_ranks[0]  if low_ranks  else (None, 0, np.nan)
    best_h_unit, _, best_h_ks = high_ranks[0] if high_ranks else (None, 0, np.nan)

    t_global = unit_data[best_g_unit][0]
    t_low    = unit_data[best_l_unit][0] if best_l_unit else np.array([])
    t_high   = unit_data[best_h_unit][0] if best_h_unit else np.array([])

    verdict      = "SPLIT BETTER" if improvement > 0 else "GLOBAL BETTER"
    same_unit    = best_l_unit == best_h_unit

    fig, axes = plt.subplots(2, 3, figsize=(21, 10))
    fig.suptitle(
        f"{name}"
        f"   global KS={g_score:.4f}  split KS={s_score:.4f}"
        f"  improvement={improvement:+.1%}"
        f"   →  {verdict}"
        + ("  [same unit → likely intrinsic]" if same_unit else ""),
        fontsize=12, fontweight="bold",
    )

    # ── (0,0)  Global KDE: candidate + all unit targets ──────────────────
    ax = axes[0, 0]
    _kde(ax, c_vals, "candidate (all)", "black", lw=2.2)
    for unit, (t_vals, pct) in unit_data.items():
        is_best = (unit == best_g_unit)
        _kde(ax, t_vals,
             f"{unit} ({pct:.1f}%)",
             unit_colors[unit],
             lw=2.2 if is_best else 1.2,
             ls="-"  if is_best else "--",
             fill=is_best)
    ax.set_title("Global: candidate vs all units (KDE)")
    ax.set_xlabel("value"); ax.legend(fontsize=8)

    # ── (0,1)  Global ECDF: candidate vs best unit ────────────────────────
    ax = axes[0, 1]
    _ecdf_plot(ax, c_vals, "candidate (all)", "black")
    _ecdf_plot(ax, t_global, f"{best_g_unit} (global best)", "steelblue")
    _ks_marker(ax, c_vals, t_global, best_g_ks)
    ax.set_title(f"Global ECDF  best={best_g_unit}  KS={best_g_ks:.4f}")
    ax.set_xlabel("value"); ax.set_ylabel("CDF"); ax.legend(fontsize=8)

    # ── (0,2)  GMM bimodal plot ────────────────────────────────────────────
    ax = axes[0, 2]
    if bim.fit:
        fit    = bim.fit
        x_fit  = fit["x_fit"]
        lo_f, hi_f = np.percentile(x_fit, [0.5, 99.5])
        ax.hist(x_fit, bins=60, density=True, alpha=0.4, color="grey",
                range=(lo_f, hi_f), label="candidate")
        xf = np.linspace(lo_f, hi_f, 400)
        for m, s, w in zip(fit["means_native"], fit["sigmas_native"], fit["weights"]):
            ax.plot(xf, w * stats.norm.pdf(xf, m, s), lw=1.8, alpha=0.85)
        if not np.isnan(fit.get("sep_native", np.nan)):
            ax.axvline(fit["sep_native"], color="red", ls="--",
                       label=f"sep={sep:.4g}")
        ax.set_xlabel("log(value)" if bim.lognormal else "value")
    ax.set_title(
        f"GMM  status={bim.status}"
        f"  BC={bim.bc:.3f}  dip_p={bim.dip_p:.3g}"
        f"  n_low={len(c_low):,}  n_high={len(c_high):,}"
    )
    ax.legend(fontsize=8)

    # ── (1,0)  Split KDE: low sub vs its best unit ────────────────────────
    ax = axes[1, 0]
    if len(c_low) >= 2:
        _kde(ax, c_low, f"low (N={len(c_low):,})", "steelblue")
    if len(t_low) >= 2:
        _kde(ax, t_low, f"{best_l_unit} (best for low)", "darkorange", fill=False, ls="--")
    ax.set_title(f"Split LOW  best={best_l_unit}  KS={best_l_ks:.4f}")
    ax.set_xlabel("value"); ax.legend(fontsize=8)

    # ── (1,1)  Split KDE: high sub vs its best unit ───────────────────────
    ax = axes[1, 1]
    if len(c_high) >= 2:
        _kde(ax, c_high, f"high (N={len(c_high):,})", "seagreen")
    if len(t_high) >= 2:
        _kde(ax, t_high, f"{best_h_unit} (best for high)", "darkorange", fill=False, ls="--")
    ax.set_title(f"Split HIGH  best={best_h_unit}  KS={best_h_ks:.4f}")
    ax.set_xlabel("value"); ax.legend(fontsize=8)

    # ── (1,2)  Score summary text panel ──────────────────────────────────
    ax = axes[1, 2]
    ax.axis("off")
    n_total = len(c_vals)
    n_low   = len(c_low)
    n_high  = len(c_high)

    lines = [
        "── SCORE SUMMARY ──────────────────────",
        "",
        f"GLOBAL  KS = {g_score:.4f}",
    ]
    for u, p, k in global_ranks:
        marker = " ←" if u == best_g_unit else ""
        lines.append(f"  {u:<18}  KS={k:.4f}{marker}")

    lines += [
        "",
        f"SPLIT   weighted KS = {s_score:.4f}",
        f"  low  N={n_low:,} ({100*n_low/n_total:.1f}%)",
    ]
    for u, p, k in low_ranks:
        marker = " ←" if u == best_l_unit else ""
        lines.append(f"    {u:<16}  KS={k:.4f}{marker}")
    lines.append(f"  high N={n_high:,} ({100*n_high/n_total:.1f}%)")
    for u, p, k in high_ranks:
        marker = " ←" if u == best_h_unit else ""
        lines.append(f"    {u:<16}  KS={k:.4f}{marker}")

    lines += [
        "",
        f"improvement  = {improvement:+.1%}",
        f"same unit?   = {same_unit}",
        f"  low  → {best_l_unit}",
        f"  high → {best_h_unit}",
        "",
        f"VERDICT: {verdict}",
    ]

    ax.text(0.04, 0.97, "\n".join(lines),
            transform=ax.transAxes, fontsize=9.5, va="top", family="monospace",
            bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.85))

    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"  figure → {out_path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Evaluate global vs split unit fit for a single TEST_NAME.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("test_name",  help="TEST_NAME to evaluate")
    p.add_argument("--units",    required=True,
                   help="Comma-separated units to evaluate, optionally with pct: "
                        "'mmol/l:85.2,µmol/l:12.1'")
    p.add_argument("--dump-dir", default="/mnt/disks/data/kanta/inject/tmp/",
                   help="Cache directory containing existing .npy arrays")
    p.add_argument("--min-target-n", type=int,   default=30,
                   help="Minimum records for a unit target to qualify")
    p.add_argument("--dip-threshold",type=float, default=0.05,
                   help="Hartigan dip test p-value threshold for bimodality check")
    p.add_argument("--out-dir",      default=".",
                   help="Directory for output figure")
    args = p.parse_args()

    name = args.test_name

    SEP = "=" * 72
    print(f"\n{SEP}")
    print(f"SPLIT EVALUATION:  {name}")
    print(SEP)

    # ── 1. Candidate values ───────────────────────────────────────────────
    print("\n[1] Candidate values")
    c_vals = load_candidate(name, args.dump_dir)
    if len(c_vals) < 2:
        sys.exit("ERROR: too few candidate values")
    print(f"  N={len(c_vals):,}  "
          f"median={np.median(c_vals):.4g}  "
          f"p5={np.percentile(c_vals,5):.4g}  "
          f"p95={np.percentile(c_vals,95):.4g}")

    # ── 2. Units (passed in) ──────────────────────────────────────────────
    print(f"\n[2] Units")
    top_units = _parse_units_arg(args.units)
    if not top_units:
        sys.exit("ERROR: --units produced no entries")
    for u, pct in top_units:
        print(f"  {u:<22}  {pct:>6.2f}%")

    # ── 3. Target arrays ──────────────────────────────────────────────────
    print(f"\n[3] Target arrays  (min_target_n={args.min_target_n})")
    unit_data = {}   # unit → (t_vals, pct)
    for unit, pct in top_units:
        t_vals = load_target(name, unit, args.dump_dir)
        status = f"N={len(t_vals):,}"
        if len(t_vals) < args.min_target_n:
            status += "  SKIP (too few)"
        print(f"  {unit:<22}  {status}")
        if len(t_vals) >= args.min_target_n:
            unit_data[unit] = (t_vals, pct)
    if not unit_data:
        sys.exit("ERROR: no qualifying target distributions")

    # ── 4. Global comparison ──────────────────────────────────────────────
    print("\n[4] Global comparison  KS(c_all, unit)")
    global_ranks = rank_units(c_vals, unit_data)
    for unit, pct, ks in global_ranks:
        tag_mark = "  ← best" if unit == global_ranks[0][0] else ""
        print(f"  {unit:<22}  KS={ks:.4f}{tag_mark}")
    g_score = global_ranks[0][2]

    print("\n  Full pipeline (global best unit):")
    best_g_unit = global_ranks[0][0]
    t_best_g    = unit_data[best_g_unit][0]
    summary, _  = run_full_pipeline(c_vals, t_best_g, f"c_all vs {best_g_unit}")
    print(f"  {summary}")

    # ── 5. Bimodal check + split ──────────────────────────────────────────
    print(f"\n[5] Bimodal check  (dip_threshold={args.dip_threshold})")
    bim = injection_engine.bimodal_check(c_vals, dip_threshold=args.dip_threshold)
    print(f"  status={bim.status}  sep={bim.separator:.4g}"
          f"  BC={bim.bc:.3f}  dip_p={bim.dip_p:.3g}"
          f"  space={'log' if bim.lognormal else 'linear'}")

    sep   = bim.separator
    c_low  = c_vals[c_vals <= sep] if not np.isnan(sep) else np.array([])
    c_high = c_vals[c_vals >  sep] if not np.isnan(sep) else np.array([])
    n_low, n_high = len(c_low), len(c_high)
    print(f"  low:  N={n_low:,}  ({100*n_low/len(c_vals):.1f}%)"
          f"   high: N={n_high:,}  ({100*n_high/len(c_vals):.1f}%)")
    if n_low < 2 or n_high < 2:
        print("  WARNING: one sub-distribution is too small — split metrics may be unreliable")

    # ── 6. Split comparison ───────────────────────────────────────────────
    print("\n[6] Split comparison  KS(c_sub, unit)")
    low_ranks  = rank_units(c_low,  unit_data) if n_low  >= 2 else []
    high_ranks = rank_units(c_high, unit_data) if n_high >= 2 else []

    print("  LOW sub-distribution:")
    for unit, pct, ks in low_ranks:
        tag_mark = "  ← best" if unit == low_ranks[0][0] else ""
        print(f"    {unit:<22}  KS={ks:.4f}{tag_mark}")

    print("  HIGH sub-distribution:")
    for unit, pct, ks in high_ranks:
        tag_mark = "  ← best" if unit == high_ranks[0][0] else ""
        print(f"    {unit:<22}  KS={ks:.4f}{tag_mark}")

    print("\n  Full pipeline (best unit per sub):")
    if low_ranks:
        best_l_unit = low_ranks[0][0]
        s, _ = run_full_pipeline(c_low, unit_data[best_l_unit][0], f"c_low  vs {best_l_unit}")
        print(f"  {s}")
    if high_ranks:
        best_h_unit = high_ranks[0][0]
        s, _ = run_full_pipeline(c_high, unit_data[best_h_unit][0], f"c_high vs {best_h_unit}")
        print(f"  {s}")

    # ── 7. Aggregated score ───────────────────────────────────────────────
    print(f"\n[7] Score aggregation")
    if low_ranks and high_ranks:
        best_l_ks   = low_ranks[0][2]
        best_h_ks   = high_ranks[0][2]
        s_score     = split_score(n_low, best_l_ks, n_high, best_h_ks)
        improvement = (g_score - s_score) / g_score if g_score > 0 else 0.0
    else:
        s_score     = np.nan
        improvement = 0.0

    best_l_unit  = low_ranks[0][0]  if low_ranks  else "—"
    best_h_unit  = high_ranks[0][0] if high_ranks else "—"
    same_unit    = best_l_unit == best_h_unit

    print(f"  global_score = {g_score:.4f}  (best unit: {global_ranks[0][0]})")
    print(f"  split_score  = {s_score:.4f}  (size-weighted avg of per-sub bests)")
    print(f"  improvement  = {improvement:+.1%}")
    print(f"  same best unit for both subs? {same_unit}"
          f"  (low={best_l_unit}, high={best_h_unit})")

    verdict = "SPLIT BETTER" if improvement > 0 else "GLOBAL BETTER"
    print(f"\n{SEP}")
    print(f"VERDICT:  {verdict}  (improvement={improvement:+.1%})")
    if same_unit:
        print("  NOTE: both sub-distributions prefer the same unit"
              " — likely intrinsic bimodality, not a unit mix")
    print(SEP)

    # ── 8. Figure ─────────────────────────────────────────────────────────
    out_path = os.path.join(args.out_dir, f"split_eval_{_name_tag(name)}.png")
    print(f"\n[8] Generating figure")
    make_figure(
        name=name, c_vals=c_vals, unit_data=unit_data,
        global_ranks=global_ranks,
        low_ranks=low_ranks, high_ranks=high_ranks,
        c_low=c_low, c_high=c_high, sep=sep, bim=bim,
        g_score=g_score, s_score=s_score, improvement=improvement,
        out_path=out_path,
    )


if __name__ == "__main__":
    main()
