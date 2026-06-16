#!/usr/bin/env python3
"""
injection_engine.py

Tests whether a candidate value distribution is compatible with a target
(reference) distribution for unit injection.

Pipeline
--------
1. KS test  : stat < ks_threshold AND p < sig_level  → PASS
              (small effect size + sufficient data to trust the assessment)
2. Welch t  : runs when KS fails; p ≥ sig_level (not significantly different) → PASS
3. MAD      : last resort — |median(candidate) − median(target)| ≤ n_mad × MAD(target) → PASS
                                                                                          → FAIL

All three tests always run (for reporting); decision order is KS → T → MAD.

Usage
-----
  injection_engine.py CANDIDATE TARGET [options]
  injection_engine.py --test-mode [options]
"""

import argparse
import os
import sys
from dataclasses import dataclass, field

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from scipy.special import kolmogorov as _kolmogorov

try:
    from sklearn.mixture import GaussianMixture as _GaussianMixture
    _GMM_AVAILABLE = True
except ImportError:
    _GMM_AVAILABLE = False


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

def _fmt(v):
    if not isinstance(v, float):
        return str(v)
    if v == 0.0 or abs(v) < 1e-3 or abs(v) >= 1e5:
        return f"{v:.4e}"
    return f"{v:.4f}"


@dataclass
class StepResult:
    name: str
    passed: bool
    details: dict = field(default_factory=dict)  # raw values (floats/ints)

    def __str__(self):
        kv = "  ".join(f"{k}={_fmt(v)}" for k, v in self.details.items())
        return f"[{self.name:<4}] {'PASS' if self.passed else 'FAIL'}  {kv}"


@dataclass
class PipelineResult:
    outcome: str       # "PASS" or "FAIL"
    decided_by: str    # "KS", "T", or "MAD"
    steps: list = field(default_factory=list)

    def __str__(self):
        lines = [f"Outcome : {self.outcome}  (decided by {self.decided_by})"]
        for s in self.steps:
            lines.append(f"  {s}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Individual tests
# ---------------------------------------------------------------------------

_BINNED_KS_N = 500_000  # switch to binned approximation above this size


def _binned_ks(data1, data2, bins=100_000):
    """
    Fast approximate two-sample KS test via histograms.

    Stat approximation error ≤ 1/bins ≈ 1e-5 (negligible vs threshold 0.3).
    P-value uses the Hodges-corrected asymptotic Kolmogorov distribution,
    identical to scipy ks_2samp(method='asymp').
    """
    n1, n2 = len(data1), len(data2)
    lo = min(data1.min(), data2.min())
    hi = max(data1.max(), data2.max())

    if lo == hi:  # all values identical — distributions are the same
        return 0.0, 1.0

    hist1, _ = np.histogram(data1, bins=bins, range=(lo, hi))
    hist2, _ = np.histogram(data2, bins=bins, range=(lo, hi))

    ecdf1 = np.cumsum(hist1) / n1
    ecdf2 = np.cumsum(hist2) / n2

    stat = float(np.max(np.abs(ecdf1 - ecdf2)))

    # Hodges (1958) correction — matches scipy's asymptotic formula exactly
    en = np.sqrt(n1 * n2 / (n1 + n2))
    pval = float(np.clip(_kolmogorov((en + 0.12 + 0.11 / en) * stat), 0.0, 1.0))

    return stat, pval


def _ks(candidate, target, ks_threshold, sig_level):
    if max(len(candidate), len(target)) > _BINNED_KS_N:
        stat, pval = _binned_ks(candidate, target)
        method = "binned"
    else:
        stat, pval = stats.ks_2samp(candidate, target)
        method = "exact"
    passed = (stat < ks_threshold) and (pval < sig_level)
    return StepResult("KS", passed, {
        "stat":         float(stat),
        "ks_threshold": ks_threshold,
        "pval":         float(pval),
        "sig_level":    sig_level,
        "method":       method,
    })


def _t(candidate, target, sig_level):
    stat, pval = stats.ttest_ind(candidate, target, equal_var=False)
    passed = pval >= sig_level  # PASS = means NOT significantly different → proceed to MAD
    return StepResult("T", passed, {
        "stat":      float(stat),
        "pval":      float(pval),
        "sig_level": sig_level,
    })


def _mad(candidate, target, n_mad):
    t_median  = np.median(target)
    t_mad     = float(stats.median_abs_deviation(target))
    c_median  = np.median(candidate)
    distance  = abs(float(c_median) - float(t_median))
    threshold = n_mad * t_mad
    return StepResult("MAD", distance <= threshold, {
        "cand_median":   float(c_median),
        "target_median": float(t_median),
        "MAD":           t_mad,
        "distance":      distance,
        "threshold":     threshold,
        "n_mad":         n_mad,
    })


# ---------------------------------------------------------------------------
# Bimodal check
# ---------------------------------------------------------------------------

@dataclass
class BimodalResult:
    status: str       # "unimodal" | "bimodal" | "bimodal_cautious"
    separator: float  # split point in original space (nan if unimodal)
    bc: float
    dip_p: float
    lognormal: bool   # which space had lower BIC
    fit: dict         # raw GMM output for the winning fit (empty dict if unavailable)
    fit_alt: dict     # raw GMM output for the losing fit (empty dict if unavailable)
    overlap_pct: float = np.nan  # GMM overlap coefficient as % (0=perfectly separated)

    def __str__(self):
        ovl = f"  overlap={self.overlap_pct:.1f}%" if not np.isnan(self.overlap_pct) else ""
        return (f"[BIMODAL] {self.status}  sep={self.separator:.4g}"
                f"  BC={self.bc:.3f}  dip_p={self.dip_p:.3g}"
                f"  space={'log' if self.lognormal else 'linear'}{ovl}")


def _bimodality_coefficient(arr):
    n = len(arr)
    g = float(stats.skew(arr))
    k = float(stats.kurtosis(arr, fisher=True))
    if n > 3:
        return (g**2 + 1) / (k + 3.0 * (n - 1)**2 / ((n - 2) * (n - 3)))
    return np.nan


def _gmm_fit(arr, lognormal):
    if not _GMM_AVAILABLE:
        return None
    if lognormal:
        pos = arr[arr > 0]
        if len(pos) < 10:
            return None
        x = np.log10(pos)
    else:
        x = arr
        pos = arr
    try:
        gmm = _GaussianMixture(n_components=2, random_state=0, max_iter=300)
        gmm.fit(x.reshape(-1, 1))
    except Exception:
        return None
    means   = gmm.means_.flatten()
    sigmas  = np.sqrt(gmm.covariances_).flatten()
    weights = gmm.weights_

    # Separator: grid search between the two means + linear interpolation.
    # fsolve is avoided because it can diverge badly with a poor starting point.
    lo_m, hi_m = float(np.min(means)), float(np.max(means))
    span  = max(hi_m - lo_m, 1e-10)
    grid  = np.linspace(lo_m - 0.1 * span, hi_m + 0.1 * span, 2000)
    diff  = (weights[0] * stats.norm.pdf(grid, means[0], sigmas[0])
           - weights[1] * stats.norm.pdf(grid, means[1], sigmas[1]))
    inner = (grid >= lo_m) & (grid <= hi_m)
    g_in, d_in = grid[inner], diff[inner]
    cx = np.where(np.diff(np.sign(d_in)))[0]
    if len(cx) > 0:
        i = cx[0]
        x0, x1, d0, d1 = g_in[i], g_in[i + 1], d_in[i], d_in[i + 1]
        sep_native = float(x0 - d0 * (x1 - x0) / (d1 - d0))
    else:
        sep_native = float(np.mean(means))
    # Overlap coefficient: ∫ min(w1·f1, w2·f2) dx on a fine grid covering ±4σ from each mean
    lo_grid = min(means[0] - 4*sigmas[0], means[1] - 4*sigmas[1])
    hi_grid = max(means[0] + 4*sigmas[0], means[1] + 4*sigmas[1])
    g = np.linspace(lo_grid, hi_grid, 2000)
    overlap_coef = float(np.trapz(
        np.minimum(weights[0] * stats.norm.pdf(g, means[0], sigmas[0]),
                   weights[1] * stats.norm.pdf(g, means[1], sigmas[1])),
        g,
    ))

    return dict(
        separator=float(10**sep_native) if lognormal else float(sep_native),
        sep_native=sep_native,
        means_native=means, sigmas_native=sigmas, weights=weights,
        bic=float(gmm.bic(x.reshape(-1, 1))),
        bc=float(_bimodality_coefficient(x)),
        overlap_pct=float(overlap_coef * 100),
        lognormal=lognormal,
        x_fit=x, x_orig=pos,
    )


_BIMODAL_MAX_N = 50_000  # dip test is unreliable above 72k; GMM also faster


def bimodal_check(arr, dip_threshold=0.05, bc_threshold=0.555):
    """
    Classify a distribution as unimodal / bimodal / bimodal_cautious.

    Decision logic:
      - dip p >= dip_threshold           → unimodal
      - dip p <  dip_threshold, BC >= bc_threshold  → bimodal
      - dip p <  dip_threshold, BC <  bc_threshold  → bimodal_cautious (overlap)
      - dip unavailable: fall back to BC alone
    """
    arr = np.asarray(arr, dtype=float)
    arr = arr[np.isfinite(arr)]

    if len(arr) > _BIMODAL_MAX_N:
        rng = np.random.default_rng(42)
        arr = arr[rng.choice(len(arr), size=_BIMODAL_MAX_N, replace=False)]

    fit_norm = _gmm_fit(arr, lognormal=False)
    fit_log  = _gmm_fit(arr, lognormal=True)
    fits     = [f for f in (fit_norm, fit_log) if f is not None]

    if not fits:
        return BimodalResult("unimodal", np.nan, np.nan, np.nan, False, {}, {})

    best = min(fits, key=lambda f: f["bic"])

    # Dip test in the same space as the best GMM fit (log or linear)
    dip_p = np.nan
    try:
        from diptest import diptest
        _, dip_p = diptest(best["x_fit"])
        dip_p = float(dip_p)
    except ImportError:
        pass
    bc   = best["bc"]
    sep  = best["separator"]

    if not np.isnan(dip_p):
        if dip_p >= dip_threshold:
            status = "unimodal"
        else:
            status = "bimodal" if bc >= bc_threshold else "bimodal_cautious"
    else:
        status = "bimodal" if bc >= bc_threshold else "unimodal"

    alt = next((f for f in fits if f is not best), {})
    return BimodalResult(status, sep, bc, dip_p, best["lognormal"], best, alt,
                         overlap_pct=best.get("overlap_pct", np.nan))


def compute_bimodal_plot_data(result):
    """Extract compact data for the bimodal diagnostic plot."""
    if not result.fit:
        return None

    def _fit_data(fit):
        if not fit:
            return None
        x   = fit["x_fit"]
        lo, hi = float(np.percentile(x, 0.5)), float(np.percentile(x, 99.5))
        xs  = np.linspace(lo, hi, _KDE_PTS)
        curves = []
        for m, s, w in zip(fit["means_native"], fit["sigmas_native"], fit["weights"]):
            from scipy import stats as _stats
            curves.append({"y": (w * _stats.norm.pdf(xs, m, s)).tolist(),
                           "mean": float(m), "sigma": float(s), "weight": float(w)})
        # histogram
        counts, edges = np.histogram(x, bins=40, range=(lo, hi), density=True)
        return {
            "lognormal":  fit["lognormal"],
            "bic":        float(fit["bic"]),
            "sep_native": float(fit["sep_native"]) if not np.isnan(fit["sep_native"]) else None,
            "x":          xs.tolist(),
            "hist_counts": counts.tolist(),
            "hist_edges":  edges.tolist(),
            "curves":     curves,
        }

    return {
        "status":    result.status,
        "separator": float(result.separator) if not np.isnan(result.separator) else None,
        "bc":        float(result.bc),
        "dip_p":     float(result.dip_p) if not np.isnan(result.dip_p) else None,
        "winner":    "log" if result.lognormal else "linear",
        "linear":    _fit_data(result.fit     if not result.fit.get("lognormal") else result.fit_alt),
        "log":       _fit_data(result.fit     if     result.fit.get("lognormal") else result.fit_alt),
    }


def _plot_bimodal_panel(ax, fit, is_winner, result_separator):
    """Draw one GMM panel (linear or log space) onto ax."""
    if not fit:
        ax.text(0.5, 0.5, "fit unavailable", ha="center", va="center",
                transform=ax.transAxes, color="grey")
        return

    x_fit  = fit["x_fit"]
    x_orig = fit["x_orig"]
    means  = fit["means_native"]
    sigmas = fit["sigmas_native"]
    weights = fit["weights"]
    lognormal = fit["lognormal"]

    lo, hi = np.percentile(x_fit, [0.5, 99.5])
    ax.hist(x_fit, bins=60, density=True, alpha=0.45, color="steelblue", range=(lo, hi))
    xf = np.linspace(lo, hi, 400)
    for m, s, w in zip(means, sigmas, weights):
        ax.plot(xf, w * stats.norm.pdf(xf, m, s), alpha=0.8)
    if not np.isnan(fit["sep_native"]):
        ax.axvline(fit["sep_native"], color="red", ls="--",
                   label=f"sep={fit['sep_native']:.3g}")

    space = "log" if lognormal else "linear"
    ax.set_xlabel("log₁₀(value)" if lognormal else "value")
    title = f"{space} space   BIC={fit['bic']:.1f}"
    ax.set_title(title, fontweight="bold" if is_winner else "normal",
                 color="black" if is_winner else "grey")
    ax.legend(fontsize=8)

    if is_winner:
        ax.spines[["top", "right", "bottom", "left"]].set_linewidth(2.5)
        for spine in ax.spines.values():
            spine.set_edgecolor("#2ca02c")
        ax.text(0.98, 0.97, "✓ winner", ha="right", va="top",
                transform=ax.transAxes, fontsize=9, color="#2ca02c", fontweight="bold")


def plot_bimodal_check(result, name, dump_dir):
    """
    Save bimodal diagnostic to dump_dir/bimodal_{tag}.png.
    Always shows both linear and log panels; winner is highlighted in green.
    No-op if GMM failed entirely.
    """
    if not result.fit:
        return
    tag      = name.replace("/", "_").replace(" ", "_")
    out_path = os.path.join(dump_dir, f"bimodal_{tag}.png")
    if os.path.exists(out_path):
        return

    fit_win = result.fit
    fit_alt = result.fit_alt

    # Assign panels: left=linear, right=log
    fit_linear = fit_win if not fit_win["lognormal"] else fit_alt
    fit_log    = fit_win if     fit_win["lognormal"] else fit_alt

    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    fig.suptitle(
        f"{name}  —  {result.status}  sep={result.separator:.4g}"
        f"  BC={result.bc:.3f}  dip_p={result.dip_p:.3g}"
        f"  winner={'log' if result.lognormal else 'linear'}",
        fontsize=10, fontweight="bold",
    )

    _plot_bimodal_panel(axes[0], fit_linear, is_winner=not result.lognormal,
                        result_separator=result.separator)
    _plot_bimodal_panel(axes[1], fit_log,    is_winner=result.lognormal,
                        result_separator=result.separator)

    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# Split improvement scoring
# ---------------------------------------------------------------------------

def _ks_stat_only(a, b):
    """KS statistic as a float (no p-value)."""
    if max(len(a), len(b)) > _BINNED_KS_N:
        return _binned_ks(a, b)[0]
    return float(stats.ks_2samp(a, b).statistic)


def rank_units_by_ks(c, unit_vals):
    """
    c         : candidate array
    unit_vals : {unit: t_vals}
    Returns list of (unit, ks_stat) sorted ascending by ks_stat.
    """
    return sorted(
        ((unit, _ks_stat_only(c, t)) for unit, t in unit_vals.items()),
        key=lambda x: x[1],
    )


def split_improvement(c_vals, c_low, c_high, unit_vals):
    """
    Compare size-weighted split KS against the global best KS.

    unit_vals : {unit: t_vals}

    Returns dict:
      global_score, global_ranks, low_ranks, high_ranks,
      split_score, improvement  (positive = split is better),
      same_best_unit            (True suggests intrinsic bimodality)
    """
    global_ranks = rank_units_by_ks(c_vals, unit_vals)
    global_score = global_ranks[0][1] if global_ranks else np.nan

    low_ranks  = rank_units_by_ks(c_low,  unit_vals) if len(c_low)  >= 2 else []
    high_ranks = rank_units_by_ks(c_high, unit_vals) if len(c_high) >= 2 else []

    if not low_ranks or not high_ranks or np.isnan(global_score):
        return dict(
            global_score=global_score, global_ranks=global_ranks,
            low_ranks=low_ranks, high_ranks=high_ranks,
            split_score=np.nan, improvement=0.0, same_best_unit=True,
        )

    n_total = len(c_low) + len(c_high)
    s_score = (len(c_low) * low_ranks[0][1] + len(c_high) * high_ranks[0][1]) / n_total
    impr    = (global_score - s_score) / global_score if global_score > 0 else 0.0

    return dict(
        global_score=global_score, global_ranks=global_ranks,
        low_ranks=low_ranks, high_ranks=high_ranks,
        split_score=s_score, improvement=impr,
        same_best_unit=low_ranks[0][0] == high_ranks[0][0],
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_pipeline(candidate, target, ks_threshold=0.3, sig_level=0.05, n_mad=3.0):
    """
    Run the injection compatibility pipeline.
    Returns a PipelineResult with outcome ("PASS"/"FAIL") and per-step details.
    details dicts contain raw floats, suitable for direct programmatic access.
    """
    ks  = _ks(candidate, target, ks_threshold, sig_level)
    t   = _t(candidate, target, sig_level)
    mad = _mad(candidate, target, n_mad)
    steps = [ks, t, mad]

    if ks.passed:
        decided_by, outcome = "KS", "PASS"
    elif t.passed:
        decided_by, outcome = "T", "PASS"
    else:
        decided_by = "MAD"
        outcome    = "PASS" if mad.passed else "FAIL"

    return PipelineResult(outcome, decided_by, steps)


# ---------------------------------------------------------------------------
# Test generation
# ---------------------------------------------------------------------------

def _generate_test_cases(n=1000, seed=42):
    """
    Synthetic cases designed to exercise each outcome in the pipeline.

    Expected results with default thresholds (ks=0.3, sig=0.05, n_mad=3):

      pass_ks   → PASS via KS  : N(0.3,1) vs N(0,1) — small stat (~0.12), p << 0.05
      fail_all  → FAIL via MAD : N(5,1) vs N(0,1)   — different means, median also far
      pass_T    → PASS via T   : N(0,0.1) vs N(0,1) — different shape, same mean (T gates before MAD)
      pass_T2   → PASS via T   : lognormal(0,σ=2) shifted to mean=0 vs N(0,1)
                                 mean=0 by construction so T passes; MAD would fail (median≈−6.4)
    """
    rng    = np.random.default_rng(seed)
    target = rng.normal(0.0, 1.0, n)

    c_pass_ks  = rng.normal(0.3, 1.0, n)
    c_fail_t   = rng.normal(5.0, 1.0, n)
    c_pass_mad = rng.normal(0.0, 0.1, n)

    # lognormal(μ=0, σ=2): E[X] = e^2 ≈ 7.39, median = 1
    # subtract mean → mean=0, median = 1 − e^2 ≈ −6.39
    c_fail_mad = rng.lognormal(mean=0.0, sigma=2.0, size=n) - np.exp(2.0)

    return [
        ("pass_ks  [expect: PASS via KS ]", c_pass_ks,  target),
        ("fail_all [expect: FAIL via MAD]", c_fail_t,   target),
        ("pass_T   [expect: PASS via T  ]", c_pass_mad, target),
        ("pass_T2  [expect: PASS via T  ]", c_fail_mad, target),
    ]


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------

def _load_values(path):
    """Load a 1-D float array from a .npy file or a text/TSV file."""
    if path.endswith(".npy"):
        return np.load(path)
    values = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                values.append(float(line.split("\t")[0]))
    return np.array(values)


# ---------------------------------------------------------------------------
# Diagnostics plot
# ---------------------------------------------------------------------------

_KDE_PTS  = 80   # points per KDE curve for HTML rendering
_ECDF_PTS = 100  # points sampled from each ECDF for HTML rendering


def compute_plot_data(candidate, target, result, prevalence=None):
    """
    Extract the minimal numeric data needed to reproduce the 3-panel diagnostic
    plot in a browser.  Returns a plain dict suitable for JSON serialisation.
    """
    rng    = np.random.default_rng(42)
    c_plot = _plot_sample(candidate, rng)
    t_plot = _plot_sample(target,    rng)

    ks_step  = next((s for s in result.steps if s.name == "KS"),  None)
    t_step   = next((s for s in result.steps if s.name == "T"),   None)
    mad_step = next((s for s in result.steps if s.name == "MAD"), None)

    # --- Panel 1: ECDF (subsample to _ECDF_PTS evenly-spaced quantiles) ---
    def _ecdf_compact(arr):
        x, y = _ecdf(arr)
        idx  = np.unique(np.linspace(0, len(x) - 1, _ECDF_PTS, dtype=int))
        return x[idx].tolist(), y[idx].tolist()

    cx, cy = _ecdf_compact(c_plot)
    tx, ty = _ecdf_compact(t_plot)

    ks_marker = None
    if ks_step:
        all_x   = np.sort(np.unique(np.concatenate([np.array(cx), np.array(tx)])))
        c_full  = np.searchsorted(np.array(cx), all_x, side="right") / len(cx)
        t_full  = np.searchsorted(np.array(tx), all_x, side="right") / len(tx)
        i_max   = int(np.argmax(np.abs(c_full - t_full)))
        ks_marker = {"x": float(all_x[i_max]),
                     "y_lo": float(min(c_full[i_max], t_full[i_max])),
                     "y_hi": float(max(c_full[i_max], t_full[i_max]))}

    # --- Panel 2: KDE linear ---
    def _kde_compact(arr, n=_KDE_PTS):
        if len(arr) < 2 or np.std(arr) == 0:
            return [], []
        kde = stats.gaussian_kde(arr)
        xs  = np.linspace(arr.min(), arr.max(), n)
        return xs.tolist(), kde(xs).tolist()

    c_kde_x, c_kde_y = _kde_compact(c_plot)
    t_kde_x, t_kde_y = _kde_compact(t_plot)

    # --- Panel 3: KDE log ---
    c_pos = c_plot[c_plot > 0]
    t_pos = t_plot[t_plot > 0]
    c_log_x, c_log_y = _kde_compact(np.log10(c_pos)) if len(c_pos) > 1 else ([], [])
    t_log_x, t_log_y = _kde_compact(np.log10(t_pos)) if len(t_pos) > 1 else ([], [])

    mad_info = None
    if mad_step:
        d = mad_step.details
        mad_info = {
            "c_median":  float(d["cand_median"]),
            "t_median":  float(d["target_median"]),
            "threshold": float(d["threshold"]),
            "distance":  float(d["distance"]),
            "MAD":       float(d["MAD"]),
            "n_mad":     float(d["n_mad"]),
            "passed":    mad_step.passed,
        }

    return {
        "outcome":    result.outcome,
        "decided_by": result.decided_by,
        "prevalence": str(prevalence) if prevalence else None,
        "n_candidate": len(candidate),
        "n_target":    len(target),
        "ecdf": {
            "c_x": cx, "c_y": cy,
            "t_x": tx, "t_y": ty,
            "ks_marker": ks_marker,
            "ks": {"stat": float(ks_step.details["stat"]),
                   "mlogp": float(-np.log10(np.clip(ks_step.details["pval"], 1e-300, 1.0))),
                   "passed": ks_step.passed} if ks_step else None,
        },
        "kde_linear": {
            "c_x": c_kde_x, "c_y": c_kde_y,
            "t_x": t_kde_x, "t_y": t_kde_y,
            "c_mean": float(np.mean(c_plot)),
            "t_mean": float(np.mean(t_plot)),
            "t": {"stat": float(t_step.details["stat"]),
                  "mlogp": float(-np.log10(np.clip(t_step.details["pval"], 1e-300, 1.0))),
                  "passed": t_step.passed} if t_step else None,
        },
        "kde_log": {
            "c_x": c_log_x, "c_y": c_log_y,
            "t_x": t_log_x, "t_y": t_log_y,
            "mad": mad_info,
        },
    }

def _ecdf(arr):
    x = np.sort(arr)
    y = np.arange(1, len(x) + 1) / len(x)
    return x, y


def _kde(ax, arr, label, color):
    if len(arr) < 2 or np.std(arr) == 0:
        return
    kde = stats.gaussian_kde(arr)
    xs  = np.linspace(arr.min(), arr.max(), 400)
    ys  = kde(xs)
    ax.plot(xs, ys, color=color, label=label, alpha=0.85)
    ax.fill_between(xs, ys, color=color, alpha=0.15)


_PLOT_MAX_N = 50_000  # downsample to this many points for KDE/ECDF plots only


def _plot_sample(arr, rng):
    if len(arr) > _PLOT_MAX_N:
        return arr[np.sort(rng.choice(len(arr), size=_PLOT_MAX_N, replace=False))]
    return arr


def plot_result(candidate, target, result, name, dump_dir, prevalence=None,
               tag_override=None):
    """
    Save a 3-panel diagnostic figure to dump_dir/plot_<tag>.png.
    Skips if the file already exists.

    Panels
    ------
    1. Empirical CDFs with KS distance marked
    2. KDE in linear scale  + t-test statistics
    3. KDE in log scale     + MAD statistics
    """
    if tag_override:
        tag = tag_override
    else:
        tag = name.replace("/", "_").replace(" ", "_")
    out_path = os.path.join(dump_dir, f"plot_{tag}.png")
    if os.path.exists(out_path):
        return

    rng       = np.random.default_rng(42)
    c_plot    = _plot_sample(candidate, rng)
    t_plot    = _plot_sample(target,    rng)
    sampled   = len(candidate) > _PLOT_MAX_N or len(target) > _PLOT_MAX_N

    ks_step  = next((s for s in result.steps if s.name == "KS"),  None)
    t_step   = next((s for s in result.steps if s.name == "T"),   None)
    mad_step = next((s for s in result.steps if s.name == "MAD"), None)

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    sample_note = f"  [plot: {_PLOT_MAX_N:,} sampled]" if sampled else ""
    prev_note   = f"\n{prevalence}" if prevalence and str(prevalence) not in ("NA", "nan") else ""
    fig.suptitle(f"{name}  —  {result.outcome} at {result.decided_by}{sample_note}{prev_note}",
                 fontsize=11, fontweight="bold")

    _TXT = dict(transform=None, va="top", fontsize=9,
                bbox=dict(boxstyle="round", facecolor="lightyellow", alpha=0.8))
    _TXT_GREY = dict(transform=None, va="top", fontsize=9,
                     bbox=dict(boxstyle="round", facecolor="lightgrey", alpha=0.6))

    # ---- Panel 1: ECDFs + KS ----------------------------------------
    ax = axes[0]
    cx, cy = _ecdf(c_plot)
    tx, ty = _ecdf(t_plot)
    ax.step(cx, cy, where="post", color="steelblue",  label="candidate", alpha=0.85)
    ax.step(tx, ty, where="post", color="darkorange", label="target",    alpha=0.85)

    if ks_step:
        d    = ks_step.details
        stat = d["stat"]
        pval = d["pval"]
        all_x = np.sort(np.unique(np.concatenate([cx, tx])))
        c_cdf = np.searchsorted(cx, all_x, side="right") / len(cx)
        t_cdf = np.searchsorted(tx, all_x, side="right") / len(tx)
        i_max = np.argmax(np.abs(c_cdf - t_cdf))
        x_ks  = all_x[i_max]
        y_lo, y_hi = sorted([c_cdf[i_max], t_cdf[i_max]])
        ax.plot([x_ks, x_ks], [y_lo, y_hi], color="red", lw=2.5, label=f"D = {stat:.4f}")
        mlogp = -np.log10(np.clip(pval, 1e-300, 1.0))
        kw = {**_TXT, "transform": ax.transAxes}
        ax.text(0.04, 0.96,
                f"KS stat = {stat:.3g}\n-log10p = {mlogp:.1f}\n→ {'PASS' if ks_step.passed else 'FAIL'}",
                **kw)

    ax.set_xlabel("value")
    ax.set_ylabel("cumulative probability")
    ax.set_title("Empirical CDFs")
    ax.legend(fontsize=8)

    # ---- Panel 2: KDE linear + t-test ----------------------------------
    ax = axes[1]
    _kde(ax, c_plot, "_nolegend_", "steelblue")
    _kde(ax, t_plot, "_nolegend_", "darkorange")
    c_mean = float(np.mean(c_plot))
    t_mean = float(np.mean(t_plot))
    ax.axvline(c_mean, color="steelblue",  ls=":", lw=1.5, alpha=0.9, label=f"{c_mean:.3g}")
    ax.axvline(t_mean, color="darkorange", ls=":", lw=1.5, alpha=0.9, label=f"{t_mean:.3g}")

    if t_step:
        d     = t_step.details
        mlogp = -np.log10(np.clip(d["pval"], 1e-300, 1.0))
        kw    = {**_TXT, "transform": ax.transAxes}
        ax.text(0.04, 0.96,
                f"t = {d['stat']:.3g}\n-log10p = {mlogp:.1f}\n→ {'PASS' if t_step.passed else 'FAIL'}",
                **kw)
    else:
        kw = {**_TXT_GREY, "transform": ax.transAxes}
        ax.text(0.04, 0.96, "t-test\nnot reached", **kw)

    ax.set_xlabel("value")
    ax.set_ylabel("density")
    ax.set_title("Distributions — linear scale")
    ax.legend(fontsize=8)

    # ---- Panel 3: KDE log + MAD ----------------------------------------
    ax = axes[2]
    c_pos = c_plot[c_plot > 0]
    t_pos = t_plot[t_plot > 0]

    if mad_step:
        d     = mad_step.details
        c_med = d["cand_median"]
        t_med = d["target_median"]
        thr   = d["threshold"]

        lo = t_med - thr
        hi = t_med + thr
        all_pos = np.concatenate([c_pos, t_pos]) if len(c_pos) and len(t_pos) else np.array([1e-6])
        lo_log  = np.log10(lo) if lo > 0 else np.log10(all_pos.min()) - 1
        hi_log  = np.log10(hi) if hi > 0 else np.log10(all_pos.max()) + 1
        band_color = "limegreen" if mad_step.passed else "salmon"
        ax.axvspan(lo_log, hi_log, alpha=0.18, color=band_color,
                   label=f"±{d['n_mad']:.0f}×MAD", zorder=0)

    if len(c_pos) > 1:
        _kde(ax, np.log10(c_pos), "_nolegend_", "steelblue")
    if len(t_pos) > 1:
        _kde(ax, np.log10(t_pos), "_nolegend_", "darkorange")

    if mad_step:
        lc = np.log10(c_med) if c_med > 0 else None
        lt = np.log10(t_med) if t_med > 0 else None
        if lc is not None:
            ax.axvline(lc, color="steelblue",  ls=":", lw=1.5, alpha=0.9,
                       label=f"{c_med:.3g}")
        if lt is not None:
            ax.axvline(lt, color="darkorange", ls=":", lw=1.5, alpha=0.9,
                       label=f"{t_med:.3g}")
        if lc is not None and lt is not None:
            ax.annotate("", xy=(lc, 0), xytext=(lt, 0),
                        arrowprops=dict(arrowstyle="<->", color="grey", lw=1.5),
                        annotation_clip=False)
            ax.text((lc + lt) / 2, 0, f"  {d['distance']:.3g}",
                    fontsize=7, va="bottom", color="grey")
        kw = {**_TXT, "transform": ax.transAxes}
        ax.text(0.04, 0.96,
                f"MAD = {d['MAD']:.3g}\ndist = {d['distance']:.3g}\nthr = {d['threshold']:.3g}\n"
                f"→ {'PASS' if mad_step.passed else 'FAIL'}",
                **kw)
    else:
        kw = {**_TXT_GREY, "transform": ax.transAxes}
        ax.text(0.04, 0.96, "MAD test\nnot reached", **kw)

    ax.set_xlabel("log₁₀(value)")
    ax.set_ylabel("density")
    ax.set_title("Distributions — log scale")
    ax.legend(fontsize=8)

    plt.tight_layout()
    plt.savefig(out_path, dpi=120, bbox_inches="tight")
    plt.close(fig)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser():
    p = argparse.ArgumentParser(
        description="Test whether a candidate distribution is compatible with a target for unit injection.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("candidate", nargs="?",
                   help="File with candidate values (.npy, one float per line, or first TSV column)")
    p.add_argument("target", nargs="?",
                   help="File with target (reference) values")
    p.add_argument("--ks-threshold", type=float, default=0.3, metavar="FLOAT",
                   help="KS statistic must be below this for KS PASS")
    p.add_argument("--sig-level",    type=float, default=0.05, metavar="FLOAT",
                   help="Significance level for KS p-value and Welch t-test")
    p.add_argument("--n-mad",        type=float, default=3.0, metavar="FLOAT",
                   help="Candidate median must lie within this many MADs of the target median")
    p.add_argument("--test-mode", action="store_true",
                   help="Run synthetic test cases designed to fail at each pipeline step")
    p.add_argument("--quiet", action="store_true",
                   help="Print only the final PASS/FAIL outcome")
    return p


def main():
    args = _build_parser().parse_args()

    if args.test_mode:
        cases = _generate_test_cases()
        for label, candidate, target in cases:
            result = run_pipeline(candidate, target, args.ks_threshold, args.sig_level, args.n_mad)
            print(f"\n{'─' * 64}")
            print(f"Case : {label}")
            print(result)
        print()
        return

    if not args.candidate or not args.target:
        _build_parser().error("provide CANDIDATE and TARGET files, or use --test-mode")

    candidate = _load_values(args.candidate)
    target    = _load_values(args.target)
    result    = run_pipeline(candidate, target, args.ks_threshold, args.sig_level, args.n_mad)

    if args.quiet:
        print(result.outcome)
    else:
        print(result)

    sys.exit(0 if result.outcome == "PASS" else 1)


if __name__ == "__main__":
    main()
