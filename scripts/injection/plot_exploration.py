#!/usr/bin/env python3
"""
plot_exploration.py

Scatter plot for unit injection exploration (TEST_NAME level).

Importable: call make_scatter_plot(plot_name).
Standalone: reads plot_name_level.tsv from the current directory.
"""

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.gridspec import GridSpec
from scipy.stats import gaussian_kde

DEFAULT_OUTPUT = "test_names_exploration_scatter.png"


def make_scatter_plot(plot_name, output=DEFAULT_OUTPUT):
    fig = plt.figure(figsize=(11, 7))
    gs  = GridSpec(1, 2, width_ratios=[4, 1], wspace=0.05)
    ax_scatter = fig.add_subplot(gs[0])
    ax_kde     = fig.add_subplot(gs[1], sharey=ax_scatter)

    ax_scatter.scatter(plot_name["COUNT"], plot_name["top_prevalence"],
                       alpha=0.5, s=20)
    ax_scatter.set_xlabel("Count (value, no unit)")
    ax_scatter.set_ylabel("Top unit prevalence (%)")
    ax_scatter.set_title("TEST_NAME level — injection candidates")
    ax_scatter.set_xscale("log")

    prev = plot_name["top_prevalence"].dropna().values
    ys   = np.linspace(prev.min(), prev.max(), 500)
    kde  = gaussian_kde(prev, bw_method="scott")
    ax_kde.fill_betweenx(ys, kde(ys), alpha=0.4, color="steelblue")
    ax_kde.plot(kde(ys), ys, color="steelblue", lw=1.2)
    ax_kde.set_xlabel("density")
    ax_kde.tick_params(labelleft=False)
    ax_kde.set_xlim(left=0)

    plt.savefig(output, dpi=150, bbox_inches="tight")
    print(f"Saved {output}")


def main():
    p = argparse.ArgumentParser(
        description="Generate exploration scatter plot from plot_name_level.tsv.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--tsv",    default="plot_name_level.tsv")
    p.add_argument("--output", default=DEFAULT_OUTPUT)
    p.add_argument("--force", action="store_true", help="Overwrite existing plot")
    args = p.parse_args()

    if args.force:
        Path(args.output).unlink(missing_ok=True)

    make_scatter_plot(pd.read_csv(args.tsv, sep="\t"), output=args.output)


if __name__ == "__main__":
    main()
