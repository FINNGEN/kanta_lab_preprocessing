#!/usr/bin/env python3
"""
build_site.py

Generate a local HTML site from injection pipeline outputs.

Usage:
  python3 build_site.py [--out-dir PATH]

Reads from {out_dir}/data/ and {out_dir}/plots/, writes index.html and doc.html
to {out_dir}/.  Pass --out-dir to match what you used with explore_test_name.py.
"""

import argparse
import json
from pathlib import Path

import markdown
import pandas as pd


# ---------------------------------------------------------------------------
# Column label overrides and preferred display order.
# Columns not listed here are appended after these in their original TSV order.
# ---------------------------------------------------------------------------

COL_LABELS = {
    "TEST_NAME":        "Test name",
    "TYPE":             "Type",
    "CUTOFF":           "Cutoff",
    "OUTCOME":          "Outcome",
    "UNIT":             "Unit",
    "UNIT_PREVALENCE":  "Unit prev (%)",
    "N_CANDIDATE":      "N cand",
    "N_TARGET":         "N target",
    "KS_STAT":          "KS stat",
    "KS_MLOGP":         "KS −log10p",
    "KS_PASS":          "KS pass",
    "T_STAT":           "T stat",
    "T_MLOGP":          "T −log10p",
    "T_PASS":           "T pass",
    "MAD_DIST":         "MAD dist",
    "MAD_THRESHOLD":    "MAD thr",
    "MAD_PASS":         "MAD pass",
    "NOTES":            "Notes",
}

COL_ORDER = list(COL_LABELS.keys())

# Pixel widths for columns that are inherently short
COL_WIDTHS = {
    "TYPE":            80,
    "CUTOFF":          80,
    "OUTCOME":         75,
    "KS_PASS":         70,
    "T_PASS":          70,
    "MAD_PASS":        70,
    "KS_STAT":         75,
    "KS_MLOGP":        85,
    "T_STAT":          75,
    "T_MLOGP":         85,
    "MAD_DIST":        80,
    "MAD_THRESHOLD":   80,
    "UNIT_PREVALENCE": 90,
    "N_CANDIDATE":     85,
    "N_TARGET":        75,
    "SUB_DIST":        70,
}

# Columns to drop entirely from the landing table (reserved for per-test pages)
EXCLUDE_COLS = {
    "SUB_DIST",
    "BIMODAL_STATUS", "BIMODAL_SEP", "BIMODAL_BC", "BIMODAL_DIP_P",
    "SCORE_GLOBAL", "SCORE_SPLIT", "SCORE_IMPROVEMENT",
    "CAND_DECILES", "TARG_DECILES",
    "PREVALENCE_DICT",
}


# ---------------------------------------------------------------------------
# HTML templates  (use __MARKER__ substitution to avoid Python/JS brace clash)
# ---------------------------------------------------------------------------

_INDEX = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Unit injection results</title>
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
  <link rel="stylesheet"
    href="https://cdn.datatables.net/1.13.8/css/dataTables.bootstrap5.min.css">
  <style>
    body     { padding: 2rem; font-size: 0.9rem; }
    nav a    { margin-right: 1.5rem; font-weight: 500; }
    .scatter { text-align: center; margin-bottom: 2rem; }
    .scatter img { max-width: 100%; max-height: 480px; }
    .pass { color: #198754; font-weight: 600; }
    .fail { color: #dc3545; font-weight: 600; }
    .skip { color: #6c757d; }
    small.hint { color: #6c757d; font-size: 0.8rem; }
    tfoot input, thead tr.search-row input {
                  width: 100%; font-size: 0.78rem; padding: 2px 4px;
                  border: 1px solid #ced4da; border-radius: 3px; box-sizing: border-box; }
    #results td, #results th { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    #results td.col-wide, #results th.col-wide { white-space: normal; min-width: 160px; }
    .summary-tables { margin-bottom: 2rem; }
    .summary-tables h4 { margin-top: 1.2rem; font-size: 1rem; font-weight: 600; }
    .summary-tables table { border-collapse: collapse; font-size: 0.85rem; margin-bottom: 0.5rem; }
    .summary-tables th, .summary-tables td { border: 1px solid #dee2e6; padding: 0.25rem 0.6rem; }
    .summary-tables th { background: #f8f9fa; }
    .summary-tables pre { background: #f8f9fa; padding: 0.75rem; border-radius: 4px;
                           font-size: 0.78rem; overflow-x: auto; }
  </style>
</head>
<body>
  <nav class="mb-3">
    <a href="index.html"><b>Results</b></a>
    <a href="doc.html">Documentation</a>
  </nav>
  <hr>
  <h2>Unit injection results</h2>

  __SCATTER_BLOCK__

  __SUMMARY_SECTION__

  <p><small class="hint">Per-column search: text columns support regex (e.g. <code>^PASS$</code>,
  <code>mmol</code>). Numeric columns support comparisons: <code>&lt;3</code>,
  <code>&gt;=20</code>, <code>!=0</code>. The global box top-right searches all columns with regex.</small></p>

  <div style="overflow-x:auto">
  <table id="results" class="table table-sm table-striped table-bordered" style="width:100%">
    <thead>
      <tr class="search-row">__FOOTERS__</tr>
      <tr>__HEADERS__</tr>
    </thead>
    <tbody></tbody>
  </table>
  </div>

  <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
  <script src="https://cdn.datatables.net/1.13.8/js/jquery.dataTables.min.js"></script>
  <script src="https://cdn.datatables.net/1.13.8/js/dataTables.bootstrap5.min.js"></script>
  <script>
  const DATA        = __DATA_JSON__;
  const COLUMNS     = __COLUMNS_JSON__;
  const OUTCOME_COL = __OUTCOME_COL_IDX__;
  const NUMERIC_COLS = __NUMERIC_COLS_JSON__;  // set of column indices with numeric data

  // Per-column filter state
  const colFilters = new Array(COLUMNS.length).fill('');

  // Parse a numeric comparison expression like "<3", ">=20", "!=0"
  function parseNumeric(expr) {
    var m = expr.trim().match(/^([<>]=?|!=?)\s*(-?\d+\.?\d*(?:e[+-]?\d+)?)$/i);
    if (!m) return null;
    var op = m[1], val = parseFloat(m[2]);
    return function (x) {
      var n = parseFloat(x);
      if (isNaN(n)) return false;
      if (op === '<')  return n <  val;
      if (op === '>')  return n >  val;
      if (op === '<=') return n <= val;
      if (op === '>=') return n >= val;
      if (op === '=' || op === '==') return n === val;
      if (op === '!=') return n !== val;
      return true;
    };
  }

  // Global custom filter handles both numeric comparisons and regex
  $.fn.dataTable.ext.search.push(function (settings, data) {
    for (var i = 0; i < colFilters.length; i++) {
      var expr = colFilters[i].trim();
      if (!expr) continue;
      var numFn = NUMERIC_COLS[i] ? parseNumeric(expr) : null;
      if (numFn) {
        if (!numFn(data[i])) return false;
      } else {
        try {
          if (!new RegExp(expr, 'i').test(data[i])) return false;
        } catch (e) { /* invalid regex — ignore */ }
      }
    }
    return true;
  });

  $(document).ready(function () {
    var table = $('#results').DataTable({
      data:       DATA,
      columns:    COLUMNS,
      pageLength: 25,
      autoWidth:  false,
      order:      [[OUTCOME_COL, 'asc']],
      search:     { regex: true, smart: false },
      columnDefs: [{
        targets: OUTCOME_COL,
        render: function (data, type) {
          if (type !== 'display') return data;
          var cls = { PASS: 'pass', FAIL: 'fail', SKIP: 'skip' }[data] || '';
          return '<span class="' + cls + '">' + data + '</span>';
        }
      }]
    });

    // Wire per-column search inputs via event delegation (works with any DOM structure)
    $(document).on('input', '#results thead tr.search-row input', function () {
      var idx = $(this).closest('th').index();
      colFilters[idx] = this.value;
      table.draw();
    });
  });
  </script>
</body>
</html>
"""

_DOC = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Documentation — unit injection</title>
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css">
  <style>
    body    { padding: 2rem; max-width: 960px; }
    nav a   { margin-right: 1.5rem; font-weight: 500; }
    img     { max-width: 100%; }
    table   { border-collapse: collapse; width: 100%; margin: 1rem 0; }
    th, td  { border: 1px solid #dee2e6; padding: 0.4rem 0.75rem; vertical-align: top; }
    th      { background: #f8f9fa; font-weight: 600; }
    pre     { background: #f8f9fa; padding: 1rem; border-radius: 4px; overflow-x: auto; }
    code    { background: #f0f0f0; padding: 0.1rem 0.3rem; border-radius: 3px;
              font-size: 0.85em; }
    pre code { background: none; padding: 0; }
  </style>
</head>
<body>
  <nav class="mb-3">
    <a href="index.html">Results</a>
    <a href="doc.html"><b>Documentation</b></a>
  </nav>
  <hr>
  __README_HTML__
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_scatter(data_dir: Path, out_dir: Path) -> str:
    p = data_dir / "test_names_exploration_scatter.png"
    if p.exists():
        return str(p.resolve().relative_to(out_dir.resolve()))
    return ""


def _md_to_html(path: Path) -> str:
    if not path.exists():
        return ""
    return markdown.markdown(path.read_text(), extensions=["tables", "fenced_code"])


def _summary_section(data_dir: Path) -> str:
    parts = []
    for fname, title in (("summary_table.md",    "Category summary"),
                         ("assignment_summary.md", "Assignment summary")):
        html = _md_to_html(data_dir / fname)
        if html:
            parts.append(f'<h4>{title}</h4>{html}')
    if not parts:
        return ""
    return '<div class="summary-tables">' + "".join(parts) + "</div>"


def _scatter_block(rel_path: str) -> str:
    if not rel_path:
        return '<p class="text-muted">Scatter plot not found.</p>'
    return (f'<div class="scatter">'
            f'<img src="{rel_path}" alt="Scatter: TEST_NAMEs by count and prevalence">'
            f'</div>')


# ---------------------------------------------------------------------------
# Page builders
# ---------------------------------------------------------------------------

def build_index(out_dir: Path, data_dir: Path,
                inj_path: Path = None, scatter_override: str = None) -> None:
    inj = inj_path or data_dir / "injection_results.tsv"
    if not inj.exists():
        print(f"Warning: {inj} not found — table will be empty")
        df = pd.DataFrame(columns=COL_ORDER)
    else:
        df = pd.read_csv(inj, sep="\t", na_values=["NA"])

    # COL_ORDER first (if present), then any remaining TSV columns, minus excluded ones
    ordered   = [c for c in COL_ORDER  if c in df.columns]
    remaining = [c for c in df.columns if c not in set(ordered) and c not in EXCLUDE_COLS]
    all_cols  = ordered + remaining
    cols      = [(c, COL_LABELS.get(c, c)) for c in all_cols]

    df_out = df[all_cols].copy().fillna("")
    if "CUTOFF" in df_out.columns:
        df_out["CUTOFF"] = df_out["CUTOFF"].replace(float("inf"), "∞")
    rows   = df_out.to_dict(orient="records")

    # Which column indices hold numeric data
    numeric_set = {i for i, c in enumerate(all_cols)
                   if pd.api.types.is_numeric_dtype(df[c])}

    # Build columns array with widths embedded; mark wide columns for CSS
    wide_cols = {"TEST_NAME", "NOTES", "UNIT"}
    def _col_def(k, l):
        d = {"data": k, "title": l}
        if k in COL_WIDTHS:
            d["width"] = f"{COL_WIDTHS[k]}px"
        if k in wide_cols:
            d["className"] = "col-wide"
        return d

    headers           = "".join(f"<th>{label}</th>" for _, label in cols)
    footers           = "".join(f'<th><input type="text" placeholder="{label}"></th>'
                                for _, label in cols)
    data_json         = json.dumps(rows, allow_nan=False)
    columns_json      = json.dumps([_col_def(k, l) for k, l in cols])
    numeric_cols_json = json.dumps({i: True for i in numeric_set})
    outcome_idx       = next((i for i, (k, _) in enumerate(cols) if k == "OUTCOME"), 3)
    scatter_path      = scatter_override if scatter_override is not None else _find_scatter(data_dir, out_dir)

    html = (_INDEX
            .replace("__SCATTER_BLOCK__",     _scatter_block(scatter_path))
            .replace("__SUMMARY_SECTION__",   _summary_section(data_dir))
            .replace("__HEADERS__",           headers)
            .replace("__FOOTERS__",           footers)
            .replace("__DATA_JSON__",         data_json)
            .replace("__COLUMNS_JSON__",      columns_json)
            .replace("__NUMERIC_COLS_JSON__",  numeric_cols_json)
            .replace("__OUTCOME_COL_IDX__",   str(outcome_idx)))

    out = out_dir / "index.html"
    out.write_text(html, encoding="utf-8")
    print(f"Wrote {out}  ({len(rows)} rows)")


def build_doc(out_dir: Path, readme_path: Path) -> None:
    text = readme_path.read_text()
    readme_html = markdown.markdown(text, extensions=["tables", "fenced_code"])
    html = _DOC.replace("__README_HTML__", readme_html)
    out = out_dir / "doc.html"
    out.write_text(html)
    print(f"Wrote {out}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(
        description="Build HTML site from injection pipeline outputs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--data-dir", default=".", metavar="PATH",
                   help="Directory containing all pipeline outputs (TSVs, PNGs, MDs)")
    p.add_argument("--out-dir", default=None, metavar="PATH",
                   help="Directory where index.html and doc.html are written "
                        "(default: same as --data-dir)")
    p.add_argument("--injection-results", default=None, metavar="PATH",
                   help="Override path for injection_results.tsv")
    p.add_argument("--scatter", default=None, metavar="PATH",
                   help="Scatter plot filename relative to --data-dir (default: auto-detected)")
    p.add_argument("--readme", default=None, metavar="PATH",
                   help="Path to README.md (default: README.md next to this script)")
    args = p.parse_args()

    data_dir = Path(args.data_dir)
    out_dir  = Path(args.out_dir) if args.out_dir else Path(".")
    readme   = Path(args.readme) if args.readme else Path(__file__).parent / "README.md"
    inj      = Path(args.injection_results) if args.injection_results else None
    scatter  = args.scatter

    build_index(out_dir, data_dir, inj_path=inj, scatter_override=scatter)
    build_doc(out_dir, readme)


if __name__ == "__main__":
    main()
