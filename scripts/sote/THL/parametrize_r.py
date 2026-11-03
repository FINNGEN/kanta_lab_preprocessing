#!/usr/bin/env python3
"""
parametrize_r.py  --  inject optparse CLI parameters into a monolithic R script.

Scans for hardcoded file paths in fread(), read_excel(), and fwrite(file=...)
calls, replaces them with opt$<name> references, and prepends an optparse block.
The main input file (.csv.gz) is processed in chunks via readr::read_csv_chunked.

Usage:
  python3 parametrize_r.py
  python3 parametrize_r.py Real_kanta_labs_2016_DF14_toFG.R -o out.R
  Rscript out.R --chunk_size 50000 --debug TRUE
"""

import re
import sys
import argparse
from pathlib import Path


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

FREAD_RE  = re.compile(r'(?P<lhs>\w+)\s*<-\s*fread\([\'"](?P<path>[^\'"]+)[\'"]')
EXCEL_RE  = re.compile(r'(?P<lhs>\w+)\s*<-\s*read_excel\([\'"](?P<path>[^\'"]+)[\'"]')
FWRITE_RE = re.compile(r'fwrite\([^,]+,\s*file\s*=\s*[\'"](?P<path>[^\'"]+)[\'"]')


def filename_stem(path):
    stem = Path(path).name
    while '.' in stem:
        stem = stem[:stem.rfind('.')]
    stem = re.split(r'[-_]\d{4}', stem)[0]
    stem = re.sub(r'[^a-zA-Z0-9]', '_', stem).strip('_').lower()
    return stem[:24]


def collect_params(lines):
    params        = []
    path_to_param = {}
    used_names    = set()
    out_idx       = [1]

    def register(pname, path, is_output):
        if path in path_to_param:
            return
        base, n = pname, 2
        while pname in used_names:
            pname = f"{base}_{n}"; n += 1
        used_names.add(pname)
        params.append((pname, path, is_output))
        path_to_param[path] = pname

    lhs_counts = {}
    for line in lines:
        if line.strip().startswith('#'):
            continue
        for pattern in (FREAD_RE, EXCEL_RE):
            m = pattern.search(line)
            if m:
                lhs = m.group('lhs')
                lhs_counts[lhs] = lhs_counts.get(lhs, 0) + 1

    for line in lines:
        if line.strip().startswith('#'):
            continue
        m = FREAD_RE.search(line)
        if m:
            lhs, path = m.group('lhs'), m.group('path')
            pname = lhs if lhs_counts.get(lhs, 1) == 1 else filename_stem(path)
            register(pname, path, is_output=False)
            continue
        m = EXCEL_RE.search(line)
        if m:
            lhs, path = m.group('lhs'), m.group('path')
            pname = lhs if lhs_counts.get(lhs, 1) == 1 else filename_stem(path)
            register(pname, path, is_output=False)
            continue
        m = FWRITE_RE.search(line)
        if m:
            path  = m.group('path')
            pname = f"output_{out_idx[0]}"; out_idx[0] += 1
            register(pname, path, is_output=True)

    return params, path_to_param


def build_preamble(params):
    lines = [
        "library(optparse)\n",
        "\n",
        "option_list <- list(\n",
        '  make_option("--data_dir",   type="character", default=file.path(getwd(), "test")),\n',
    ]
    for pname, path, is_output in params:
        fname = Path(path).name
        if is_output:
            if not fname.endswith('.gz'):
                fname += '.gz'
            lines.append(f'  make_option("--{pname}", type="character", default=file.path(getwd(), "{fname}")),\n')
        else:
            lines.append(f'  make_option("--{pname}", type="character", default=NULL),\n')
    lines += [
        '  make_option("--chunk_size", type="integer",  default=10000L),\n',
        '  make_option("--test",       action="store_true", default=FALSE),\n',
        '  make_option("--debug",      type="logical",  default=FALSE)\n',
        ")\n",
        'opt <- parse_args(OptionParser(option_list=option_list))\n',
        "\n",
    ]
    for pname, path, is_output in params:
        if not is_output:
            fname = Path(path).name
            lines.append(f'if (is.null(opt${pname})) opt${pname} <- file.path(opt$data_dir, "{fname}")\n')
    lines.append("\n")
    return lines


def substitute(lines, path_to_param):
    out = []
    for line in lines:
        if re.match(r'\s*library\s*\(\s*optparse\s*\)', line):
            continue
        for path, pname in path_to_param.items():
            line = line.replace(f"'{path}'", f"opt${pname}")
            line = line.replace(f'"{path}"', f"opt${pname}")
        out.append(line)
    return out


def inject_chunk_loop(lines, main_pname):
    """
    Replace the single fread(opt$<main>) call with a readr chunk loop.
    Everything between that line and the last fwrite becomes the loop body.
    fwrite calls inside the loop are modified to append after the first chunk.
    """
    main_re   = re.compile(rf'(\w+)\s*<-\s*fread\(opt\${re.escape(main_pname)}')
    fwrite_re = re.compile(r'\bfwrite\(')

    main_idx     = None
    main_varname = 'chunk'
    fwrite_idxs  = []

    for i, line in enumerate(lines):
        m = main_re.search(line)
        if m and main_idx is None:
            main_idx     = i
            main_varname = m.group(1)
        if fwrite_re.search(line) and main_idx is not None:
            fwrite_idxs.append(i)

    if main_idx is None or not fwrite_idxs:
        return lines

    last_fwrite_idx = max(fwrite_idxs)

    before = lines[:main_idx]
    body   = lines[main_idx + 1 : last_fwrite_idx + 1]
    after  = lines[last_fwrite_idx + 1:]

    # Modify fwrite calls: remove col.names/row.names, add append mode
    def patch_fwrite(line):
        line = re.sub(r',\s*col\.names\s*=\s*\w+', '', line)
        line = re.sub(r',\s*row\.names\s*=\s*\w+', '', line)
        line = re.sub(r'\)\s*$', ', append=!first_chunk, col.names=first_chunk, nThread=1, compress="gzip")\n', line.rstrip())
        return line

    indented_body = []
    for line in body:
        line = patch_fwrite(line) if fwrite_re.search(line) else line
        indented_body.append('    ' + line)

    loop = [
        '\n',
        'library(readr)\n',
        'first_chunk <- TRUE\n',
        'n_chunk     <- 0L\n',
        'n_total     <- 0L\n',
        'message("Starting chunked processing of ", opt$' + main_pname + ')\n',
        'invisible(read_csv_chunked(\n',
        f'  opt${main_pname},\n',
        f'  SideEffectChunkCallback$new(function({main_varname}, .pos) {{\n',
        f'    {main_varname} <- as.data.frame({main_varname})\n',
        f'    {main_varname}[] <- lapply({main_varname}, as.character)\n',
        f'    chunk_input_rows_ <- nrow({main_varname})\n',
        '    if (isTRUE(opt$test) && n_chunk >= 2L) return(invisible(NULL))\n',
    ] + indented_body + [
        '    chunk_kept_rows_ <- nrow(' + main_varname + ')\n',
        '    n_total <<- n_total + chunk_kept_rows_\n',
        '    n_chunk <<- n_chunk + 1L\n',
        '    if (isTRUE(opt$debug)) {\n',
        f'      message(sprintf("  chunk %d | %d/%d (%.0f%%) | total kept: %d", n_chunk, chunk_kept_rows_, chunk_input_rows_, 100*chunk_kept_rows_/chunk_input_rows_, n_total))\n',
        '    } else {\n',
        f'      message(sprintf("  chunk %d | %d/%d (%.0f%%)", n_chunk, chunk_kept_rows_, chunk_input_rows_, 100*chunk_kept_rows_/chunk_input_rows_))\n',
        '    }\n',
        '    first_chunk <<- FALSE\n',
        '    invisible(NULL)  # prevent FALSE return from stopping readr iteration\n',
        '  }),\n',
        '  chunk_size = opt$chunk_size,\n',
        '  col_types  = cols(.default = col_character()),\n',
        '  progress   = FALSE\n',
        '))\n',
        'message("Done. Total rows processed: ", n_total)\n',
        '\n',
    ]

    return before + loop + after


def main():
    here = Path(__file__).parent
    ap   = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("script", nargs="?", default=str(here / "Real_kanta_labs_2016_DF14_toFG.R"))
    ap.add_argument("-o", "--output", default="out.R")
    args = ap.parse_args()

    try:
        with open(args.script, encoding="utf-8") as f:
            lines = f.readlines()
    except UnicodeDecodeError:
        with open(args.script, encoding="latin-1") as f:
            lines = f.readlines()

    params, path_to_param = collect_params(lines)
    if not params:
        print("No file references found.", file=sys.stderr)
        sys.exit(1)

    # Identify main input: the .gz file
    main_pname = next((p for p, path, _ in params if path.endswith('.gz')), None)

    print(f"Found {len(params)} file reference(s):", file=sys.stderr)
    for pname, path, is_output in params:
        tag  = "output" if is_output else "input "
        main = " [MAIN - will be chunked]" if pname == main_pname else ""
        print(f"  [{tag}] --{pname:30s} default: {path}{main}", file=sys.stderr)

    body = substitute(lines, path_to_param)
    if main_pname:
        body = inject_chunk_loop(body, main_pname)

    result = build_preamble(params) + body

    with open(args.output, 'w', encoding="utf-8") as f:
        f.writelines(result)
    print(f"Written to {args.output}", file=sys.stderr)


if __name__ == "__main__":
    main()
