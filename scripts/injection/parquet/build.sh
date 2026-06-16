#!/usr/bin/env bash
# build.sh <input.tsv.gz>
# Runs the two-step pipeline:
#   1. munged TSV.gz  →  clean TSV.gz  (make_tsv_gzipped.sql)
#   2. clean TSV.gz   →  INJECT.parquet (make_parquet.sql)
# Outputs are written next to the input file.

set -euo pipefail

DEFAULT_INPUT=~/fg-3/kanta_v3/core/kanta_dev_2026_03_09.txt.gz

if [[ $# -gt 1 ]]; then
    echo "Usage: $0 [input.tsv.gz]" >&2
    exit 1
fi

INPUT="$(realpath "${1:-$DEFAULT_INPUT}")"
CLEAN_TSV="$PWD/clean.tsv.gz"
PARQUET="$PWD/INJECT.parquet"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> Step 1: $INPUT → $CLEAN_TSV"
clickhouse local \
    --query "$(cat "$SCRIPT_DIR/make_tsv_gzipped.sql")" \
    --param_filePathMungedTxtGz="$INPUT" \
    | gzip > "$CLEAN_TSV"

echo "==> Step 2: $CLEAN_TSV → $PARQUET"
clickhouse local \
    --query "$(cat "$SCRIPT_DIR/make_parquet.sql")" \
    --param_filePathCleanTxtGz="$CLEAN_TSV" \
    > "$PARQUET"

echo "Done. Output: $PARQUET"
