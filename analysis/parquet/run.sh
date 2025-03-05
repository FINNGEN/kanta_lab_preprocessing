#!/usr/bin/env bash
set -euo pipefail
if [[ "${TRACE-0}" == "1" ]]; then
  set -o xtrace
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


echo "Hello!"

echo
echo "Making .txt.gz file..."
time clickhouse --param_filePathMungedTxtGz "$1" --queries-file "${SCRIPT_DIR}/make_tsv_gzipped.sql" | gzip -c > "$2"

echo
echo "Making .parquet file..."
time clickhouse --param_filePathCleanTxtGz "$2" --queries-file "${SCRIPT_DIR}/make_parquet.sql"  > "$3"
