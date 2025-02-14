#!/usr/bin/env bash
set -euo pipefail
if [[ "${TRACE-0}" == "1" ]]; then
  set -o xtrace
fi

echo "Hello!"

echo
echo "Making .txt.gz file..."
time clickhouse --param_filePathMungedTxtGz "$1" --queries-file make_tsv_gzipped.sql | gzip -c > "$2"

echo
echo "Making .parquet file..."
time clickhouse --param_filePathCleanTxtGz "$2" --queries-file make_parquet.sql > "$3"

echo
echo "Basic QC:  " $2
clickhouse --param_filePath "$2" --queries-file qc.sql

echo
echo "Basic QC:  " $3
clickhouse --param_filePath "$3" --queries-file qc.sql

echo
echo "Bye."

