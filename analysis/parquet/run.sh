#!/usr/bin/env bash
set -euo pipefail
if [[ "${TRACE-0}" == "1" ]]; then
  set -o xtrace
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Ensure output directory exists
mkdir -p "$2"

# Generate filenames with prefix
TXT_GZ_FILE="${2%/}/${3}.txt.gz"
PARQUET_FILE="${2%/}/${3}.parquet"

echo "Hello!"
echo
echo "Making .txt.gz file..."
time clickhouse --param_filePathMungedTxtGz "$1" --queries-file "${SCRIPT_DIR}/make_tsv_gzipped.sql" | gzip -c > "$TXT_GZ_FILE"
echo
echo "Making .parquet file..."
time clickhouse --param_filePathCleanTxtGz "$TXT_GZ_FILE" --queries-file "${SCRIPT_DIR}/make_parquet.sql" > "$PARQUET_FILE"


echo
echo "Basic QC:  " $3
clickhouse --param_filePath "$PARQUET_FILE" --queries-file "${SCRIPT_DIR}/qc.sql" 


python3 "${SCRIPT_DIR}/count_na.py" $PARQUET_FILE
