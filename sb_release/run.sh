#!/usr/bin/env bash
set -euo pipefail
if [[ "${TRACE-0}" == "1" ]]; then
  set -o xtrace
fi
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if core name is provided
if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <input_file> <output_dir> <output_prefix> <core_name>"
  echo "  <core_name> - Name of subdirectory in SCRIPT_DIR containing SQL scripts"
  exit 1
fi

# Input parameters
INPUT_FILE="$1"
OUTPUT_DIR="$2"
OUTPUT_PREFIX="$3"
CORE_NAME="$4"

# Define SQL directory
SQL_DIR="${SCRIPT_DIR}/${CORE_NAME}"

# Check if SQL directory exists
if [[ ! -d "$SQL_DIR" ]]; then
  echo "Error: SQL directory not found: $SQL_DIR"
  exit 1
fi

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Generate filenames with prefix
TXT_GZ_FILE="${OUTPUT_DIR%/}/${OUTPUT_PREFIX}.txt.gz"
PARQUET_FILE="${OUTPUT_DIR%/}/${OUTPUT_PREFIX}.parquet"

echo "Hello!"
echo
echo "Making .txt.gz file..."
time clickhouse --param_filePathMungedTxtGz "$INPUT_FILE" --queries-file "${SQL_DIR}/make_tsv_gzipped.sql" | gzip -c > "$TXT_GZ_FILE"
echo
echo "Making .parquet file..."
time clickhouse --param_filePathCleanTxtGz "$TXT_GZ_FILE" --queries-file "${SQL_DIR}/make_parquet.sql" > "$PARQUET_FILE"
echo
echo "Basic QC:  " $OUTPUT_PREFIX
clickhouse --param_filePath "$PARQUET_FILE" --queries-file "${SCRIPT_DIR}/qc.sql" 
python3 "${SCRIPT_DIR}/count_na.py" $PARQUET_FILE
python3 "${SCRIPT_DIR}/schema.py" $PARQUET_FILE  "${OUTPUT_DIR%/}/${OUTPUT_PREFIX}_schema.json"
