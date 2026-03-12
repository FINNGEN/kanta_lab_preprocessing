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

CPU_COUNT=$(nproc)
MEM_LIMIT_BYTES=$(free -b | awk '/^Mem:/{print $2 - 1073741824}')


# Ensure the temporary directory for sorting exists
time clickhouse local \
     --param_filePathMungedTxtGz "$INPUT_FILE" \
     --param_outputFile "$TXT_GZ_FILE" \
     --queries-file "${SQL_DIR}/make_tsv_gzipped.sql" \
     --max_memory_usage "$MEM_LIMIT_BYTES"  | gzip -c > "$TXT_GZ_FILE"

echo
echo "Making .parquet file..."
time clickhouse local \
  --param_filePathCleanTxtGz "$TXT_GZ_FILE" \
  --queries-file "${SQL_DIR}/make_parquet.sql" \
  --max_memory_usage "$MEM_LIMIT_BYTES" \
  > "$PARQUET_FILE"
echo

#CHECK ORDER
time clickhouse local \
  --query "
    SELECT count() AS unsorted_count
    FROM (
      SELECT 
        ROW_ID,
        lagInFrame(ROW_ID, 1, ROW_ID - 1) OVER (ORDER BY rowNumberInAllBlocks()) AS prev_ROW_ID
      FROM file('$PARQUET_FILE', Parquet)
    )
    WHERE ROW_ID <= prev_ROW_ID
  " --format TSVRaw
if [ $? -eq 0 ]; then
    echo "Check passed: Parquet is sorted by ROW_ID."
else
    echo "Check FAILED: Parquet is NOT sorted by ROW_ID!" >&2
    exit 1
fi

# Or simpler: just check if min ROW_ID in each chunk increases
time clickhouse local \
  --query "
    WITH chunks AS (
      SELECT 
        intDiv(rowNumberInAllBlocks(), 1000000) AS chunk,
        min(ROW_ID) AS min_id,
        max(ROW_ID) AS max_id
      FROM file('$PARQUET_FILE', Parquet)
      GROUP BY chunk
    )
    SELECT count() 
    FROM chunks AS c1
    JOIN chunks AS c2 ON c2.chunk = c1.chunk + 1
    WHERE c1.max_id > c2.min_id
  " --format TSVRaw

if [ $? -eq 0 ]; then
    echo "Check passed: Parquet is sorted by ROW_ID."
else
    echo "Check FAILED: Parquet is NOT sorted by ROW_ID!" >&2
    exit 1
fi

echo "Basic QC:  " "${OUTPUT_DIR%/}/${OUTPUT_PREFIX}.log"
python3 "${SCRIPT_DIR}/qc.py" $PARQUET_FILE | tee  "${OUTPUT_DIR%/}/${OUTPUT_PREFIX}.log"
python3 "${SCRIPT_DIR}/count_na.py" $PARQUET_FILE | tee -a  "${OUTPUT_DIR%/}/${OUTPUT_PREFIX}.log"

echo "Schema: " "${OUTPUT_DIR%/}/${OUTPUT_PREFIX}_schema.json"
python3 "${SCRIPT_DIR}/schema.py" $PARQUET_FILE  "${OUTPUT_DIR%/}/${OUTPUT_PREFIX}_schema.json"
