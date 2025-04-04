#!/bin/bash

# Check if input file and output directory are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <input_file> <output_directory> [column_header]"
    echo "Default column_header: harmonization_omop::OMOP_ID"
    exit 1
fi

# Input parameters
input_file="$1"
output_dir="$2"
# Use provided column header or default if not specified
column_header="${3:-harmonization_omop::OMOP_ID}"

# Ensure the input file exists
if [ ! -f "$input_file" ]; then
    echo "File not found: $input_file"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$output_dir"

# Extract the header and find the column number
header=$(zcat -f "$input_file" | head -n1)
column_number=$(echo "$header" | tr '\t' '\n' | grep -n "^${column_header}$" | cut -d: -f1)

# Check if the column was found
if [ -z "$column_number" ]; then
    echo "Error: Column header '${column_header}' not found in the file"
    exit 1
fi

echo "Found '${column_header}' at column ${column_number}"

# Split the file based on the determined column, preserving header in each file
awk -v header="$header" -v outdir="$output_dir" -v col="$column_number" '
BEGIN { FS=OFS="\t" }
NR == 1 { next }  # Skip original header
{
    key = $col;
    # Replace any slashes in the key with underscores to prevent directory creation issues.
    gsub("/", "_", key);
    filename = outdir "/" key ".tsv";
    if (!(key in files)) {
        print header > filename;
        files[key] = 1;
    }
    print $0 > filename;
}' <(zcat -f "$input_file")

# Compress all generated TSV files
echo "Compressing files..."
find "$output_dir" -name "*.tsv" -type f -exec gzip {} \;

echo "Files split and compressed successfully in directory: $output_dir"
