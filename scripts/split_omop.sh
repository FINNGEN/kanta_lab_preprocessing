#!/bin/bash

# Check if input file and output directory are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_file> <output_directory>"
    exit 1
fi

# Input file and output directory
input_file="$1"
output_dir="$2"

# Ensure the input file exists
if [ ! -f "$input_file" ]; then
    echo "File not found: $input_file"
    exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$output_dir"

# Extract the header
header=$(zcat -f "$input_file" | head -n1)

# Split the file based on column 25, preserving header in each file
awk -v header="$header" -v outdir="$output_dir" '
BEGIN { FS=OFS="\t" }
NR == 1 { next }  # Skip original header
{
    key = $25
    filename = outdir "/" key ".tsv"
    if (!(key in files)) {
        print header > filename
        files[key] = 1
    }
    print $0 > filename
}' <(zcat -f $input_file  )

# Compress all generated TSV files
echo "Compressing files..."
find "$output_dir" -name "*.tsv" -type f -exec gzip {} \;

echo "Files split and compressed successfully in directory: $output_dir"
