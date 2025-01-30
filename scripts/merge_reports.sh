#!/bin/bash

# Input and output file names
input_file="$1"
output_file="$2"

# Check if input and output files are provided
if [ $# -ne 2 ]; then
    echo "Usage: $0 <input_gzipped_file> <output_file>"
    exit 1
fi


zcat -f  $input_file | awk '
BEGIN { FS=OFS="\t"; seen_key = 0 }
{
    if (NR == 1) {
        # Initialize with first row
        prev_key = $1 SUBSEP $2 SUBSEP $3
        prev_value = $4
        seen_key = 1
        next
    }

    current_key = $1 SUBSEP $2 SUBSEP $3

    if (current_key == prev_key) {
        # Same key, append value
        prev_value = prev_value "::" $4
	seen_key++
    } else {
        # Different key, output previous key
        split(prev_key, cols, SUBSEP)
     	prefix = seen_key > 1 ? "HUOM::" seen_key "::" : ""
	print cols[1], cols[2], cols[3], prefix prev_value
        # Reset for new key
        prev_key = current_key
        prev_value = $4
        seen_key =1 
    }
}
END {
    # Output last key
    split(prev_key, cols, SUBSEP)
    prefix = seen_key > 1 ? "HUOM::"seen_key"::" : ""
    print cols[1], cols[2], cols[3], prefix prev_value
}' | bgzip -c  > $output_file

zcat -f $input_file | wc -l
zcat $output_file | wc -l
echo "Merged file created: $output_file"
