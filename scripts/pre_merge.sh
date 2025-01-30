#!/bin/bash

# Create a directory to store the merged files
DIR="/mnt/disks/data/kanta/v2/"
mkdir -p $DIR

merged_dir="$DIR/merged_files"
rm -r $merged_dir && mkdir -p "$merged_dir"

# Loop through the files and merge the parts
ls "$DIR"/raw/*internal_2.0.txt.gz* > $DIR/tmp.txt
while read file
do
    echo $file
    du -sb $file | awk '{$1=$1/2^30"GB"}1'

    # Check if the file has multiple parts
    if [[ $file == *".part"* ]]; then
        # Get the base filename without the part number
        base_filename=$(echo "$file" | sed 's/\.part[0-9][0-9]*//')
        
        # Concatenate the parts into a single file
        cat "$file" >> "$merged_dir/$(basename "$base_filename")"
    else
        # Copy the single-part file to the merged directory
        cp "$file" "$merged_dir/"
    fi
done < $DIR/tmp.txt

