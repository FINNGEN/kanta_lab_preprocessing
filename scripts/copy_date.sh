#!/bin/bash
filenames=$1
dir=$2
destination="${dir%/}"

# Process each filename from the input file
while read f;
do
    full_basename=$(basename "$f")
    
    # Split the filename into name and extension
    if [[ "$full_basename" =~ ^([^.]+)(\..+)$ ]]; then
        basename_part="${BASH_REMATCH[1]}"
        full_extension="${BASH_REMATCH[2]}"
        
        # Generate the current date
        current_date=$(date +"%Y_%m_%d")
        
        # Create new filename with date added to basename
        new_basename="${basename_part}_${current_date}${full_extension}"
        
        echo "gsutil cp $f $destination/${new_basename}"
    else
        echo "Filename does not have an extension"
        exit 1
    fi
done < $filenames
