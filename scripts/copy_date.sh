#!/bin/bash


filename="$1"
prefix="$2"
destination=$3


# Extract the basename of the filename
basename=$(basename "$filename")
# Extract the basename of the filename
if [[ "$basename" =~ ^${prefix}_(.*)\.([^.]+)$ ]]; then
    rest_of_filename="${BASH_REMATCH[1]}"
    extension="${BASH_REMATCH[2]}"
    echo  $rest_of_filename $extension
    current_date=$(date +"%Y_%m_%d")
    new_basename="${prefix}_${current_date}_${rest_of_filename}.${extension}"
    echo $new_basename
    echo "gsutil cp $filename $destination/${new_basename}"
else
    echo "Filename does not match the expected pattern (<prefix>_FOO_BAR.extension)"
    exit 1
fi

