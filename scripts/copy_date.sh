#!/bin/bash

filenames=$1
prefix=$2
dir=$3
destination="${dir%/}"

# Extract the basename of the filename
while read f;
do
    basename=$(basename "$f")
    # Extract the basename of the filename
    if [[ "$basename" =~ ^${prefix}(.*)\.([^.]+)$ ]]; then
	rest_of_filename="${BASH_REMATCH[1]}"
	extension="${BASH_REMATCH[2]}"
	#echo  $rest_of_filename $extension
	current_date=$(date +"%Y_%m_%d")
	new_basename="${prefix}_${current_date}${rest_of_filename}.${extension}"
	#echo $new_basename
	echo "gsutil cp $f $destination/${new_basename}"
    else
	echo "Filename does not match the expected pattern (<prefix>_FOO_BAR.extension)"
	exit 1
    fi
done < $filenames
