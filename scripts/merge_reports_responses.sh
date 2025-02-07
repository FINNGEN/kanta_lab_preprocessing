#!/bin/bash

input_file="$1"
merge_columns="$2"
out_file="$3"


DIR=$(dirname $input_file)
temp_file=$DIR/tmp.txt
merge_cols=$(zcat -f $input_file | head -n1 | tr '\t' '\n' | nl | grep -wf <(echo $merge_columns | tr ',' '\n')  | awk '{print $1}' | tr '\n' ','  | rev | cut -c 2- | rev)
echo $merge_cols

if [ $# == 4 ]; then
    extract_columns="$4"
    out_cols=$(zcat -f $input_file | head -n1 | tr '\t' '\n' | nl | grep -wf <(echo $extract_columns | tr ',' '\n')  | awk '{print $1}' | tr '\n' ','  | rev | cut -c 2- | rev)
    echo $out_cols
else
    out_cols=$(zcat -f $input_file | head -n1 | tr '\t' '\n' | nl | awk '{print $1}' | tr '\n' ','  | rev | cut -c 2- | rev)
    
fi

paste <(zcat $input_file | cut -f $merge_cols | tr '\t' '-') <(zcat $input_file | cut -f $out_cols ) > $temp_file
head -n1 $temp_file |bgzip -c > $out_file
sed -E 1d $temp_file | sort  | bgzip -c >> $out_file

rm $temp_file

