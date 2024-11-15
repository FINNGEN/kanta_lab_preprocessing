version 1.0

workflow pre_merge {
  input {
    File kanta_list
    String prefix
    String version
  }
  String out_file = sub(prefix,"VERSION",version)
  call pre_merge {
    input:
    kanta_list=kanta_list,
    out_file = out_file,
  }
  

}
task pre_merge {
  
  input {
    File kanta_list
    String out_file
  }

  Array[File] kanta_files = read_lines(kanta_list)
  Int disk = ceil(size(kanta_files,"GB"))*4 + 20

  command <<<
  echo ~{out_file}
  echo "MERGE PART FILES"
  merged_dir="merged"
  mkdir $merged_dir
  # merge partial gz files
  cat ~{write_lines(kanta_files)} > file_list.txt
  current=0
  total=$(wc -l < file_list.txt)
  while read file;
  do
      ((current++))
      echo $current/$total   $(du -sb $file |  awk '{print $1/2^30"GB"}') $(basename $file)
      # Check if the file has multiple parts
      if [[ $file == *".part"* ]]; then
          # Get the base filename without the part number
          base_filename=$(echo "$file" | sed 's/\.part[0-9][0-9]*//')    
          # Concatenate the parts into a single file
          cat "$file" >> "$merged_dir/$(basename "$base_filename")"
      else
          # Copy the single-part file to the merged directory
          cp "$file" "$merged_dir"
      fi
  done  < file_list.txt

  echo "CHECK SIZE"
  for file in $merged_dir/*;
  do
      echo  $(du -sb $file |  awk '{print $1/2^30"GB"}') $(basename $file)
      zcat $file | wc -l
  done

  echo "MERGE ALL"
  ls $merged_dir
  # merge files removing header from other files
  zcat $merged_dir/*gz |  awk 'NR > 1 && /^FINNGENID/ { next } 1'| bgzip -c  > ~{out_file}
  
  >>>
  runtime {
    disks:   "local-disk ~{disk} HDD"
  }

  output {
    File kanta_data = out_file
  }

}

