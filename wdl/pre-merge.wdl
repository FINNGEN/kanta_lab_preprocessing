version 1.0

workflow pre_merge {
  input {
    Boolean test
    File kanta_list
    File reports
    String sort_merge_cols
    String prefix
    String version
  }
  ############
  #--REPORTS--##
  ############
  # sort report file based on new multi column key
  call sort_file as sort_report {
    input:
    input_file = reports,
    sort_merge_cols = sort_merge_cols,
    test = false # i want to use all reports
  }
  # merge the reports file where ids match so free text is just one column
  call join_reports { input:reports= sort_report.sorted_file }

  ##############
  #--RESPONSES--##
  ##############
  # merge the various chunks into single files for each year
  call merge_responses {input:kanta_list=kanta_list}
  scatter (responses_file in merge_responses.merged_responses) {
    # sort it as in the other task
    call sort_file as sort_responses {
      input :
      test = test, # work with 10k lines only
      input_file= responses_file,
      sort_merge_cols = sort_merge_cols,
      out_cols = merge_responses.responses_cols
    }
    # join each response with the pre sorted reports file
    call merge_reports_responses {input:reports =join_reports.joined_reports ,responses = sort_responses.sorted_file}
  }
  #########$###
  #--MERGING--##
  #############
  call merge_files {input:rr_files = merge_reports_responses.merged_file,out_file = sub(prefix,"VERSION",if test then version +"_test" else version) }
}


task join_reports{
  input {
    File reports
  }
  String out_file = sub(basename(reports),".txt.gz","_joined_ids.txt.gz")
  command <<<
  # Process the file
  zcat -f ~{reports} | awk '
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
        seen_key=1
    }
  }
  END {
    # Output last key
    split(prev_key, cols, SUBSEP)
    prefix = seen_key > 1 ? "HUOM::" seen_key "::" : ""
    print cols[1], cols[2], cols[3], prefix prev_value
  }' | bgzip -c  > ~{out_file}

  zcat ~{reports} | wc -l
  zcat ~{out_file} | wc -l 
  >>>
  runtime { disks:   "local-disk ~{ceil(size(reports,'GB')*3) + 10} HDD" }
  output {
    File joined_reports = out_file
  }
} 

task merge_files {
  input {
    Array[File] rr_files
    String out_file
  }
  command <<<
  zcat ~{rr_files[0]} | head -n1 | bgzip -c > ~{out_file}
  while read f; do  echo $f &&  zcat $f | sed -E 1d | bgzip -c >> ~{out_file}; done < ~{write_lines(rr_files)}
  wc -l ~{out_file}
  >>>
  runtime {disks:   "local-disk ~{ceil(size(rr_files,'GB'))*3 + 10} HDD"}
  output { File merged_file = out_file}
}

task merge_reports_responses {
  input {
    File reports
    File responses
  }
  String out_file = sub(basename(responses),'responses','responses_reports')
  command <<<
  echo ~{out_file}
  zcat ~{responses} | wc -l
  join -t $'\t' -a 1 -o auto -e NA --header <(zcat ~{responses}) <(zcat  ~{reports} ) |  cut -f 2- | bgzip -c > ~{out_file}
  wc -l ~{out_file}
  >>>
  runtime {
    disks:"local-disk ~{ceil(size(responses,'GB') + size(reports,'GB')*3) + 2} HDD"
  }
  output {
    File merged_file = out_file
  }
}

task sort_file {
  input {
    Boolean test
    File input_file
    String sort_merge_cols
    String out_cols
  }
  String out_file =   basename(input_file,".txt.gz") + "_sorted.txt.gz"
  command <<<
  echo ~{input_file} ~{out_file}
  # MERGE COLS
  echo ~{sort_merge_cols}
  merge_cols=$(zcat -f ~{input_file} | head -n1 | tr '\t' '\n' | nl | grep -wf <(echo ~{sort_merge_cols} | tr ',' '\n')  | awk '{print $1}' | tr '\n' ','  | rev | cut -c 2- | rev)
  echo $merge_cols
  # OUT_COLS
  echo ~{out_cols}
  out_cols=$(zcat -f ~{input_file} | head -n1 | tr '\t' '\n' | nl | grep -wf <(echo ~{out_cols} | tr ',' '\n')  | awk '{print $1}' | tr '\n' ','  | rev | cut -c 2- | rev)
  echo $out_cols
  # OUT FILE
  echo "MERGE COLUMNS"
  CMD="paste <(zcat ~{input_file} | cut -f $merge_cols | tr '\t' '-') <(zcat ~{input_file}  | cut -f $out_cols  ) ~{if test then ' | head -n 10000 '  else ''} > tmp.txt" 
  echo $CMD
  eval $CMD
  echo "SORT ID COLUMNS"
  head -n1 tmp.txt |bgzip -c > ~{out_file}
  cat tmp.txt| sed -E 1d | sort  | bgzip -c >> ~{out_file}
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(input_file,'GB'))*10 + 2} HDD"
  }
  output {
    File sorted_file = out_file
  }
}

task merge_responses {
  
  input {
    File kanta_list
  }

  Array[File] kanta_files = read_lines(kanta_list)
  Int disk = ceil(size(kanta_files,"GB"))*4 + 20

  command <<<
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
      zcat $file | head -n1 | tr '\t' ',' > header.txt
  done
  cat header.txt
  >>>
  runtime {
    disks:   "local-disk ~{disk} HDD"
  }

  output {
    Array[File] merged_responses = glob("merged/*gz")
    String responses_cols = read_string("header.txt")
  }

}

