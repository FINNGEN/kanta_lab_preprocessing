version 1.0

workflow kanta_sort_dup{
  input {
    # works with 100k lines
    Boolean test
    File kanta_data
  }

  call get_cols {}  # metadata
  # split input in chunks
  call split {
    input:
    test = test,
    kanta_data = kanta_data,
    cols = get_cols.cols,
    s_cols = get_cols.s_cols
  }

  # extract columns sort and extract duplicates/errs
  scatter (i in range(length(split.chunks))) {
    call sort {
      input :
      index = i,
      chunk = split.chunks[i],
      sort_cols = split.sort_cols
    }
  }
  # merge chunks (unique/dup/err)
  String prefix = basename(kanta_data,'.txt.gz')
  call merge {
    input :
    sorted_chunks = sort.sorted_chunk,
    sort_cols = split.sort_cols,
    header = split.header,
    prefix = if test then prefix+ "_test"  else prefix 
  }
}

task merge {
  input {
    File header
    Array[File] sorted_chunks
    Array[String] sort_cols
    String prefix
  }

  Int chunk_size = ceil(size(sorted_chunks,"GB"))
  command <<<
  # CONCAT PRE-SORTED FILES
  echo "SORT FILES"
  /usr/bin/time -v sort -t $'\t' -m -k ~{sep=" -k " sort_cols}  ~{sep=" " sorted_chunks}  > sorted.txt
  # REMOVE DUPS
  IFS=',' read -ra cols <<< ~{sep ="," sort_cols}
  prefix="~{prefix}_$(date +"%Y_%m_%d")"
  unique="${prefix}_unique.tsv.gz"
  dups="${prefix}_duplicates.tsv.gz"
  errs="${prefix}_err.tsv.gz"
  echo "$unique"

  header_content="ROW_ID\t$(cat ~{header})"
  echo -e "$header_content" | gzip > "$unique"
  echo -e "$header_content" | gzip > "$dups"
  echo -e "$header_content" | gzip > "$errs"

  
  # Initialize counters
  row_id=0      # Current row number
  count=0       # Count of unique records
  dup_count=0   # Count of duplicate records
  err_count=0   # Count of error records
  prev_key=""   # Previous key for comparison
  
  # Process the sorted input file line by line
  while IFS= read -r line; do
      # Increment row counter for each line
      ((row_id++))
      # Initialize key for comparison and error flag
      key=""
      err=0
      # Split the line into fields based on tab delimiter
      IFS=$'\t' read -ra fields <<< "$line"
      # Extract values from specified columns and build a composite key
      for i in "${cols[@]}"; do
          idx=$((i-1))  # Convert to 0-based index
          if [ $idx -lt ${#fields[@]} ]; then
              # Add field value to the key with a separator
              key+="${fields[$idx]}|"
          else
              # If index is out of bounds, set error flag
              err=1
              break
          fi
      done
      # WRite out to file(s)
      if [ $err -eq 1 ]; then
          # Error case: write to error file
          echo -e "$row_id\t$line" | gzip >> "$errs"
          ((err_count++))
      elif [ "$key" != "$prev_key" ]; then
          # New unique value: write to unique file
          echo -e "$row_id\t$line" | gzip >> "$unique"
          prev_key="$key"  # Update the previous key
          ((count++))
      else
          # Duplicate value: write to duplicates file
          echo -e "$row_id\t$line" | gzip >> "$dups"
          ((dup_count++))
      fi
  done < sorted.txt  # Read from sorted.txt input file
  
  # Output statistics
  echo $count       # Number of unique records
  echo $err_count   # Number of error records
  echo $dup_count   # Number of duplicate records

  # Calculate and output duplication rate (avoiding divide-by-zero)
  total=$((count+dup_count))
  if [ $total -eq 0 ]; then
      echo "0.0000"  # Handle case with no valid records
  else
      # Calculate percentage of duplicates with 4 decimal places
      echo "scale=4; $dup_count/$total" | bc
  fi
  >>>
  runtime {
    disks:   "local-disk ~{chunk_size*4} HDD"
  }
  output {
    Array[File] kanta_files = glob("~{prefix}*gz")
  }
}

task sort {
  input {
    File chunk
    Array[String] sort_cols
    Int index
  }
  String out_file = "kanta_sorted_" + index
  command <<<
  zcat ~{chunk} | sort -t $'\t'  -k ~{sep=" -k " sort_cols}  > ~{out_file} 
  >>>

  runtime {
    disks:   "local-disk ~{ceil(size(chunk,'GB'))*3 + 10} HDD"
  }

  output {
    File sorted_chunk = out_file
  }
}

task get_cols {
  input {String branch}

  command <<<
  # get required columns to cut from git repo
  curl -s https://raw.githubusercontent.com/FINNGEN/kanta_lab_preprocessing/~{branch}/finngen_qc/magic_config.py > config.py
  python3 -c "import config;o= open('./columns.txt','wt') ;o.write('\n'.join(list(config.config['rename_cols'].keys())) + '\n');o.write('\n'.join(config.config['other_cols'])+ '\n')"
  python3 -c "import config;o= open('./sort_columns.txt','wt') ;o.write('\n'.join(config.config['sort_cols'])+ '\n')"
  >>>
  runtime {
    disks:   "local-disk 10 HDD"
  }
  output {
    Array[String] cols = read_lines("columns.txt")
    Array[String] s_cols = read_lines("sort_columns.txt")
  }
}
  
task split {
  input {
    Boolean test
    File kanta_data
    Int n_chunks
    Array[String] cols
    Array[String] s_cols
  }
 
  Int disk_size = ceil(size(kanta_data,"GB"))*10*n_chunks
  
  command <<<
  echo "SORT KANTA"
  cat ~{write_lines(cols)} > columns.txt
  cat ~{write_lines(s_cols)} > sort_columns.txt
  COLS=$(zcat ~{kanta_data} |  head -n1 | tr '\t' '\n'  | grep -wnf columns.txt | cut -f 1 -d ':' | tr '\n' ',' | rev | cut -c2- | rev)
  echo $COLS
  
  # uncompress and split new header from body
  zcat ~{kanta_data} | cut -f $COLS | head -n1  > header.txt
  zcat ~{kanta_data} | cut -f $COLS | sed -E 1d  ~{if test then " | head -n 10000 " else ""}> tmp.tsv
  
  # GET SORT COLS AND KEEP ORDER
  echo "COLS"
  while read f;
  do
      cat header.txt | head -n1 | tr '\t' '\n'|  grep -wn $f |  cut -f 1 -d ':' >> sort_cols.txt
  done <  sort_columns.txt
  cat sort_cols.txt
  
  # SPLIT INTO N FILES
  split tmp.tsv -n l/~{n_chunks} -d kanta_chunk --filter='gzip > $FILE.gz'
  >>>

  runtime {
    disks: "local-disk ~{disk_size} HDD"
  }

  output {
    Array[File] chunks = glob("./kanta_chunk*gz")
    File header = "header.txt"
    Array[String] sort_cols = read_lines("sort_cols.txt")
  }
}

