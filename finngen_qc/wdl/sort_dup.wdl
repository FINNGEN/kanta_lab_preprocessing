version 1.0

workflow kanta_sort_dup{
  input {
    File kanta_data
  }
  call split {input:kanta_data = kanta_data}

  scatter (i in range(length(split.chunks))) {
    call sort {
      input :
      index = i,
      chunk = split.chunks[i],
      sort_cols = split.sort_cols
    }
  }

  call merge {
    input :
    sorted_chunks = sort.sorted_chunk,
    sort_cols = split.sort_cols,
    header = split.header
  }
  
}

task merge {
  input {
    File header
    Array[File] sorted_chunks
    Array[String] sort_cols
  }

  Int disk_size = ceil(size(sorted_chunks,"GB"))*3
  String prefix = "kanta_sorted"
  
  command <<<
  SORT_COLS=$(cat ~{write_lines(sort_cols)} |   awk '{print "-k "$0}' | tr '\n' ' ')
  echo $SORT_COLS

  # CONCAT PRE-SORTED FILES
  sort -m  $SORT_COLS ~{sep=" " sorted_chunks} > tmp.txt

  # UNIQUE LINES
  cat ~{header} | gzip > ~{prefix}_unique.tsv.gz
  cat tmp.txt | awk '!seen[$~{sep =",$" sort_cols}]++' | gzip >> ~{prefix}_unique.tsv.gz

  # DUPLICATE LINES
  cat ~{header} | gzip > ~{prefix}_duplicates.tsv.gz
  cat tmp.txt | awk 'seen[$~{sep =",$" sort_cols}]++' | gzip >> ~{prefix}_duplicates.tsv.gz

  ls *
  >>>
   runtime {
    disks:   "local-disk ~{disk_size} HDD"
  }

  output {
    File uniuqe = prefix + "_unique.tsv.gz"
    File dups   = prefix + "_duplicates.tsv.gz"
    
  }

}
task sort {
  input {
    File chunk
    Array[String] sort_cols
    Int index
  }
  Int disk_size = ceil(size(chunk,"GB"))*3
  String out_file = "kanta_sorted_" + index
  command <<<
  # get args for sort based off the column indices to sort over
  SORT_COLS=$(cat ~{write_lines(sort_cols)} |   awk '{print "-k "$0}' | tr '\n' ' ')
  echo $SORT_COLS
  zcat ~{chunk} | sort  $SORT_COLS  > ~{out_file} 
  >>>

  runtime {
    disks:   "local-disk ~{disk_size} HDD"
  }

  output {
    File sorted_chunk = out_file
  }
}

task split {
  input {
    File kanta_data
    Int n_chunks
    Array[String] columns
    Array[String] sort_columns
  }

  Int disk_size = ceil(size(kanta_data,"GB"))*10*n_chunks
  
  command <<<
  echo "KANTA"
  # get columns to cut
  COLS=$(zcat ~{kanta_data} |  head -n1 | tr '\t' '\n'  | grep -wnf ~{write_lines(columns)} | cut -f 1 -d ':' | tr '\n' ',' | rev | cut -c2- | rev)
  echo $COLS
  
  # uncompress and split new header from body
  zcat ~{kanta_data} | cut -f $COLS | head -n1  > header.txt
  zcat ~{kanta_data} | cut -f $COLS | sed -E 1d > tmp.tsv
  
  # GET SORT COLS
  cat header.txt | head -n1 | tr '\t' '\n'|  grep -wnf ~{write_lines(sort_columns)}|  cut -f 1 -d ':' > sort_cols.txt
  cat sort_cols.txt
  
  # SPLIT INTO N FILES
  split tmp.tsv -n ~{n_chunks} -d kanta_chunk --filter='gzip > $FILE.gz'
  >>>

  runtime {
    disks: "local-disk ~{disk_size} HDD"
    memory: "32 GB"
  }

  output {
    Array[File] chunks = glob("./kanta_chunk*gz")
    File header = "header.txt"
    Array[String] sort_cols = read_lines("sort_cols.txt")
  }
}
