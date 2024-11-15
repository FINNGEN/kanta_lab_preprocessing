version 1.0

workflow kanta_sort_dup{
  input {
    File kanta_data
    String prefix
  }

  call date_prefix {input:base_prefix=prefix}
  call get_cols {}
  call split {
    input:
    kanta_data = kanta_data,
    cols = get_cols.cols,
    s_cols = get_cols.s_cols
  }
  
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
    header = split.header,
    prefix = date_prefix.prefix
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
  String unique = prefix +  "_unique.tsv.gz"
  String dups   = prefix + "_duplicates.tsv.gz"
  String errs   = prefix + "_err.tsv.gz"
  
  command <<<
  # CONCAT PRE-SORTED FILES
  echo "SORT"
  /usr/bin/time -v sort -t $'\t' -m -k ~{sep=" -k " sort_cols}  ~{sep=" " sorted_chunks}  > sorted.txt
  
  python3 <<EOF
  from operator import itemgetter
  import gzip
  # get col indices
  cols = [elem -1 for elem in [~{sep ="," sort_cols}]]
  # initial empty values
  values = ['']*len(cols)
  with open('sorted.txt') as i,gzip.open('~{dups}','wt') as dup,gzip.open('~{unique}','wt') as out,gzip.open('~{errs}','wt') as err:
    # copy header to out files
    with open('~{header}') as tmp: head = tmp.read()
    out.write(head),dup.write(head)
    dup_count,count,err_count = 0,0,0
    for line in i:
      # read in new sort values to compare
      try:
        new_values = itemgetter(*cols)(line.strip().split('\t'))
        if new_values != values: # new value found, so update values and output to unique file   
          values = new_values
          f = out
          count += 1
        else:
          f = dup
          dup_count +=1
      except:
        f = err
        err_count +=1
      f.write(line)
  print(count)
  print(err_count)
  print(dup_count)
  print(round(dup_count/(count+dup_count),4))
  EOF
  >>>
   runtime {
     disks:   "local-disk ~{chunk_size*4} HDD"
  }

  output {
    File unique_file = unique
    File dup_file    = dups
    File err_file    = errs
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
  input {
    String branch
  }
  command <<<
  # get columns to cut from repo
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
    File kanta_data
    Int n_chunks
    Array[String] cols
    Array[String] s_cols
  }
 
  Int disk_size = ceil(size(kanta_data,"GB"))*10*n_chunks
  
  command <<<
  echo "KANTA"

  cat ~{write_lines(cols)} > columns.txt
  cat ~{write_lines(s_cols)} > sort_columns.txt
  
  COLS=$(zcat ~{kanta_data} |  head -n1 | tr '\t' '\n'  | grep -wnf columns.txt | cut -f 1 -d ':' | tr '\n' ',' | rev | cut -c2- | rev)
  echo $COLS
  
  # uncompress and split new header from body
  zcat ~{kanta_data} | cut -f $COLS | head -n1  > header.txt
  zcat ~{kanta_data} | cut -f $COLS | sed -E 1d > tmp.tsv
  
  # GET SORT COLS AND KEEP ORDER
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

task date_prefix {
  input {
    String base_prefix
  }
  command <<<
  echo ~{base_prefix}_$(date +%Y_%m_%d) > tmp.txt
  >>>
  output {
    String prefix = read_string("tmp.txt")
  }
}
