version 1.0

workflow kanta_analysis {
  input {
    String prefix
    String kanta_docker
    # test mode will use only 100k lines and 4 cpus
    Boolean test
    File kanta_munged_data
  }

  # splits input in chunks
  call split { input:test = test,kanta_data = kanta_munged_data}

  scatter (i in range(length(split.chunks))) {
    call analysis { input: docker = kanta_docker, prefix = i,chunk = split.chunks[i] }
  }

  # get date 
  call date_prefix {input:base_prefix= "kanta_analysis" + if test then "_test" else ""}
  # merge chunks
  call merge { input: prefix =  date_prefix.prefix,analysis_chunks = analysis.analysis_chunk}
  call merge_logs {input: prefix =  date_prefix.prefix,logs = flatten(analysis.logs)}

  call release {
    input:
    docker = kanta_docker,
    mem = if test then 8 else 128,
    prefix = prefix,
    analysis_data  = merge.analysis_file
  }
}

task release {
  input {
    String docker
    File analysis_data
    Int mem
    String prefix
  }
  command <<<
  set -euxo pipefail
  /usr/bin/time -v bash /analysis/parquet/run.sh ~{analysis_data} . ~{prefix} 2> tmp.txt
  cat tmp.txt &&  cat tmp.txt | awk '/Maximum resident set size/ {print "Max memory usage (GB):", $6/1024/1024}'
  >>>
  runtime {
    docker : "~{docker}"
    disks: "local-disk ~{ceil(size(analysis_data,'GB')) * 4 + 10} HDD"
    mem: "~{mem} GB"
    cpu : 8
  }
  output {
    File release_file_gz = "~{prefix}.txt.gz"
    File release_file_pq = "~{prefix}.parquet"
  }
}

task merge {
  input {
    Array[File] analysis_chunks
    String prefix
  }
  String out_file = prefix + ".txt.gz"
  command <<<
  # write header to reports file
  zcat ~{analysis_chunks[0]} | head -n1 | bgzip -c > ~{out_file}
  # merge files including reports
  while read f; do echo $f && date +%Y-%m-%dT%H:%M:%S && zcat $f | sed -E 1d | bgzip -c >> ~{out_file} ; done < <(cat ~{write_lines(analysis_chunks)} | sort -V )
  >>>
  runtime {disks: "local-disk ~{ceil(size(analysis_chunks,'GB')) * 4 + 10} HDD"}
  output {File analysis_file = out_file}
}

task merge_logs {
  input {
    Array[File] logs
    String prefix
  }
  command <<<
  #  merge all warn,abbr,unit files
  cat ~{write_lines(logs)} > logs.txt
  # write headers
  for f in {err,warn} ; do  cat logs.txt | grep $f | head -n1 | xargs head -n1 > ~{prefix}"_"$f".txt"; done
  for f in {err,warn,log} ;do while read i ;do cat $i | sed -E 1d >> ~{prefix}"_"$f".txt"; done < <(cat logs.txt | grep $f | sort -V);done
  >>>
  runtime {disks: "local-disk ~{ceil(size(logs,'GB')) * 4 + 10} HDD"}
  output {
    File out_log  = "~{prefix}_log.txt"
    File out_err  = "~{prefix}_err.txt"
    File out_warn = "~{prefix}_warn.txt"
  }
}

task analysis {
  input {
    String docker
    File chunk
    String prefix
    Int cpus
  }

  command <<<
  set -euxo pipefail
  python3 /analysis/main.py --gz  --mp --raw-data ~{chunk} --prefix ~{prefix} 
  ls 
  >>>
  runtime {
    docker : "~{docker}"
    disks: "local-disk ~{ceil(size(chunk,'GB')) * 3 + 10} HDD"
    mem: "~{cpus} GB"
    cpu : "~{cpus}"
  }
  output {
    File analysis_chunk = "~{prefix}_analysis.txt.gz"
    Array[File] logs = glob("./~{prefix}*txt")
  }
}

task split{
  input {
    File kanta_data
    Int n_chunks
    Boolean test
  }

  Int chunks = if test then 4  else n_chunks
  command <<<
  zcat ~{kanta_data} | head -n1 > header.txt
  zcat ~{kanta_data} | sed -E 1d ~{if test then " | head -n 10000 "  else ""} > tmp.tsv
  for f in {00..~{chunks-1}}; do cat header.txt | bgzip -c > kanta$f.gz; done
  split tmp.tsv -n l/~{chunks} -d kanta --filter='gzip >> $FILE.gz'
  >>>

  runtime {
    disks: "local-disk ~{ceil(size(kanta_data,'GB')) * 10 + 20} HDD"
  }

  output {
    Array[File] chunks = glob("./kanta*gz")
    File header = "header.txt"
  }
}

task date_prefix {
  input {
    String base_prefix
  }
  command <<<
  echo ~{base_prefix}_$(date +%Y_%m_%d) > tmp.txt
  >>>
  meta {
    volatile: true
  }
  output {
    String prefix = read_string("tmp.txt")
  }
}
