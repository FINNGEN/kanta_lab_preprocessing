version 1.0

workflow kanta_core {
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
    call munge {
      input: docker = kanta_docker, prefix = i,chunk = split.chunks[i]
    }
  }
  String base_prefix =  "kanta_core_test" + if test then "_test" else ""
  # merge chunks & logs
  call merge { input: prefix = base_prefix,munged_chunks = munge.munged_chunk}
  call merge_logs {input: prefix =  base_prefix,logs = flatten(munge.logs)}
  # build parquet and release file
  call release { input: docker = kanta_docker, mem = if test then 4 else 64, prefix = prefix, munged_data  = merge.merged_file}
  # DOUBLE CHECK THAT WE ARE WORKING ONLY WITH SAMPLES IN INCLUSION LIST
  call validate_outputs {input : parquet_file = release.core_files[1],docker=kanta_docker}
}

task validate_outputs {
  input {
    String docker
    File parquet_file
    File inclusion_list
  }
  command <<<
  RELEASE_SAMPLES='./release_samples.txt'
  INCLUSION_SAMPLES='./inclusion_samples.txt'

  # Check if RELEASE_SAMPLES exists
  clickhouse --query="SELECT DISTINCT FINNGENID FROM file('~{parquet_file}', Parquet) ORDER BY FINNGENID " | sort  > $RELEASE_SAMPLES
  echo "N samples in release file: $(wc -l "$RELEASE_SAMPLES" | awk '{print $1}')"

  #Inclusion list
  zcat -f ~{inclusion_list} | sort | uniq > "$INCLUSION_SAMPLES"
  echo "N samples in inclusion file: $(wc -l "$INCLUSION_SAMPLES" | awk '{print $1}')"

  # Calculate EXTRA_SAMPLES regardless of file creation
  comm -23 "$RELEASE_SAMPLES" "$INCLUSION_SAMPLES" > extra_samples.txt  
  EXTRA_SAMPLES=$(cat extra_samples.txt | wc -l)
  echo "Release samples that are not in exclusion list: $EXTRA_SAMPLES"
  
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(parquet_file,'GB')) + 10} HDD"
    docker : "~{docker}"
  }
  output{ File extra_samples = "./extra_samples.txt"}
}

task release {
  input {
    String docker
    File munged_data
    Int mem
    String prefix
  }
  String meta_prefix = prefix +  "_metadata_columns"
  command <<<
  echo ~{mem}
  set -euxo pipefail
  awk '/^MemTotal:/{print $2/1024/1024}' /proc/meminfo
  /usr/bin/time -v bash /sb_release/run.sh ~{munged_data} . ~{prefix} core 2> tmp.txt
  cat tmp.txt &&  cat tmp.txt | awk '/Maximum resident set size/ {print "Max memory usage (GB):", $6/1024/1024}'
  /usr/bin/time -v bash /sb_release/run.sh ~{munged_data} . ~{meta_prefix} metadata 2> tmp.txt
  cat tmp.txt &&  cat tmp.txt | awk '/Maximum resident set size/ {print "Max memory usage (GB):", $6/1024/1024}'
  
  >>>
  runtime {
    docker : "~{docker}"
    disks: "local-disk ~{ceil(size(munged_data,'GB')) * 4 + 10} HDD"
    memory: "~{mem} GB"
    cpu : mem/4
  }
  output {
    Array[File] core_files = ["~{prefix}.txt.gz","~{prefix}.parquet","~{prefix}.log","~{prefix}_schema.json"]
    Array[File] meta_files = ["~{meta_prefix}.txt.gz","~{meta_prefix}.parquet","~{meta_prefix}.log","~{meta_prefix}_schema.json"]
  }
}

task merge {
  input {
    Array[File] munged_chunks
    String prefix
  }
  String out_file = prefix + ".txt.gz"
  command <<<
  # write header to reports file
  zcat ~{munged_chunks[0]} | head -n1 | bgzip -c > ~{out_file}
  # merge files including reports
  while read f; do echo $f && date +%Y-%m-%dT%H:%M:%S && zcat $f | sed -E 1d | bgzip -c >> ~{out_file} ; done < <(cat ~{write_lines(munged_chunks)} | sort -V )
  >>>
  runtime {disks: "local-disk ~{ceil(size(munged_chunks,'GB')) * 4 + 10} HDD"}
  output {File merged_file = out_file}
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

task munge {
  input {
    String docker
    File chunk
    String prefix
    Int cpus
  }

  command <<<
  set -euxo pipefail
  python3 /core/main.py --gz  --mp --raw-data ~{chunk} --prefix ~{prefix} 
  ls 
  >>>
  runtime {
    docker : "~{docker}"
    disks: "local-disk ~{ceil(size(chunk,'GB')) * 3 + 10} HDD"
    mem: "~{cpus} GB"
    cpu : "~{cpus}"
  }
  output {
    File munged_chunk = "~{prefix}.txt.gz"
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
