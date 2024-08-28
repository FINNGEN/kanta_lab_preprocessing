version 1.0

workflow kanta_munge {
  input {
    File kanta_data
    String prefix
  }

  call split {
    input:
    kanta_data = kanta_data
  }

  call date_prefix {input:base_prefix=prefix}
  scatter (i in range(length(split.chunks))) {
    call munge {
      input:
      prefix = i,
      chunk = split.chunks[i]
    }
  }
  call merge {
    input:
    prefix = date_prefix.prefix,
    munged_chunks = munge.munged_chunk
  }
  call merge_logs {
    input:
    prefix = date_prefix.prefix,
    logs = flatten(munge.logs)
  }
}


task merge {
  input {
    Array[File] munged_chunks
    String prefix
  }
  String out_file = prefix +"_munged.txt.gz"
  command <<<
  zcat ~{munged_chunks[0]} | head -n1 | bgzip -c > ~{out_file}
  while read f; do echo $f && date +%Y-%m-%dT%H:%M:%S && zcat $f | sed -E 1d | bgzip -c >> ~{out_file} ; done < <(cat ~{write_lines(munged_chunks)} | sort -V )
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(munged_chunks,'GB')) * 4 + 10} HDD"
  }

  output {
    File munged = out_file
  }
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
  for f in {err,warn,abbr,unit} ; do  cat logs.txt | grep $f | head -n1 | xargs head -n1 > ~{prefix}"_"$f".txt"; done
  for f in {err,warn,abbr,unit,log} ;do while read i ;do cat $i | sed -E 1d >> ~{prefix}"_"$f".txt"; done < <(cat logs.txt | grep $f | sort -V);done
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(logs,'GB')) * 4 + 10} HDD"
  }

  output {
    File out_log  = "~{prefix}_log.txt"
    File out_err  = "~{prefix}_err.txt"
    File out_abbr = "~{prefix}_abbr.txt"
    File out_unit = "~{prefix}_unit.txt"
    File out_warn = "~{prefix}_warn.txt"
  }
}

task munge {
  input {
    String kanta_docker
    File chunk
    String prefix
    Int cpus
  }
  command <<<
  set -euxo pipefail
  python3 /finngen_qc/main.py  --out .  --raw-data ~{chunk} --log info --mp --harmonization --gz --prefix ~{prefix}
  >>>
  runtime {
    docker : "~{kanta_docker}"
    disks: "local-disk ~{ceil(size(chunk,'GB')) * 4 + 10} HDD"
    mem: "~{cpus} GB"
    cpu : "~{cpus}"
  }

  output {
    File munged_chunk = "~{prefix}_munged.txt.gz"
    Array[File] logs = glob("./~{prefix}*txt")
    Array[File] problematic = glob("./*duplicates*gz")
  }
}

task split{
  input {
    File kanta_data
    Int n_chunks
  }

  command <<<
  zcat ~{kanta_data} | head -n1 > header.txt
  zcat ~{kanta_data} | sed -E 1d > tmp.tsv
  for f in {00..~{n_chunks-1}}; do cat header.txt | bgzip -c > kanta$f.gz; done
  split tmp.tsv -n l/~{n_chunks} -d kanta --filter='gzip >> $FILE.gz'
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
  output {
    String prefix = read_string("tmp.txt")
  }
}
