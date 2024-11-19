version 1.0

workflow kanta_munge {
  input {
    File kanta_data
    String prefix
    # test mode will use only 100k lines and 4 cpus
    Boolean test
  }

  # gets prefix with date and test prefix
  String full_prefix = if test then prefix +"_test" else prefix
  call date_prefix {input:base_prefix=full_prefix}
  
  # builds sex dictionary mapping from pheno file
  call sex_map {}
  # splits input in chunks
  call split {
    input:
    test = test,
    kanta_data = kanta_data
  }

  # PROPER MUNGING ACTION
  scatter (i in range(length(split.chunks))) {
    call munge {
      input:
      prefix = i,
      chunk = split.chunks[i],
      sex_map = sex_map.sex_map
    }
  }
  # MERGE CHUNKS AND LOGS
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
    File sex_map
  }
  String out_chunk =  "~{prefix}_munged.txt.gz"
  command <<<
  set -euxo pipefail
  python3 /finngen_qc/main.py  --out .  --raw-data ~{chunk} --log info --mp --harmonization --gz --prefix ~{prefix}
  # ADD SEX
  join --header -t $'\t' -a 1 -o auto -e NA <(zcat ~/fg-3/kanta_v2/munged/kanta_2024_08_26_munged.txt.gz  ) ~{sex_map} | bgzip -c > tmp.txt.gz
  mv tmp.txt.gz ~{out_chunk}
  
  >>>
  runtime {
    docker : "~{kanta_docker}"
    disks: "local-disk ~{ceil(size(chunk,'GB')) * 4 + 10} HDD"
    mem: "~{cpus} GB"
    cpu : "~{cpus}"
  }

  output {
    File munged_chunk = out_chunk
    Array[File] logs = glob("./~{prefix}*txt")
    Array[File] problematic = glob("./*duplicates*gz")
  }
}

task split{
  input {
    File kanta_data
    Int n_chunks
    Boolean test
  }

  Int chunks = if test then  4  else n_chunks
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


task sex_map {
  input {
    File min_pheno
  }

  Int disk_size = ceil(size(min_pheno,"GB")) * 2
  String sex_file = "sex_map.txt"
  command <<<
  zcat ~{min_pheno} | head -n1 > header.txt
  sexcol=$(awk '{for(i=1;i<=NF;i++){if($i=="SEX"){print i; exit}}}' header.txt)
  zcat ~{min_pheno} | cut -f 1,$sexcol | head -n1 > ~{sex_file}
  zcat ~{min_pheno} | cut -f 1,$sexcol | sed -E 1d | sort >> ~{sex_file}
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(min_pheno,'GB')) * 3} HDD"
  }
  output {
    File sex_map = sex_file
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
