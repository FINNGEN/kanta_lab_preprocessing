version 1.0

workflow kanta_munge {
  input {
    File kanta_data
    String kanta_docker
    String prefix
    # test mode will use only 100k lines and 4 cpus
    Boolean test
  }

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
      docker = kanta_docker,
      prefix = i,
      chunk = split.chunks[i]
    }
  }
  # MERGE CHUNKS AND LOGS
  String base_prefix = "kanta" +  if test then "_test" else prefix
  call merge_logs {
    input:
    prefix = base_prefix,
    logs = flatten(munge.logs)
  }
  call merge {
    input:
    docker = kanta_docker,
    prefix = base_prefix,
    munged_chunks = munge.munged_chunk
  }
}

task merge {
  input {
    Array[File] munged_chunks
    String prefix
    String docker 

  }
  String out_file = prefix +"_munged.txt.gz"

  command <<<
  # write header
  zcat ~{munged_chunks[0]} | head -n1 | bgzip -c > ~{out_file}
  # merge files without headers
  while read f; do echo $f && date +%Y-%m-%dT%H:%M:%S && zcat $f | sed -E 1d | bgzip -c >> ~{out_file} ; done < <(cat ~{write_lines(munged_chunks)} | sort -V )
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(munged_chunks,'GB')) * 4 + 10} HDD"
    docker : "~{docker}"
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
    String docker
    File chunk
    String prefix
    Int cpus
    String harmonization_branch
  }
  String out_chunk =  "~{prefix}_munged.txt.gz"
  command <<<
  set -euxo pipefail
  python3 /finngen_qc/main.py  --out .  --raw-data ~{chunk} --log info --mp --harmonization --gz --prefix ~{prefix} --harmonization-gh-branch ~{harmonization_branch}
  zcat ~{out_chunk} | wc -l
  >>>
  runtime {
    docker : "~{docker}"
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
  zcat ~{kanta_data} | sed -E 1d ~{if test then " | head -n 4000000 "  else ""} > tmp.tsv
  for f in {00..~{chunks-1}}; do cat header.txt | bgzip -c > kanta$f.gz; done
  split tmp.tsv -n l/~{chunks} -d kanta --filter='gzip >> $FILE.gz'
  >>>
  runtime {disks: "local-disk ~{ceil(size(kanta_data,'GB')) * 10 + 20} HDD"}
  output {
    Array[File] chunks = glob("./kanta*gz")
    File header = "header.txt"
  }
}



