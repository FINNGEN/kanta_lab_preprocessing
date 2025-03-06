version 1.0

workflow kanta_analysis {
  input {
    String prefix
    String kanta_docker
    # test mode will use only 100k lines and 4 cpus
    Boolean test
  }

  call date_prefix {
    input:
    base_prefix= if test then "kanta_test" else "kanta"
  }
  call analysis {
    input:
    docker = kanta_docker,
    prefix = date_prefix.prefix,
    test = test,
  }
  call release {
    input:
    docker = kanta_docker,
    prefix = prefix,
    analysis_data = analysis.analysis_file
  }
}


task release {

 input {
    String docker
    File analysis_data
    String prefix
    Int mem
  }

  command <<<
  set -euxo pipefail
  /usr/bin/time -v bash /analysis/parquet/run.sh ~{analysis_data} . ~{prefix} 2> tmp.txt
  cat tmp.txt &&  cat tmp.txt | awk '/Maximum resident set size/ {print "Max memory usage (GB):", $6/1024/1024}'
  ls
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

task analysis {
  input {
    String docker
    File kanta_munged_data
    String prefix
    Boolean test
    Int cpus
  }

  command <<<
  set -euxo pipefail
  python3 /analysis/main.py --gz --lines 230000000 --mp --raw-data ~{kanta_munged_data} --prefix ~{prefix} ~{if test then " --test" else ""}
  ls 
  >>>
  runtime {
    docker : "~{docker}"
    disks: "local-disk ~{ceil(size(kanta_munged_data,'GB')) * 3 + 10} HDD"
    mem: "~{cpus} GB"
    cpu : "~{cpus}"
  }

  output {
    File analysis_file = "~{prefix}_analysis.txt.gz"
    Array[File] logs = glob("./~{prefix}*txt")
  }
}


task date_prefix {
  input {String base_prefix}
  command <<<
  echo ~{base_prefix}_$(date +%Y_%m_%d) > tmp.txt
  >>>
  meta {volatile: true}
  output { String prefix = read_string("tmp.txt")}
}
