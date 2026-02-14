version 1.0

workflow kanta_munge {
  input {
    File kanta_data
    String kanta_docker
    String? analysis_docker
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

  call GetCurrentDate{}
  # MERGE CHUNKS AND LOGS
  String base_prefix = prefix + (if test then "_test" else "") + "_" + GetCurrentDate.date_string
  call merge_logs {
    input:
    prefix = base_prefix,
    logs = flatten(munge.logs)
  }
  call merge {
    input:
    docker = select_first([analysis_docker,kanta_docker]),
    prefix = base_prefix,
    munged_chunks = munge.munged_chunk
  }
  call analysis {
    input:
    docker = select_first([analysis_docker,kanta_docker]),
    merged_file = merge.merged,
    merged_parquet=merge.merged_parquet,
    prefix = base_prefix
  }

}


task analysis {
  input {
    File merged_parquet
    File merged_file
    String prefix
    String docker
  }

  String unmap  = prefix+ "_unmapped_entries.txt"
  String injection =  prefix+ "_candidate_injections.txt"
  String injection_issues = prefix+ "_injection_check.tsv"
  command <<<
  # this step creates the table of most common unit per OMOP_ID
  python3 /qc_scripts/create_harmonization_table.py --input ~{merged_parquet} 
  # this step is similar but in reverse. it checks that the injections led to a KS <.3 for harmonized values
  python3 /qc_scripts/injection_check.py ~{merged_parquet} -o ~{injection_issues}
  # this step creates a candidate injection based on KS values for unharmonized data with source values
  # it also returns the counts of TEST_NAME,UNIT(cleaned) that do not have a mapping
  python3 /qc_scripts/unharmonized.py ~{merged_file}  -a ~{injection} -u ~{unmap}
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(merged_file,'GB')) + 20} HDD"
    docker : "~{docker}"
    memory: "16 GB"
  }

  output {
    File harmonization_counts = "harmonization_counts.tsv"
    File harmonization_diffs = "harmonization_diffs.tsv"
    File umapped_entries = "~{unmap}"
    File injection_candidates = "~{injection}"
    File injection_mismathces =  "~{injection_issues}"
  }
}


task merge {
  input {
    Array[File] munged_chunks
    String prefix
    String docker 
  }
  String parquet_prefix = prefix + "_formatted"
  String out_file = prefix +".txt.gz"

  command <<<
  # Get the exact first line to use as a filter
  FIRST_LINE=$(zcat ~{munged_chunks[0]} | head -n1)
  # Use that string to delete duplicates in the stream
  sort -V ~{write_lines(munged_chunks)} | xargs pigz -dc | sed "1b; /^$FIRST_LINE$/d" | pigz -c > ~{out_file}
  bash /sb_release/run.sh ~{out_file} . ~{parquet_prefix} munged
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(munged_chunks,'GB')) * 4 + 10} HDD"
    docker : "~{docker}"
  }
 
  output {
    File merged = out_file
    File merged_parquet = "~{parquet_prefix}.parquet"
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
    disks: "local-disk ~{ceil(size(chunk,'GB')) * 4 + 8} HDD"
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
  split tmp.tsv -n l/~{chunks} --verbose -d kanta --filter='gzip >> $FILE.gz'
  >>>
  runtime {disks: "local-disk ~{ceil(size(kanta_data,'GB')) * 10 + 20} HDD"}
  output {
    Array[File] chunks = glob("./kanta*gz")
    File header = "header.txt"
  }
}


task GetCurrentDate {
  command <<<
  date +%Y_%m_%d | tr -d '\n'
  >>>
  output {
    String date_string = read_string(stdout())
  }
  meta {
    volatile: true
  }
}
