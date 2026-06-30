version 1.0

workflow kanta_core {
  input {
    String prefix
    String kanta_docker
    String? release_docker
    String? analysis_docker
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
  String base_prefix =  if test then prefix + "_test" else prefix
  # merge chunks & logs
  call merge { input: prefix = base_prefix,munged_chunks = munge.munged_chunk,docker=kanta_docker}
  call merge_logs {input: prefix =  base_prefix,logs = flatten(munge.logs)}
  # build parquet and release file
  call release { input: docker = select_first([release_docker, kanta_docker]), mem = if test then 4 else 64, prefix = prefix, munged_data  = merge.merged_file}
  call validate_outputs {input : parquet_file = release.core_files[1],docker=kanta_docker}
  # CHECKS AND PLOTS
  call compare_versions {input: new_parquet=release.core_files[1],docker=select_first([analysis_docker, kanta_docker]),prefix=prefix}
  # builds updated pos/neg  tables
  call build_pos_tables{input:merged_file = merge.merged_file,docker=select_first([analysis_docker, kanta_docker])}
  # checks extraction automatically
  call qc_extracted {input: core_parquet=release.core_files[1],docker=select_first([analysis_docker, kanta_docker]),prefix=prefix}

}


task qc_extracted {
  input {
    File core_parquet
    String docker
    String prefix
  }

  String dist_summary = prefix + "_extraction_summary.tsv"
  command <<<
  # makes KS comparison between extracted and harmonized data for all OMOP IDs with both type of entries
  python3 /qc_scripts/omop_extracted_dist.py --full  --file_path ~{core_parquet} --summary-file ~{dist_summary}
  >>>
  output {
    File summary  = dist_summary
    Array[File] plots = glob("./plots/*png")
  }
   runtime {
     disks: "local-disk ~{2*ceil(size(core_parquet,'GB')) + 10} HDD"
     docker : "~{docker}"
     memory: "16 GB"
  }
}

task compare_versions {
  input {
    String docker
    String prefix
    File new_parquet
    File old_parquet
  }
  command <<<
  # extracs counts from new release
  python3 /qc_scripts/counts.py ~{new_parquet} -c FINNGENID,MEASUREMENT_VALUE_HARMONIZED,MEASUREMENT_VALUE_EXTRACTED,TEST_OUTCOME,TEST_OUTCOME_TEXT_EXTRACTED,OUTCOME_POS_EXTRACTED -b QC_PASS -o . --prefix ~{prefix}
  # extracs counts from previous release
  python3 /qc_scripts/counts.py ~{old_parquet} -c FINNGENID,MEASUREMENT_VALUE_HARMONIZED,MEASUREMENT_VALUE_EXTRACTED,TEST_OUTCOME,TEST_OUTCOME_TEXT_EXTRACTED,OUTCOME_POS_EXTRACTED -b QC_PASS -o . --prefix old
  # extracts omop names
  python3 -c "import csv, sys; reader = csv.DictReader(sys.stdin); print('conceptId\tconceptName'); [print(f\"{row['conceptId']}\t{row['conceptName']}\") for row in reader]" < /finngen_qc/data/LABfi_ALL.usagi.csv | (sed -u 1q;sort -u) > omop_name_table.tsv
  ls
  # builds summary table and plots comparisons
  python3 /qc_scripts/count_plot.py --new ~{prefix}_omop_analysis.tsv  --old ./old_omop_analysis.tsv --names ./omop_name_table.tsv --new_suffix ~{prefix} --old_suffix ~{basename(old_parquet,'.parquet')} --out_tsv ~{prefix}_omop_comparison.tsv --out_img ~{prefix}_omop_comparison.png
  ls
  >>>
  output {
    File figure = "~{prefix}_omop_comparison.png"
    File comparison = "~{prefix}_omop_comparison.tsv"
    File summary_table = "./~{prefix}_omop_analysis.tsv"
    File summary_md = "./~{prefix}_omop_analysis.md"
    }
  runtime {
    disks: "local-disk ~{2*ceil(size(new_parquet,'GB')) + 10} HDD"
    docker : "~{docker}"
  }
}

task build_pos_tables{
  input {
    String docker
    File merged_file
  }

  command <<<
  python3 /qc_scripts/extract_pos_counts.py ~{merged_file} --map /finngen_qc/data/LABfi_ALL.usagi.csv --pn_orig /core/data/negpos_mapping.tsv --plus_orig /core/data/kanta_plusplus_abnormality.tsv
  >>>
  output{
    File plus_summary = "./plusplus_summary.tsv"
    File posneg_summary = "./pos_neg_summary.tsv"
    Array[File] pasteable = glob("./*pasteable*")
  }
  
  runtime {
    disks: "local-disk ~{ceil(size(merged_file,'GB')) + 10} HDD"
    docker : "~{docker}"
  }
  
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
  String core_prefix = prefix +  "_core"
  String meta_prefix = prefix +  "_extended_columns"
  command <<<
  echo ~{mem}
  set -euxo pipefail
  awk '/^MemTotal:/{print $2/1024/1024}' /proc/meminfo
  /usr/bin/time -v bash /sb_release/run.sh ~{munged_data} . ~{core_prefix} core 2> tmp.txt
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
    Array[File] core_files = ["~{core_prefix}.txt.gz","~{core_prefix}.parquet","~{core_prefix}.log","~{core_prefix}_schema.json"]
    Array[File] meta_files = ["~{meta_prefix}.txt.gz","~{meta_prefix}.parquet","~{meta_prefix}.log","~{meta_prefix}_schema.json"]
  }
}

task merge {
  input {
    Array[File] munged_chunks
    String prefix
    String docker
  }
  String out_file = prefix + ".txt.gz"
  String dup_file = prefix +"_duplicates.txt.gz"
  command <<<
  # write header to reports file
  zcat ~{munged_chunks[0]} | head -n1 | bgzip -c > tmp.txt.gz
  # merge files including reports
  while read f; do echo $f && date +%Y-%m-%dT%H:%M:%S && zcat $f | sed -E 1d | bgzip -c >> tmp.txt.gz ; done < <(cat ~{write_lines(munged_chunks)} | sort -V )

  python3 /core/duplicates.py --input tmp.txt.gz --prefix ~{prefix}

  >>>
  runtime {
    disks: "local-disk ~{ceil(size(munged_chunks,'GB')) * 4 + 10} HDD"
    docker : "~{docker}"
  }
  output {
    File merged_file = out_file
    File duplicates = dup_file
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
  FILE=~{kanta_data}
  TEST=~{if test then 1 else 0}
  CHUNKS=$(( TEST ? 4 : ~{n_chunks} ))
  LINES=$(( TEST ? 4000000 : 250000000 ))

  echo "$([[ $TEST -eq 1 ]] && echo TEST || echo FULL) MODE: CHUNKS=$CHUNKS, LINES=$LINES"
  CHUNK_SIZE=$(( (LINES + CHUNKS - 1) / CHUNKS ))
  echo "Splitting $LINES lines into $CHUNKS chunks of ~$CHUNK_SIZE each"
  gzip -dc "$FILE" | head -n1 > header.txt && echo "Header saved to header.txt"
  cmd="gzip -dc \"$FILE\" | tail -n +2"
  [[ $TEST -eq 1 ]] && cmd+=" | head -n $LINES"
  eval "$cmd" | split -l "$CHUNK_SIZE" -d --verbose   --filter='{ cat header.txt; cat; } | bgzip -c > ${FILE}.gz' - kanta
  >>>

  runtime {
    disks: "local-disk ~{ceil(size(kanta_data,'GB')) * 10 + 20} HDD"
  }

  output {
    Array[File] chunks = glob("./kanta*gz")
    File header = "header.txt"
  }
}
