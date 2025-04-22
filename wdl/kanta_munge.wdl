version 1.0

workflow kanta_munge {
  input {
    File kanta_data
    String prefix
    String kanta_docker
    # test mode will use only 100k lines and 4 cpus
    Boolean test
  }

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
      docker = kanta_docker,
      prefix = i,
      chunk = split.chunks[i],
      sex_map = sex_map.sex_map
    }
  }
  # MERGE CHUNKS AND LOGS
  String base_prefix = "kanta" +  if test then "_test" else ""
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
  # DOUBLE CHECK THAT WE ARE WORKING ONLY WITH SAMPLES IN INCLUSION LIST
  call validate_outputs {input : parquet_file = release.release_file_pq,docker=kanta_docker}
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



task merge {
  input {
    Array[File] munged_chunks
    String prefix
    Array[String] reports_columns
    String docker 

  }
  String out_file = prefix +"_munged.txt.gz"
  String dup_file = prefix +"_munged_duplicates.txt.gz"
  String reports_file = prefix +"_munged_reports.txt.gz"

  command <<<
  # write header to reports file
  zcat ~{munged_chunks[0]} | head -n1 | bgzip -c > tmp.gz

  # merge files including reports
  while read f; do echo $f && date +%Y-%m-%dT%H:%M:%S && zcat $f | sed -E 1d | bgzip -c >> tmp.gz ; done < <(cat ~{write_lines(munged_chunks)} | sort -V )

  # remove duplicates
  python3 /finngen_qc/duplicates.py --input tmp.gz --prefix ~{prefix}_munged_reports

  # EXTRACT REPORTS COLUMNS
  COLS=$(zcat ~{reports_file} | head -n1 | tr '\t' '\n' | grep -wnf ~{write_lines(reports_columns)}  | cut -d : -f1 | tr '\n' ',' | sed 's/,$//')
  echo $COLS
  zcat ~{reports_file} | cut -f $COLS --complement | bgzip -c > ~{out_file}
  zcat  ~{prefix}_munged_reports_duplicates.txt.gz | cut -f $COLS --complement | bgzip -c > ~{dup_file}
  
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(munged_chunks,'GB')) * 4 + 10} HDD"
    docker : "~{docker}"

  }
 
  output {
    File munged_all = reports_file
    File munged = out_file
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
    File sex_map
  }
  String out_chunk =  "~{prefix}_munged.txt.gz"
  command <<<
  set -euxo pipefail
  python3 /finngen_qc/main.py  --out .  --raw-data ~{chunk} --log info --mp --harmonization --gz --prefix ~{prefix}
  # MERGE WITH SEX EXCLUDING SAMPLES NOT IN SEX MAP (AND THUS IN INCLUSION LIST)
  join --header -t $'\t' -o auto -e NA <(zcat ~{out_chunk}  ) ~{sex_map} | bgzip -c > tmp.txt.gz
  zcat tmp.txt.gz | wc -l  &&  zcat ~{out_chunk} | wc -l
  mv tmp.txt.gz ~{out_chunk}
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

task sex_map {
  input {File min_pheno}
  String sex_file = "sex_map.txt"
  command <<<
  # get sex col
  sexcol=$(awk '{for(i=1;i<=NF;i++){if($i=="SEX"){print i; exit}}}' <(zcat ~{min_pheno} | head -n1))
  # extract sex only and sort
  zcat ~{min_pheno} | cut -f 1,$sexcol | (sed -u 1q ; sort )>> ~{sex_file}
  >>>
  runtime {disks: "local-disk ~{ceil(size(min_pheno,'GB')) * 3} HDD"}
  output {File sex_map = sex_file}
}

