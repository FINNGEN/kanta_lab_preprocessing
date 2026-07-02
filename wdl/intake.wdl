version 1.0

workflow intake {
  input {
    File source_list_file
    File phenotype_file
    String assemble_docker
    String tidyup_docker
    Int partition_n_buckets = 24
    String prefix = "finngen_R14_kanta_laboratory_responses_internal_1.0"
    Boolean test = false
  }

  Array[Array[File]] all_source_pairs = read_tsv(source_list_file)
  Array[Array[File]] source_pairs = if test then [all_source_pairs[0]] else all_source_pairs

  call assemble {
    input:
      source_pairs = source_pairs,
      docker = assemble_docker,
  }

  call tidyup {
    input:
      assembled_file = assemble.assembled_file,
      phenotype_file = phenotype_file,
      docker = tidyup_docker,
      partition_n_buckets = partition_n_buckets,
      prefix = prefix,
  }

  output {
    File assembled = assemble.assembled_file
    File tidied_parquet = tidyup.tidied_parquet
    File tidied_tsv_gz = tidyup.tidied_tsv_gz
    File tidied_duplicates_parquet = tidyup.tidied_duplicates_parquet
  }
}


task assemble {
  input {
    Array[Array[File]] source_pairs
    String docker
  }

  command <<<
    set -euxo pipefail
    python3 -m kanta.intake.assemble \
      --source-list-file ~{write_tsv(source_pairs)} \
      --output-file assembled.parquet
  >>>

  output {
    File assembled_file = "assembled.parquet"
  }

  runtime {
    docker: docker
    disks: "local-disk ~{ceil(size(flatten(source_pairs), 'GB') * 3)} SSD"
    memory: "8 GB"
    cpu: 4
  }
}


task tidyup {
  input {
    File assembled_file
    File phenotype_file
    String docker
    Int partition_n_buckets
    String prefix
  }

  command <<<
    set -euxo pipefail
    echo "cpus: $(nproc)"
    python3 -m kanta.intake.tidyup \
      --assembled-file ~{assembled_file} \
      --phenotype-file ~{phenotype_file} \
      --output-file ~{prefix}.parquet \
      --partition-n-buckets ~{partition_n_buckets}
    
    clickhouse --query "SELECT * FROM '~{prefix}.parquet'" \
      --format TSVWithNames \
      --max_threads "$(nproc)" \
      --input_format_parquet_preserve_order 1 \
      | pigz > ~{prefix}.txt.gz
  >>>

  output {
    File tidied_parquet = "~{prefix}.parquet"
    File tidied_tsv_gz = "~{prefix}.txt.gz"
    File tidied_duplicates_parquet = "~{prefix}_duplicates.parquet"
  }

  runtime {
    docker: docker
    predefinedMachineType: "n2d-highcpu-32"
    disks: "local-disk ~{ceil(size(assembled_file, 'GB')) * 3} SSD"
  }
}
