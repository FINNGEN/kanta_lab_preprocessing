version 1.0

workflow intake {
  input {
    File source_list_file
    File phenotype_file
    File? include_ids_file
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
    prefix = prefix
  }
  
  call tidyup {
    input:
    assembled_file = assemble.assembled_file,
    phenotype_file = phenotype_file,
    include_ids_file = include_ids_file,
    docker = tidyup_docker,
    partition_n_buckets = partition_n_buckets,
    prefix = prefix
  }

  output {
    File assembled = assemble.assembled_file
    File tidied_parquet = tidyup.tidied_parquet
    File tidied_duplicates_parquet = tidyup.tidied_duplicates_parquet
    File? include_filter_summary = tidyup.include_filter_summary
    File oid_map = tidyup.oid_map
  }
}


task assemble {
  input {
    Array[Array[File]] source_pairs
    String docker
    String prefix
  }

  command <<<
  set -euxo pipefail
    python3 -m kanta.intake.assemble \
      --source-list-file ~{write_tsv(source_pairs)} \
      --output-file ~{prefix}_assembled.parquet
  >>>

  output {
    File assembled_file = "~{prefix}_assembled.parquet"
  }

  runtime {
    docker: docker
    disks: "local-disk ~{ceil(size(flatten(source_pairs), 'GB') * 3)} SSD"
  }
}


task tidyup {
  input {
    File assembled_file
    File phenotype_file
    File? include_ids_file
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
      --partition-n-buckets ~{partition_n_buckets} \
      ~{"--include-ids-file " + include_ids_file}

    # ROWID -> record OID bridge to the statement data (scripts/statements/README.md). The lookup-array cache stays in the
    # task's working dir, it isn't an output.
    python3 /scripts/statements/oid_map.py \
      --assembled ~{assembled_file} \
      --tidyup ~{prefix}.parquet \
      --output ~{prefix}_oid_map.parquet
  >>>

  output {
    File tidied_parquet = "~{prefix}.parquet"
    File tidied_duplicates_parquet = "~{prefix}_duplicates.parquet"
    File? include_filter_summary = "~{prefix}_include_filter.tsv"
    File oid_map = "~{prefix}_oid_map.parquet"
  }

  runtime {
    docker: docker
    predefinedMachineType: "n2d-highcpu-32"
    # + 4 GB for the OID step's lookup array (~1.2 GB for 300M rows); its output is smaller than the assembled file's
    # OID columns, and tidyup's own temporary files are gone by then.
    disks: "local-disk ~{ceil(size(assembled_file, 'GB')) * 3 + 5} SSD"
  }
}
