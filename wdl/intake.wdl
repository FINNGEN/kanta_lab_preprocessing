version 1.0

workflow intake {
  input {
    File source_list_file
    File phenotype_file
    String assemble_docker
    String tidyup_docker
    Int partition_n_buckets = 24
    String prefix = "finngen_R14_kanta_laboratory_responses_internal_1.0"
  }

  call assemble {
    input:
      source_list_file = source_list_file,
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
    File tidied = tidyup.tidied_file
  }
}


task assemble {
  input {
    File source_list_file
    String docker
  }

  command <<<
    set -euxo pipefail
    sed 's|gs://[^/]*/|/mnt/disks/gcs/|g' ~{source_list_file} > source_list_fuse.txt
    python3 -m kanta.intake.assemble \
      --source-list-file source_list_fuse.txt \
      --output-file assembled.parquet
  >>>

  output {
    File assembled_file = "assembled.parquet"
  }

  runtime {
    docker: docker
    disks: "local-disk 100 HDD"
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
    python3 -m kanta.intake.tidyup \
      --assembled-file ~{assembled_file} \
      --phenotype-file ~{phenotype_file} \
      --output-file ~{prefix}.parquet \
      --partition-n-buckets ~{partition_n_buckets}
  >>>

  output {
    File tidied_file = "~{prefix}.parquet"
  }

  runtime {
    docker: docker
    disks: "local-disk ~{ceil(size(assembled_file, 'GB')) * 3 + 20} HDD"
    memory: "32 GB"
    cpu: 32
  }
}
