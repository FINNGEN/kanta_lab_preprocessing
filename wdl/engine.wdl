version 1.0

workflow engine {
  input {
    File tidied_parquet
    String docker
    String prefix = "finngen_R14_kanta_laboratory_responses_1.0"
    Boolean test = false
    Int chunk_size = 200000
    String injection_branch = "injection-dev"
  }

  call run_engine {
    input:
    tidied_parquet = tidied_parquet,
    docker = docker,
    prefix = prefix,
    test = test,
    chunk_size = chunk_size,
    injection_branch = injection_branch
  }

  # QC tables built from this run's output, for curating the engine's reference data
  call abnormality {
    input:
    release_parquet = run_engine.release_parquet,
    docker = docker
  }

  call pos_tables {
    input:
    engine_parquet = run_engine.engine_parquet,
    docker = docker
  }

  output {
    File engine_parquet = run_engine.engine_parquet
    File errors_parquet = run_engine.errors_parquet
    File abbr_parquet = run_engine.abbr_parquet
    File unit_parquet = run_engine.unit_parquet
    File release_parquet = run_engine.release_parquet
    File log = run_engine.log
    File abnormality_table = abnormality.table
    File abnormality_ranges = abnormality.ranges
    File pos_neg_summary = pos_tables.pos_neg_summary
    File pos_neg_summary_pasteable = pos_tables.pos_neg_summary_pasteable
    File plusplus_summary = pos_tables.plusplus_summary
    File plusplus_summary_pasteable = pos_tables.plusplus_summary_pasteable
  }
}


task run_engine {
  input {
    File tidied_parquet
    String docker
    String prefix
    Boolean test
    Int chunk_size
    String injection_branch
  }

  command <<<
    set -euxo pipefail
    echo "cpus: $(nproc)"
    # The engine writes its chunk files under tempfile.mkdtemp(), i.e. /tmp by default,
    # which lives on the boot disk in Cromwell. Point it at the sized local disk instead.
    mkdir -p tmp
    export TMPDIR="$(pwd)/tmp"
    python3 -m kanta.engine \
      --input-file ~{tidied_parquet} \
      --output-prefix ~{prefix} \
      --chunk-size ~{chunk_size} \
      --injection-branch ~{injection_branch} \
      ~{if test then "--test" else ""}
  >>>

  output {
    File engine_parquet = "~{prefix}.parquet"
    File errors_parquet = "~{prefix}_errors.parquet"
    File abbr_parquet = "~{prefix}_abbr.parquet"
    File unit_parquet = "~{prefix}_unit.parquet"
    File release_parquet = "~{prefix}_RELEASE.parquet"
    File log = "~{prefix}.log"
  }

  runtime {
    docker: docker
    cpu: 16
    memory: "32 GB"
    disks: "local-disk ~{ceil(size(tidied_parquet, 'GB') * 10) + 50} SSD"
  }
}


task abnormality {
  input {
    File release_parquet
    String docker
  }

  command <<<
    set -euxo pipefail
    # Per-OMOP_ID abnormality limits (engine's abnormality_estimation.table.tsv), estimated
    # from TEST_OUTCOME on QC_PASS > 0 rows. DuckDB spills to the working directory.
    python3 /scripts/qc_scripts/abnormality.py --parquet_file ~{release_parquet}
  >>>

  output {
    File table = "abnormality_estimation.table.tsv"
    File ranges = "abnormality_estimation.txt"
  }

  runtime {
    docker: docker
    cpu: 8
    memory: "32 GB"
    disks: "local-disk ~{ceil(size(release_parquet, 'GB') * 3) + 20} SSD"
  }
}


task pos_tables {
  input {
    File engine_parquet
    String docker
  }

  command <<<
    set -euxo pipefail
    # Free-text pos/neg and "+" summaries vs. the engine's negpos_mapping.tsv /
    # kanta_plusplus_abnormality.tsv. Needs the main output: _RELEASE has no free text.
    python3 /scripts/qc_scripts/extract_pos_counts_parquet.py ~{engine_parquet}
  >>>

  output {
    File pos_neg_summary = "pos_neg_summary.tsv"
    File pos_neg_summary_pasteable = "pos_neg_summary_pasteable.tsv"
    File plusplus_summary = "plusplus_summary.tsv"
    File plusplus_summary_pasteable = "plusplus_summary_pasteable.tsv"
  }

  runtime {
    docker: docker
    cpu: 8
    memory: "32 GB"
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 2) + 20} SSD"
  }
}
