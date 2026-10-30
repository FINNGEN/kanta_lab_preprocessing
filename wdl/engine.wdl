version 1.0

workflow engine {
  input {
    File tidied_parquet
    # Separate images so a rebuild for the downstream tasks doesn't invalidate run_engine's call
    # cache (the docker image is part of the cache key): keep engine_docker (and the other
    # run_engine inputs, incl. prefix) unchanged and only the downstream tasks rerun.
    # qc_docker defaults to engine_docker.
    String engine_docker
    String? qc_docker
    String prefix = "finngen_R14_kanta_laboratory_responses_1.0"
    Boolean test = false
    Int chunk_size = 200000
    String injection_branch = "master"
    # an earlier release (its _RELEASE.parquet) to compare this run's per-OMOP counts with
    File old_release_parquet
  }

  String downstream_docker = select_first([qc_docker, engine_docker])

  call run_engine {
    input:
    tidied_parquet = tidied_parquet,
    docker = engine_docker,
    prefix = prefix,
    test = test,
    chunk_size = chunk_size,
    injection_branch = injection_branch
  }

  # QC tables built from this run's output, for curating the engine's reference data
  call abnormality {
    input:
    release_parquet = run_engine.release_parquet,
    docker = downstream_docker
  }

  call pos_tables {
    input:
    engine_parquet = run_engine.engine_parquet,
    docker = downstream_docker
  }

  # Reference-range texts -> numeric limits, checked against the rows; LIMITS feeds the next run's
  # reference extraction (approved + final limits), UNDECIDED (still to decide on) and REJECTED (out for good) split the pairs not approved
  call ref_limits {
    input:
    engine_parquet = run_engine.engine_parquet,
    docker = downstream_docker
  }

  # Nightingale metabolomics panel + risk scores, unpacked from free text into their own tables
  call metabolomics {
    input:
    engine_parquet = run_engine.engine_parquet,
    docker = downstream_docker
  }

  # Per-OMOP counts of this release and of an earlier one, and how they changed (incl. the rows with a reference limit)
  call release_counts {
    input:
    release_parquet = run_engine.release_parquet,
    old_release_parquet = old_release_parquet,
    docker = downstream_docker
  }

  # Javier's summary tables per test name x unit (for the OMOP mapping), every row > 5 people
  call summaries {
    input:
    engine_parquet = run_engine.engine_parquet,
    docker = downstream_docker
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
    File ref_text_table = ref_limits.table
    File ref_text_limits = ref_limits.limits
    File ref_text_undecided = ref_limits.undecided
    File ref_text_rejected = ref_limits.rejected
    File metabolomics_biomarkers = metabolomics.biomarkers
    File metabolomics_risks = metabolomics.risks
    File counts_new = release_counts.counts_new
    File counts_old = release_counts.counts_old
    File counts_change = release_counts.change
    File counts_change_plot = release_counts.change_plot
    File counts_appeared = release_counts.appeared
    File counts_disappeared = release_counts.disappeared
    File summary_test = summaries.test
    File summary_unit_source = summaries.unit_source
    File summary_values_source = summaries.values_source
    File summary_values = summaries.values
    File summary_outcomes = summaries.outcomes
    File summary_reference = summaries.reference
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
    python3 /scripts/abnormality/abnormality.py --parquet_file ~{release_parquet}
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
    python3 /scripts/posneg/extract_pos_counts_parquet.py ~{engine_parquet}
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


task ref_limits {
  input {
    File engine_parquet
    String docker
  }

  command <<<
    set -euxo pipefail
    # Rows per test name x reference text, the limits parsed from each text, and the value/sex/age checks.
    # Step 1 is one duckdb pass; its cache and spill files go to ./tmp on the local disk.
    python3 /scripts/ref_ranges/ref_text_limits.py --input ~{engine_parquet} --out . --tmp tmp
  >>>

  output {
    File table = "ref_text_table.tsv"
    File limits = "ref_text_table_LIMITS.tsv"
    File undecided = "ref_text_table_UNDECIDED.tsv"
    File rejected = "ref_text_table_REJECTED.tsv"
  }

  runtime {
    docker: docker
    cpu: 8
    memory: "32 GB"
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 2) + 20} SSD"
  }
}


task metabolomics {
  input {
    File engine_parquet
    String docker
  }

  command <<<
    set -euxo pipefail
    # Nightingale panel (S-NGMuut) and the 8 disease-risk scores, parsed out of free text.
    # Needs the main output: _RELEASE has no free text. Single-threaded, streams in batches.
    python3 /scripts/metabolomics/unpack_metabolomics.py --input ~{engine_parquet} --out .
  >>>

  output {
    File biomarkers = "nightingale_results_metabolomics.tsv.gz"
    File risks = "nightingale_results_risks.tsv.gz"
  }

  runtime {
    docker: docker
    cpu: 2
    memory: "16 GB"
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 3 / 2) + 20} SSD"
  }
}


task release_counts {
  input {
    File release_parquet
    File old_release_parquet
    String docker
  }

  command <<<
    set -euxo pipefail
    # Per OMOP_CONCEPT_ID for both releases: non-missing values, failed QC (QC_PASS 0), rows with a lower or upper
    # reference limit (count_plot.py counts the parquets first, with counts.py); then the change per column (new / old)
    export MPLCONFIGDIR="$(pwd)/mpl"
    python3 /scripts/counts/count_plot.py --new ~{release_parquet} --old ~{old_release_parquet}
  >>>

  output {
    File counts_new = "new_~{basename(release_parquet, '.parquet')}_omop_analysis.tsv"
    File counts_old = "old_~{basename(old_release_parquet, '.parquet')}_omop_analysis.tsv"
    File change = "relative_change.tsv"
    File change_plot = "relative_change_histograms.png"
    File appeared = "appeared_omop_ids.tsv"
    File disappeared = "disappeared_omop_ids.tsv"
  }

  runtime {
    docker: docker
    cpu: 8
    memory: "32 GB"
    disks: "local-disk ~{ceil(size(release_parquet, 'GB') + size(old_release_parquet, 'GB')) + 20} SSD"
  }
}


task summaries {
  input {
    File engine_parquet
    String docker
  }

  command <<<
    set -euxo pipefail
    # Per test name x unit: counts, unit and value sources, value deciles, outcomes, mean reference limits per sex x
    # age (summaryReference, now with the limits filled from text). Needs the main output (post-injection unit).
    python3 /scripts/summaries/summaries.py --input ~{engine_parquet} --out . --out-cache cache
  >>>

  output {
    File test = "summaryTest.tsv"
    File unit_source = "summaryUnitSource.tsv"
    File values_source = "summaryValuesSource.tsv"
    File values = "summaryValues.tsv"
    File outcomes = "summaryOutcomes.tsv"
    File reference = "summaryReference.tsv"
  }

  runtime {
    docker: docker
    cpu: 8
    memory: "32 GB"
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 2) + 20} SSD"
  }
}
