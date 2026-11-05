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
    String harmonization_branch = "kanta_v4"
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
    injection_branch = injection_branch,
    harmonization_branch = harmonization_branch
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

  # Reference tables for the next run, built from this run's output:
  # - reference-range texts -> numeric limits, checked against the rows; LIMITS feeds the next run's reference
  #   extraction (approved + final limits), UNDECIDED (still to decide on) and REJECTED (out for good) split the pairs
  #   not approved
  # - coding system -> organisation and maakunta from THL's sote register (coding_system_map.tsv, to copy into
  #   src/kanta/engine/data/ for the next run)
  call extract_regions_limits {
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

  call get_date { input: prefix = prefix }
  call release {
    input:
    prefix = prefix,
    release_name = get_date.release_name,
    engine_parquet_path = "~{run_engine.engine_parquet}",
    errors_parquet_path = "~{run_engine.errors_parquet}",
    abbr_parquet_path = "~{run_engine.abbr_parquet}",
    unit_parquet_path = "~{run_engine.unit_parquet}",
    release_parquet_path = "~{run_engine.release_parquet}",
    log_path = "~{run_engine.log}",
    change_plot_path = "~{release_counts.change_plot}",
    metabolomics_biomarkers_path = "~{metabolomics.biomarkers}",
    metabolomics_risks_path = "~{metabolomics.risks}",
    # every copied file lands on the local disk
    disk_gb = ceil(size([run_engine.engine_parquet, run_engine.errors_parquet, run_engine.abbr_parquet,
                         run_engine.unit_parquet, run_engine.release_parquet, run_engine.log,
                         release_counts.change_plot, metabolomics.biomarkers, metabolomics.risks], "GB")) + 20
  }

  output {
    String release_name = get_date.release_name
    File engine_parquet = release.engine_parquet
    File errors_parquet = release.errors_parquet
    File abbr_parquet = release.abbr_parquet
    File unit_parquet = release.unit_parquet
    File release_parquet = release.release_parquet
    File log = release.log
    File abnormality_table = abnormality.table
    File abnormality_ranges = abnormality.ranges
    File pos_neg_summary = pos_tables.pos_neg_summary
    File pos_neg_summary_pasteable = pos_tables.pos_neg_summary_pasteable
    File plusplus_summary = pos_tables.plusplus_summary
    File plusplus_summary_pasteable = pos_tables.plusplus_summary_pasteable
    File ref_text_table = extract_regions_limits.table
    File ref_text_limits = extract_regions_limits.limits
    File ref_text_undecided = extract_regions_limits.undecided
    File ref_text_rejected = extract_regions_limits.rejected
    File coding_system_map = extract_regions_limits.coding_system_map
    File coding_system_counts = extract_regions_limits.coding_system_counts
    File metabolomics_biomarkers = release.metabolomics_biomarkers
    File metabolomics_risks = release.metabolomics_risks
    File counts_new = release_counts.counts_new
    File counts_old = release_counts.counts_old
    File counts_change = release_counts.change
    File counts_change_plot = release.change_plot
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
    String harmonization_branch
  }

  Int cpus = 64
  command <<<
    set -euxo pipefail
    echo "cpus: $(nproc) ~{cpus}"
    # The engine writes its chunk files under tempfile.mkdtemp(), i.e. /tmp by default,
    # which lives on the boot disk in Cromwell. Point it at the sized local disk instead.
    mkdir -p tmp
    export TMPDIR="$(pwd)/tmp"
    python3 -m kanta.engine \
      --input-file ~{tidied_parquet} \
      --output-prefix ~{prefix} \
      --chunk-size ~{chunk_size} \
      --injection-branch ~{injection_branch} \
      --harmonization-branch ~{harmonization_branch} \
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
    cpu: "~{cpus}"
    memory: "64 GB"
    disks: "local-disk ~{ceil(size(tidied_parquet, 'GB') * 10) + 50} SSD"
    # a preempted VM is reported as a failed task, so retry instead of failing the workflow
    maxRetries: 2
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
    disks: "local-disk ~{ceil(size(release_parquet, 'GB') * 3) + 20} HDD"
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
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 2) + 20} HDD"
  }
}


task extract_regions_limits {
  input {
    File engine_parquet
    String docker
  }

  command <<<
    set -euxo pipefail
    # Rows per test name x reference text, the limits parsed from each text, and the value/sex/age checks.
    # Step 1 is one duckdb pass; its cache and spill files go to ./tmp on the local disk.
    python3 /scripts/ref_ranges/ref_text_limits.py --input ~{engine_parquet} --out . --tmp tmp
    # Coding systems with more than 5 rows -> organisation and maakunta, from THL's sote register (bundled in the
    # image next to the script): coding_system_counts.tsv (counts) and coding_system_map.tsv (the engine's table)
    python3 /scripts/sote/data/coding_system_map.py --input ~{engine_parquet} --out .
  >>>

  output {
    File table = "ref_text_table.tsv"
    File limits = "ref_text_table_LIMITS.tsv"
    File undecided = "ref_text_table_UNDECIDED.tsv"
    File rejected = "ref_text_table_REJECTED.tsv"
    File coding_system_map = "coding_system_map.tsv"
    File coding_system_counts = "coding_system_counts.tsv"
  }

  runtime {
    docker: docker
    cpu: 8
    memory: "32 GB"
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 2) + 20} HDD"
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
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 3 / 2) + 20} HDD"
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
    disks: "local-disk ~{ceil(size(release_parquet, 'GB') + size(old_release_parquet, 'GB')) + 20} HDD"
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
    disks: "local-disk ~{ceil(size(engine_parquet, 'GB') * 2) + 20} HDD"
  }
}


# <prefix>_<today>. Volatile: never call-cached, so the date is always today's even when every other task is cached.
# Runtime from the workflow options' defaults (google_inputs.json).
task get_date {
  input { String prefix }
  command <<< date -u +%Y_%m_%d | tr -d '\n' >>>
  output { String release_name = prefix + "_" + read_string(stdout()) }
  meta { volatile: true }
}


task release {
  input {
    String prefix
    String release_name
    # gs:// paths (String, not File): nothing is localized, the files are read through the gcsfuse mount
    String engine_parquet_path
    String errors_parquet_path
    String abbr_parquet_path
    String unit_parquet_path
    String release_parquet_path
    String log_path
    # Unprefixed downstream outputs, published with the release name in front: release_counts' plot of the per-OMOP
    # count changes vs the earlier release, and the metabolomics tables
    String change_plot_path
    String metabolomics_biomarkers_path
    String metabolomics_risks_path
    Int disk_gb
  }

  command <<<
    set -euxo pipefail
    name="~{release_name}"
    #gcsfuze trick to avoide localization (very slow with HDD and pointless in this case)
    to_fuse() { echo "$1" | sed 's|gs://[^/]*/|/mnt/disks/gcs/|'; }
    for f in ~{engine_parquet_path} ~{errors_parquet_path} ~{abbr_parquet_path} ~{unit_parquet_path} ~{release_parquet_path} ~{log_path}; do
      base=$(basename "$f")
      cp "$(to_fuse "$f")" "./${base/~{prefix}/$name}"
    done
    # These names have no prefix to replace: the release name goes in front
    for f in ~{change_plot_path} ~{metabolomics_biomarkers_path} ~{metabolomics_risks_path}; do
      cp "$(to_fuse "$f")" "./${name}_$(basename "$f")"
    done
    ls -l
  >>>

  output {
    File engine_parquet = "~{release_name}.parquet"
    File errors_parquet = "~{release_name}_errors.parquet"
    File abbr_parquet = "~{release_name}_abbr.parquet"
    File unit_parquet = "~{release_name}_unit.parquet"
    File release_parquet = "~{release_name}_RELEASE.parquet"
    File log = "~{release_name}.log"
    File change_plot = "~{release_name}_relative_change_histograms.png"
    File metabolomics_biomarkers = "~{release_name}_nightingale_results_metabolomics.tsv.gz"
    File metabolomics_risks = "~{release_name}_nightingale_results_risks.tsv.gz"
  }

  runtime { disks: "local-disk ~{disk_gb} HDD" }
}
