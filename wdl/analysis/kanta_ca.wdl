version 1.0

workflow kanta_ca {
    input {
        String prefix
        File pheno_list_file
        File pheno_file
        String ss_root
        String docker
        Boolean test
        Array[String] covariates

        String rsid_col
        String chromosome_col
        String position_col
        String allele1_col
        String allele2_col
        String freq_col
        String beta_col
        String se_col
        String p_col
        String mlogp_col
        String delimiter
    }

    call filter_covariates {
        input:
            pheno_file = pheno_file,
            pheno_list = pheno_list_file,
            covariates = covariates
    }

    scatter (pheno in read_lines(pheno_list_file)) {
        call extract_regions {
            input:
                sumstats = sub(ss_root, "PHENO", pheno),
                pheno = pheno,
                # Pass workflow inputs to task
                rsid_col = rsid_col,
                chromosome_col = chromosome_col,
                position_col = position_col,
                allele1_col = allele1_col,
                allele2_col = allele2_col,
                freq_col = freq_col,
                beta_col = beta_col,
                se_col = se_col,
                p_col = p_col,
                delimiter = delimiter
        }
    }

    call merge_regions {
        input:
            hits = extract_regions.region,
            test = test
    }

    Map[String, String] cov_map = read_map(filter_covariates.cov_pheno_map)
    Array[Array[String]] all_regions = read_tsv(merge_regions.regions)

    scatter (region in all_regions) {
        String pheno = region[0]
        String chrom = region[1]
        String region_limits = region[2]
        String locus = region[3]

        call regenie_conditional {
            input:
                docker = docker,
                prefix = prefix,
                locus = locus,
                region = region_limits,
                pheno = pheno,
                chrom = chrom,
                covariates = cov_map[pheno],
                mlogp_col = mlogp_col,
                chr_col = chromosome_col,
                pos_col = position_col,
                ref_col = allele1_col,
                alt_col = allele2_col,
                sumstats_root = ss_root,
                beta_col = beta_col,
                se_col = se_col,
                pheno_file = pheno_file
        }
    }

    output {
        Array[File] results = flatten(regenie_conditional.conditional_chains)
    }
}

task regenie_conditional {
  input {
    # GENERAL PARAMS
    String docker
    String prefix
    # hit info
    String locus
    String region
    String pheno
    String chrom
    # files to localize
    File pheno_file
    String bgen_root
    String null_root
    String sumstats_root
    # column names and stuff
    String chr_col
    String pos_col
    String ref_col
    String alt_col
    String mlogp_col
    String beta_col
    String se_col
    # Script parameters/options
    Float conditioning_mlogp_threshold
    Int max_steps
    String covariates
    String? regenie_params
  }
    
    # localize all files based on roots and pheno/chrom info
  File sumstats = sub(sumstats_root, "PHENO", pheno)
  File sum_tabix = sumstats + ".tbi"
  File null = sub(null_root, "PHENO", pheno)
  File bgen = sub(bgen_root, 'CHROM', chrom)
  File bgen_sample = bgen + ".sample"
  File bgen_index = bgen + ".bgi"
  
  # runtime params based on file sizes
  Int disk_size = 120

  command <<<
  tabix -h ~{sumstats} ~{region} > region_sumstats.txt
  
  python3 /scripts/regenie_conditional.py \
          --out ./~{prefix} \
          --bgen ~{bgen} \
          --null-file ~{null} \
          --sumstats region_sumstats.txt \
          --pheno-file ~{pheno_file} \
          --pheno ~{pheno} \
          --locus-region ~{locus} \
          ~{region} \
          --pval-threshold ~{conditioning_mlogp_threshold} \
          --max-steps ~{max_steps} \
          --chr-col '~{chr_col}' \
          --pos-col '~{pos_col}' \
          --ref-col '~{ref_col}' \
          --alt-col '~{alt_col}' \
          --mlogp-col '~{mlogp_col}' \
          --beta-col '~{beta_col}' \
          --sebeta-col '~{se_col}' \
          --covariates ~{covariates} \
          ~{if defined(regenie_params) then " --regenie-params " + regenie_params else ""} \
          --log info
  >>>
    
  output {
    Array[File] conditional_chains = glob("./${prefix}*.snps")
    Array[File] logs = glob("./${prefix}*.log")
    Array[File] regenie_output = glob("./${prefix}*.conditional")
  }
    
  runtime {
    docker: "~{docker}"
    disks: "local-disk ${disk_size} HDD"
  }
}
  
task merge_regions {
  input {
    Array[File] hits
    Boolean test
  }
    
  String outfile = "regions.txt"
    
  command <<<
  while read f; do cat $f >> tmp.txt; done < ~{write_lines(hits)}
  cat tmp.txt ~{if test then " | shuf | head -n 10" else ""} > ~{outfile}
  >>>

  output {File regions = outfile}
}

task filter_covariates {
  
  input {
    File pheno_file
    Array[String] covariates
    File pheno_list
    Int threshold_cov_count
  }
  
  String outfile = "./pheno_cov_map_" + threshold_cov_count + ".txt"
  Int disk_size = ceil(size(pheno_file,'GB')) + 2 * 2
  
  command <<<

      set -euxo pipefail
      
      python3 <<CODE
      
      import pandas as pd
      import numpy as np
      
      #read in phenos as list of phenos regardless
      tot_phenos = []
      phenos_groups = []
      with open('~{pheno_list}') as i:
          for line in i:
              phenos = line.strip().split()
              phenos_groups.append(phenos)
              tot_phenos += phenos    

      #read in phenos mapping all valid entries to 1 and NAs to 0
      pheno_df= pd.read_csv('~{pheno_file}',sep='\t',usecols=tot_phenos).notna().astype(int)
      print(pheno_df)
      # read in covariates getting absolute values (handles PCs)
      covariates= '~{sep="," covariates}'.split(',')
      cov_df= pd.read_csv('~{pheno_file}',sep='\t',usecols=covariates).abs()
      print(cov_df)

      # now for each pheno calculate product of each covariate with itself
      
      with open('~{outfile}','wt') as o,open('~{outfile}'.replace('.txt','.err.txt'),'wt') as tmp_err:
          for i,pheno_list in enumerate(phenos_groups):
              pheno_name = ','.join(pheno_list)
              # for each group of phenos (possibly a single one) multiply all covs and pheno column and count how many non 0 entries are there: this means that the entry has a valid pheno and a non null covariates.
              df = pd.DataFrame()
              for pheno in pheno_list:
                  m = (cov_df.mul(pheno_df[pheno],0)>0).sum().to_frame(pheno)
                  df = pd.concat([df,m],axis =1)

              print(f"{i+1}/{len(phenos_groups)} {pheno_name}")
              #If it's a group of phenos the min function will return the lowest count across all phenos
              tmp_df = df[pheno_list].min(axis =1)
              covs = tmp_df.index[tmp_df >= ~{threshold_cov_count}].tolist()
              missing_covs = [elem for elem in covariates if elem not in covs]
              if missing_covs:tmp_err.write(f"{pheno_name}\t{','.join(missing_covs)}\n")
              o.write(f"{pheno_name}\t{','.join(covs)}\n")
      
      CODE

  >>>
  output {
    File cov_pheno_map = outfile
    File cov_pheno_warning = sub(outfile,'.txt','.err.txt')
  }
  
  runtime {
    memory: "64 GB"
    disks: "local-disk ${disk_size} HDD"
  }

}

  
task extract_regions {
  input {
    File sumstats
    String pheno
    # Passed from Workflow
    String rsid_col
    String chromosome_col
    String position_col
    String allele1_col
    String allele2_col
    String freq_col
    String beta_col
    String se_col
    String p_col
    String delimiter

    # Task-specific parameters remain
    Int window
    Int max_region_width
    Float window_shrink_ratio
    Float p_threshold
    Float? minimum_pval

    Boolean scale_se_by_pval
    Boolean exclude_MHC
    Boolean x_chromosome
    Boolean set_variant_id
    String? set_variant_id_map_chr
    
        String docker
    Int mem
  }
  
  command <<<
  set -euo pipefail
  
  make_finemap_inputs.py \
      --sumstats ~{sumstats} \
      --rsid-col '~{rsid_col}' \
      --chromosome-col '~{chromosome_col}' \
      --position-col '~{position_col}' \
      --allele1-col '~{allele1_col}' \
      --allele2-col '~{allele2_col}' \
      --freq-col '~{freq_col}' \
      --beta-col '~{beta_col}' \
      --se-col '~{se_col}' \
      --p-col '~{p_col}' \
      --delimiter '~{delimiter}' \
      --grch38 \
      ~{true='--exclude-MHC ' false='' exclude_MHC} \
      --prefix ~{pheno} \
      --out ~{pheno} \
      --window ~{window} \
      ~{if (max_region_width < 0) then "" else "--max-region-width " + max_region_width } \
      --window-shrink-ratio ~{window_shrink_ratio} \
      ~{true='--scale-se-by-pval ' false='' scale_se_by_pval} \
      ~{true='--x-chromosome ' false='' x_chromosome} \
      ~{true='--set-variant-id ' false='' set_variant_id} \
      ~{if defined(set_variant_id_map_chr) then '--set-variant-id-map-chr ' + set_variant_id_map_chr  else ''} \
      --p-threshold ~{p_threshold} \
      ~{if defined(minimum_pval) then '--min-p-threshold ' + minimum_pval else ''} \
      --wdl

  # Define the res variable by reading the file created by make_finemap_inputs.py
  res=$(cat ~{pheno}_had_results)
  
  # Handle case where no results were found (res == "False")
  if [ "$res" == "False" ]; then
      touch ~{pheno}.lead_snps.txt
      touch ~{pheno}.bed
      touch ~{pheno}.formatted.txt
  else
      # Existing logic for successful runs
      python3 -c "import pandas as pd; snps = pd.read_csv('~{pheno}.lead_snps.txt', sep='\t'); bed = pd.read_csv('~{pheno}.bed', sep='\t', names=['chr', 'start', 'end']); bed['chr'] = bed['chr'].astype(str); snps['chromosome'] = snps['chromosome'].astype(str); snps_with_regions = snps.apply(lambda row: pd.Series({'region': '{}:{}-{}'.format(row['chromosome'], m.iloc[0]['start'], m.iloc[0]['end']), 'chr': row['chromosome'], 'start': m.iloc[0]['start'], 'end': m.iloc[0]['end'], 'rsid': row['rsid'], 'mlogp': row['mlogp']}) if len(m := bed[(bed['chr'] == row['chromosome']) & (bed['start'] <= row['position']) & (bed['end'] >= row['position'])]) > 0 else pd.Series({'region': None, 'chr': None, 'start': None, 'end': None, 'rsid': None, 'mlogp': None}), axis=1).dropna(); top_snps = snps_with_regions.loc[snps_with_regions.groupby('region')['mlogp'].idxmax()]; top_snps['chr_num'] = pd.to_numeric(top_snps['chr'], errors='coerce'); top_snps = top_snps.sort_values(['chr_num', 'start']); open('~{pheno}.formatted.txt', 'w').write('\n'.join(['~{pheno}\t{}\t{}\t{}\t{:.5g}'.format(row['chr'], row['region'], row['rsid'], row['mlogp']) for _, row in top_snps.iterrows()]) + '\n')"
  fi
  >>>

  output {
    File region = pheno + ".formatted.txt"
    File leadsnps = pheno + ".lead_snps.txt"
    File bed = pheno + ".bed"
    File log = pheno + ".log"
    Boolean had_results = read_boolean("~{pheno}_had_results")
  }

  runtime {
    docker: "~{docker}"
    memory: "${mem} GB"
    disks: "local-disk 20 HDD"
    preemptible: 2
    noAddress: true
  }
}
