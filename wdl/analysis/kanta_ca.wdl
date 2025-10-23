version 1.0

workflow kanta_ca {
  input {
    File pheno_list_file
    String ss_root
    String zones
    String docker
    
    # MOVED TO WORKFLOW INPUT
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
  }

  scatter (pheno in read_lines(pheno_list_file)) {
    call extract_regions {
      input:
        zones = zones,
        sumstats = sub(ss_root,"PHENO",pheno),
        pheno = pheno,
        docker = docker, # Pass workflow docker to task
        
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
  output {
    # NOTE: Outputting scattered results requires using an Array[File]
    Array[File] leadsnps = extract_regions.leadsnps
    Array[File] bed = extract_regions.bed
    Array[File] log = extract_regions.log
    Array[Boolean] had_results = extract_regions.had_results
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
      String zones
    }
    
    command <<<
    set -euo pipefail
    
    make_finemap_inputs.py \
        --sumstats ~{sumstats} \
        --rsid-col "~{rsid_col}" \
        --chromosome-col "~{chromosome_col}" \
        --position-col "~{position_col}" \
        --allele1-col "~{allele1_col}" \
        --allele2-col "~{allele2_col}" \
        --freq-col "~{freq_col}" \
        --beta-col "~{beta_col}" \
        --se-col "~{se_col}" \
        --p-col "~{p_col}" \
        --delimiter "~{delimiter}" \
        --grch38 \
        ~{true='--exclude-MHC' false='' exclude_MHC} \
        --prefix ~{pheno} \
        --out ~{pheno} \
        --window ~{window} \
        ~{if (max_region_width <0) then "" else "--max-region-width " + max_region_width} \
        --window-shrink-ratio ~{window_shrink_ratio} \
        ~{true='--scale-se-by-pval' false='' scale_se_by_pval} \
        ~{true='--x-chromosome' false='' x_chromosome} \
        ~{true='--set-variant-id' false='' set_variant_id} \
        ~{if defined(set_variant_id_map_chr) then '--set-variant-id-map-chr ' + set_variant_id_map_chr else ''} \
        --p-threshold ~{p_threshold} \
        ~{if defined(minimum_pval) then '--min-p-threshold ' + minimum_pval else ''} \
        --wdl     
    # Handle case where no results were found
    if [ "$res" == "False" ]; then
	touch ~{pheno}.lead_snps.txt
	touch ~{pheno}.bed
	touch ~{pheno}_formatted.txt
    else
	python3 -c "import pandas as pd; snps = pd.read_csv('~{pheno}.lead_snps.txt', sep='\t'); bed = pd.read_csv('~{pheno}.bed', sep='\t', names=['chr', 'start', 'end']); bed['chr'] = bed['chr'].astype(str); snps['chromosome'] = snps['chromosome'].astype(str); snps_with_regions = snps.apply(lambda row: pd.Series({'region': '{}:{}-{}'.format(row['chromosome'], m.iloc[0]['start'], m.iloc[0]['end']), 'chr': row['chromosome'], 'start': m.iloc[0]['start'], 'end': m.iloc[0]['end'], 'rsid': row['rsid'], 'mlogp': row['mlogp']}) if len(m := bed[(bed['chr'] == row['chromosome']) & (bed['start'] <= row['position']) & (bed['end'] >= row['position'])]) > 0 else pd.Series({'region': None, 'chr': None, 'start': None, 'end': None, 'rsid': None, 'mlogp': None}), axis=1).dropna(); top_snps = snps_with_regions.loc[snps_with_regions.groupby('region')['mlogp'].idxmax()]; top_snps['chr_num'] = pd.to_numeric(top_snps['chr'], errors='coerce'); top_snps = top_snps.sort_values(['chr_num', 'start']); open('~{pheno}_formatted.txt', 'w').write('\n'.join(['~{pheno}\t{}\t{}\t{}\t{:.5g}'.format(row['chr'], row['region'], row['rsid'], row['mlogp']) for _, row in top_snps.iterrows()]) + '\n')"
	
    fi
    >>>
    
    output {
      # Fix: If no results, the glob will find the dummy file. Otherwise, it finds the real ones.
      File region = pheno + ".formatted.txt"
      File leadsnps = pheno + ".lead_snps.txt"
      File bed = pheno + ".bed"
      File log = pheno + ".log"
      Boolean had_results = read_boolean("~{pheno}_had_results")
    }
    
    runtime {
      docker: "~{docker}"
      disks: "local-disk 20 HDD"
      preemptible: 2
      noAddress: true
    }
}
