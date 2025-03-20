version 1.0

workflow kanta_regenie {
  input {
    String docker
    File kanta_parquet
    String pheno
    Int omop_id
    File cov_file
    Boolean is_binary
    String measurement_col_name
    File bgen_list
    File? validate_hits
  }

  # EXTRACTS FROM HARMONIZED KANTA DATA THE ENTRIES WITH MATCHING OMOP
  # CALCULATES AVERAGE AGE AND MEASUREMENT_UNIT AND MERGES WITH COV FILE
  call create_pheno_file {
    input :
    pheno = pheno,
    cov_file=cov_file,
    parquet_file = kanta_parquet,
    mes_col = measurement_col_name,
    omop_id = omop_id,
  }

  # REGENIE STEP1
  call step1 {
    input :
    docker = docker,
    phenolist = [pheno],
    is_binary = is_binary,
    cov_pheno = create_pheno_file.pheno_file,
  }

  # REGENIE STEP2
  scatter (bgen in read_lines(bgen_list)) {
    call step2 {
      input:
      docker = docker,
      bgen = bgen,
      validate_hits=validate_hits,
      phenolist=[pheno],
      is_binary=is_binary,
      cov_pheno=create_pheno_file.pheno_file,
      covariates=step1.covars_used,
      pred=step1.pred,
      loco=step1.loco,
      nulls=step1.nulls,
      firth_list=step1.firth_list,
    }
  }

  Array[File] results = flatten(step2.regenie)
  call gather { input : files = results }
    
}


task gather {
  input {
    Array[File] files
  }
  command <<<
  pheno=`basename ~{files[0]} .regenie.gz | awk -F "." '{sub(/[^_]*_/, "", $NF); print $NF}'`
  mkdir regenie munged

  echo -e "`date`\tconcatenating result pieces into regenie/$pheno.regenie.gz, sorting by chr pos just in case"
  cat <(zcat ~{files[0]} | head -1)  <(for file in ~{sep=" " files};do zcat $file | tail -n+2 ;done | sort -k1,1g -k2,2g) | bgzip > regenie/$pheno.regenie.gz
   echo -e "`date`\tconverting to munged/$pheno.gz to a format used for importing to pheweb"
   zcat regenie/$pheno.regenie.gz | awk 'BEGIN {FS=" "; OFS="\t"; split("CHROM GENPOS ALLELE0 ALLELE1 LOG10P BETA SE A1FREQ", REQUIRED_FIELDS)}  NR==1 {for(i=1;i<=NF;i++) h[$i]=i;for(i in REQUIRED_FIELDS) if (!(REQUIRED_FIELDS[i] in h)) {print REQUIRED_FIELDS[i]" expected in regenie header">>"/dev/stderr"; exit 1} print "#chrom","pos","ref","alt","pval","mlogp","beta","sebeta","af_alt"} NR>1  {print $h["CHROM"],$h["GENPOS"],$h["ALLELE0"],$h["ALLELE1"],10^-$h["LOG10P"],$h["LOG10P"],$h["BETA"],$h["SE"],$h["A1FREQ"]}'  | bgzip > munged/$pheno.gz
   
   echo -e "`date`\ttabixing munged/$pheno.gz"
   tabix -s1 -b2 -e2 munged/$pheno.gz
   echo -e "`date`\tdone"
  >>>
  output {
    File regenie = glob("regenie/*.regenie.gz")[0]
    File pheweb = glob("munged/*.gz")[0]
    File pheweb_tbi = glob("munged/*.gz.tbi")[0]
  }
  
  runtime {
    memory: "8 GB"
    disks: "local-disk 200 HDD"
    preemptible: 2
    noAddress: true
  }
}

task step2 {

  input {
    Array[String] phenolist
    File cov_pheno
    String covariates
    String test
    Boolean is_binary
    File bgen
    File? validate_hits
    File pred
    File firth_list
    Array[File] loco
    Array[File] nulls
    Int bsize
    String options
    String docker
  }
  String test_cmd = if test == "additive" then "" else "--test "+ test
  File bgi = bgen + ".bgi"
  File sample = bgen + ".sample"
  String prefix = sub(basename(pred), "_pred.list", "") + "." + basename(bgen)
  
  command <<<
  ## continue statement exits with 1.... switch to if statement below in case want to pipefail back
  ##set -euxo pipefail
  n_cpu=`grep -c ^processor /proc/cpuinfo`
  # move loco files to /cromwell_root as pred file paths point there
  for file in ~{sep=" " loco}; do
      mv $file .
  done
  
  # move null files to /cromwell_root as firth_list file paths point there
  for file in ~{sep=" " nulls}; do
      mv $file .
  done
  
  regenie \
      --step 2 \
      ~{test_cmd} \
      ~{if is_binary then "--bt --af-cc" else ""} \
      --bgen ~{bgen} \
      --ref-first \
      --sample ~{sample} \
      --covarFile ~{cov_pheno} \
      --covarColList ~{covariates} \
      --phenoFile ~{cov_pheno} \
      --phenoColList ~{sep="," phenolist} \
      --pred ~{pred} \
      ~{if is_binary then "--use-null-firth ~{firth_list}" else ""} \
      ~{if defined(validate_hits) then "--extract ~{validate_hits}" else ""} \
      --bsize ~{bsize} \
      --threads $n_cpu \
      --gz \
      --out ~{prefix} \
      ~{options}
    
  >>>

  output {
    Array[File] log = glob("*.log")
    Array[File] regenie = glob("*.regenie.gz")
  }

  runtime {
    docker: "~{docker}"
    memory:"8 GB"
    disks: "local-disk " + (ceil(size(bgen, "G")) + 5) + " HDD"
    zones: "europe-west1-b europe-west1-c europe-west1-d"
    preemptible: 2
    noAddress: true
  }
}

  
task step1 {
  input {
    Array[String] phenolist
    Boolean is_binary
    File grm_bed
    String prefix = basename(grm_bed, ".bed")
    File cov_pheno
    String covariates
    Int bsize
    String options
    String docker
    Int covariate_inclusion_threshold
  }
  File grm_bim = sub(grm_bed, ".bed", ".bim")
  File grm_fam = sub(grm_bed, ".bed", ".fam")

  command <<<
  set -eux
  n_cpu=`grep -c ^processor /proc/cpuinfo`
  
  #filter out covariates with too few observations
  covars=~{covariates}
  COVARFILE=~{cov_pheno}
  PHENO="~{sep=',' phenolist}"
  THRESHOLD=~{covariate_inclusion_threshold}
  # Filter binary covariates that don't have enough covariate values in them
  # Inputs: covariate file, comma-separated phenolist, comma-separated covariate list, threshold for excluding a covariate
  # If a covariate is quantitative (=having values different from 0,1,NA), it is masked and will be passed through.
  # If a binary covariate has value NA, it will not be counted towards 0 or 1 for that covariate.
  # If a covariate does not seem to exist (e.g. PC{1:10}), it will be passed through.
  # If any of the phenotypes is not NA on that row, that row will be counted. This is in line with the step1 mean-imputation for multiple phenotypes.
  zcat -f $COVARFILE | awk -v covariates=$covars  -v phenos=$PHENO -v th=$THRESHOLD > new_covars  '
        BEGIN{FS="\t"}
        NR == 1 {
            covlen = split(covariates,covars,",")
            phlen = split(phenos,phenoarr,",")
            for (i=1; i<=NF; i++){
                h[$i] = i
                mask[$i] = 0
            }
        }
        NR > 1 {
            #if any of the phenotypes is not NA, then take the row into account
            process_row=0
            for (ph in phenoarr){
                if ($h[phenoarr[ph]] != "NA"){
                    process_row = 1
                }
            }
            if (process_row == 1){
                for (co in covars){
                    if($h[covars[co]] == 0) {
                        zerovals[covars[co]] +=1
                    }
                    else if($h[covars[co]] == 1) {
                        onevals[covars[co]] +=1
                    }
                    else if($h[covars[co]] == "NA"){
                        #no-op
                        na=0;
                    }
                    else {
                        #mask this covariate to be included, no matter the counts
                        #includes both covariate not found in header and quantitative covars
                        mask[covars[co]] =1
                    }
                }
            }

        }
        END{
            SEP=""
            for (co in covars){
                if( ( zerovals[covars[co]] > th && onevals[covars[co]] > th ) || mask[covars[co]] == 1 ){
                    printf("%s%s" ,SEP,covars[co])
                    SEP=","
                }
                printf "Covariate %s zero count: %d one count: %d mask: %d\n",covars[co],zerovals[covars[co]],onevals[covars[co]],mask[covars[co]] >> "/dev/stderr";
            }
        }'

  NEWCOVARS=$(cat new_covars)
  # fid needs to be the same as iid in fam
  awk '{$1=$2} 1' ~{grm_fam} > tempfam && mv tempfam ~{grm_fam}

  regenie \
        --step 1 \
        ~{if is_binary then "--bt" else ""} \
        --bed ~{sub(grm_bed, "\\.bed$", "")} \
        --covarFile ~{cov_pheno} \
        --covarColList $NEWCOVARS \
        --phenoFile ~{cov_pheno} \
        --phenoColList ~{sep="," phenolist} \
        --bsize ~{bsize} \
        --lowmem \
        --lowmem-prefix tmp_rg \
        --gz \
        --threads $n_cpu \
        --out ~{prefix} \
        ~{if is_binary then "--write-null-firth" else ""} \
        ~{options}

  # rename loco files with phenotype names and update pred.list accordingly giving it a unique name
  awk '{orig=$2; sub(/_[0-9]+.loco.gz/, "."$1".loco.gz", $2); print "mv "orig" "$2} ' ~{prefix}_pred.list | bash
  phenohash=`echo ~{sep="," phenolist} | md5sum | awk '{print $1}'`
  awk '{sub(/_[0-9]+.loco.gz/, "."$1".loco.gz", $2)} 1' ~{prefix}_pred.list > ~{prefix}.$phenohash.pred.list
  loco_n=$(wc -l ~{prefix}.$phenohash.pred.list|cut -d " " -f 1)
  
  #check that loco predictions were created for every pheno
  if [[ $loco_n -ne ~{length(phenolist)} ]]; then
      echo "The model did not converge. This job will abort."
      exit 1
  fi
  
  if [[ "~{is_binary}" == "true" ]]
  then
      # rename firth files with phenotype names and update firth.list accordingly giving it a unique name
      awk '{orig=$2; sub(/_[0-9]+.firth.gz/, "."$1".firth.gz", $2); print "mv "orig" "$2} ' ~{prefix}_firth.list | bash
      awk '{sub(/_[0-9]+.firth.gz/, "."$1".firth.gz", $2)} 1' ~{prefix}_firth.list > ~{prefix}.$phenohash.firth.list
      
      #check if there is a firth approx per every null
      firth_n=$(wc -l ~{prefix}.$phenohash.firth.list|cut -d " " -f 1)
      if [[ $loco_n -ne $firth_n ]]; then
          echo "fitting firth null approximations FAILED. This job will abort."
          exit 1
      fi
  else # touch files to have quant phenos not fail output globbing
      touch ~{prefix}.$phenohash.firth.list
      touch get_globbed.firth.gz
  fi
  >>>
  runtime {
    docker: "~{docker}"
    disks: "local-disk 200 HDD"
    memory:"16 GB"
    zones: "europe-west1-b europe-west1-c europe-west1-d"
    preemptible: 2
    noAddress: true
  }
  output {
    File log = prefix + ".log"
    Array[File] loco = glob("*.loco.gz")
    File pred = glob("*.pred.list")[0]
    Array[File] nulls = glob("*.firth.gz")
    File firth_list = glob("*.firth.list")[0]
    String covars_used = read_string("new_covars")
    File covariatelist = "new_covars"
  }
}
  
task create_pheno_file {
  input {
    String pheno
    File parquet_file
    File cov_file
    Int min_count
    Int omop_id
    String mes_col
    String docker
  }
  String out_file = pheno + "_pheno.txt.gz"
  command <<<
  set -eux
  PHENO="~{pheno}"
  OMOP_ID="~{omop_id}"
  PARQUET="~{parquet_file}"
  MEASUREMENT_COLUMN="~{mes_col}"
  # EXTRACT IDs and dump to tsv
  clickhouse --query "
  SELECT
    FINNGENID,
    EVENT_AGE,
    $MEASUREMENT_COLUMN
  FROM (
    SELECT
        FINNGENID,
        EVENT_AGE,
        $MEASUREMENT_COLUMN,
        count(*) OVER (PARTITION BY FINNGENID) AS measurement_count
    FROM
        file('$PARQUET', Parquet)
    WHERE
        OMOP_CONCEPT_ID = '$OMOP_ID'
        AND $MEASUREMENT_COLUMN IS NOT NULL
  )
  WHERE measurement_count >= ~{min_count}
  INTO OUTFILE 'output.tsv'
  FORMAT TSVWithNames;
  "
  head output.tsv
  # with pandas, calculate medians and RINT the median measurement column into PHENO column
  python3 -c "import sys, pandas as pd, numpy as np, scipy.stats as stats; df = pd.read_csv('./output.tsv',sep='\t').groupby('FINNGENID').median();print(df);new_col = '$PHENO'; col = '$MEASUREMENT_COLUMN';print(new_col,col); df[new_col] = stats.norm.ppf((stats.rankdata(df[col]) - 0.375) / (len(df[col]) + 0.25)); df.to_csv('./transformed.tsv', sep='\t')" 
  head transformed.tsv
  # sort cov file and merge
  zcat -f  ~{cov_file}|   (sed -u 1q; sort) > cov.txt
  join -t $'\t' --header cov.txt transformed.tsv | bgzip -c > ~{out_file}
  >>>
  runtime {
    docker : "~{docker}"
    disks:   "local-disk ~{ceil(size(parquet_file,'GB')) + ceil(size(cov_file,'GB')) + 10} HDD"
  }
  output {
    File pheno_file = out_file
    }
}

