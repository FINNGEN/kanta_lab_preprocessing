version 1.0

workflow kanta_core {
  input {
    String prefix
    String kanta_docker
    String? release_docker
    String? analysis_docker
    # test mode will use only 100k lines and 4 cpus
    Boolean test
    File kanta_munged_data
  }

  # splits input in chunks
  call split { input:test = test,kanta_data = kanta_munged_data}
  scatter (i in range(length(split.chunks))) {
    call munge {
      input: docker = kanta_docker, prefix = i,chunk = split.chunks[i]
    }
  }
  String base_prefix =  if test then prefix + "_test" else prefix
  # merge chunks & logs
  call merge { input: prefix = base_prefix,munged_chunks = munge.munged_chunk,docker=kanta_docker}
  call merge_logs {input: prefix =  base_prefix,logs = flatten(munge.logs)}
  # build parquet and release file
  call release { input: docker = select_first([release_docker, kanta_docker]), mem = if test then 4 else 64, prefix = prefix, munged_data  = merge.merged_file}
  call validate_outputs {input : parquet_file = release.core_files[1],docker=kanta_docker}
  call compare_versions {input: new_parquet=release.core_files[1],docker=select_first([analysis_docker, kanta_docker]),prefix=prefix}
}

task compare_versions {
  input {
    String docker
    String prefix
    File new_parquet
    File old_parquet
  }
  command <<<
  python3 /sb_release/counts.py ~{new_parquet} -c FINNGENID,MEASUREMENT_VALUE_HARMONIZED,MEASUREMENT_VALUE_EXTRACTED,TEST_OUTCOME,TEST_OUTCOME_TEXT_EXTRACTED,OUTCOME_POS_EXTRACTED -b QC_PASS -o . --prefix ~{prefix}
  python3 /sb_release/counts.py ~{old_parquet} -c FINNGENID,MEASUREMENT_VALUE_HARMONIZED,MEASUREMENT_VALUE_EXTRACTED,TEST_OUTCOME,TEST_OUTCOME_TEXT_EXTRACTED,OUTCOME_POS_EXTRACTED -b QC_PASS -o . --prefix old
  python3 -c "import csv, sys; reader = csv.DictReader(sys.stdin); print('conceptId\tconceptName'); [print(f\"{row['conceptId']}\t{row['conceptName']}\") for row in reader]" < /finngen_qc/data/LABfi_ALL.usagi.csv > omop_name_table.tsv
  ls
  # Define your bash variables
  NEW_FILE='~{prefix}_omop_analysis.tsv'
  OLD_FILE='old_omop_analysis.tsv'
  NEW_LAB="~{prefix}"
  OLD_LAB="~{basename(old_parquet,'.parquet')}"

  # Run the command using os.environ to pull the bash variables into Python
  export NEW_FILE OLD_FILE NEW_LAB OLD_LAB MPLCONFIGDIR='/tmp/matplotlib_cache'; mkdir -p $MPLCONFIGDIR; python3 -c "import os, pandas as pd, matplotlib.pyplot as plt, numpy as np; NF,OF,NL,OL=os.environ['NEW_FILE'],os.environ['OLD_FILE'],os.environ['NEW_LAB'],os.environ['OLD_LAB']; v_new=pd.read_csv(NF,sep='\t'); v_old=pd.read_csv(OF,sep='\t'); names=pd.read_csv('omop_name_table.tsv',sep='\t'); names['NAME']=names['conceptId'].astype(str); m=v_new.merge(v_old,on='NAME',how='left',suffixes=('_'+NL,'_'+OL)).merge(names[['NAME','conceptName']],on='NAME',how='left'); cols=[c for c in v_new.columns if c!='NAME']; out={'NAME':m['NAME']}; out.update({c:m.apply(lambda r,col=c: np.nan if pd.isna(r[f'{col}_{OL}']) or r[f'{col}_{OL}']==0 else round(r[f'{col}_{NL}']/r[f'{col}_{OL}'],3),axis=1) for c in cols}); df_out=pd.DataFrame(out); df_out.to_csv('relative_change.tsv',sep='\t',index=False,na_rep='NA'); fig,ax=plt.subplots(len(cols),3,figsize=(24,6*len(cols)),squeeze=False); [(lambda mask,i,col: (ax[i,0].scatter(m.loc[mask,f'{col}_{NL}'],df_out.loc[mask,col],alpha=0.4,s=20), ax[i,0].set_xscale('log'), ax[i,0].axhline(1,color='red',ls='--'), ax[i,0].set_title(f'{col}: Full Scatter'), ax[i,1].scatter(m.loc[mask,f'{col}_{NL}'],df_out.loc[mask,col],alpha=0.4,s=20,color='orange'), ax[i,1].set_ylim(0,2), ax[i,1].set_xscale('log'), ax[i,1].axhline(1,color='red',ls='--'), ax[i,1].set_title(f'{col}: Zoomed Scatter'), ax[i,2].hist(df_out.loc[mask,col].dropna(),bins=50,range=(0,2),color='green',alpha=0.6,edgecolor='black'), ax[i,2].axvline(1,color='red',ls='--'), ax[i,2].text(0.95,0.95,f'n > 2.0: {sum(df_out.loc[mask,col] > 2)}',transform=ax[i,2].transAxes,ha='right',va='top',bbox=dict(boxstyle='round',facecolor='white',alpha=0.5)), ax[i,2].set_title(f'{col}: RelChange Hist')))( (df_out[col].notna()), i, col) for i,col in enumerate(cols)]; plt.tight_layout(); plt.savefig('relative_change_analysis.png',dpi=200); plt.close()"

  mv ./relative_change_analysis.png ~{prefix}_analysis.png
  mv ./relative_change.tsv ~{prefix}_analysis.tsv
  >>>
  output {
    File figure = "~{prefix}_analysis.png" 
    File comparison = "~{prefix}_analysis.tsv" 
    File summary_table = "./~{prefix}_omop_analysis.tsv"
    File summary_md = "./~{prefix}_omop_analysis.md"
    }
  runtime {
    disks: "local-disk ~{2*ceil(size(new_parquet,'GB')) + 10} HDD"
    docker : "~{docker}"
  }

  
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

task release {
  input {
    String docker
    File munged_data
    Int mem
    String prefix
  }
  String core_prefix = prefix +  "_core"
  String meta_prefix = prefix +  "_metadata_columns"
  command <<<
  echo ~{mem}
  set -euxo pipefail
  awk '/^MemTotal:/{print $2/1024/1024}' /proc/meminfo
  /usr/bin/time -v bash /sb_release/run.sh ~{munged_data} . ~{core_prefix} core 2> tmp.txt
  cat tmp.txt &&  cat tmp.txt | awk '/Maximum resident set size/ {print "Max memory usage (GB):", $6/1024/1024}'
  /usr/bin/time -v bash /sb_release/run.sh ~{munged_data} . ~{meta_prefix} metadata 2> tmp.txt
  cat tmp.txt &&  cat tmp.txt | awk '/Maximum resident set size/ {print "Max memory usage (GB):", $6/1024/1024}'
  
  >>>
  runtime {
    docker : "eu.gcr.io/finngen-sandbox-v3-containers/kanta:parquet"
    disks: "local-disk ~{ceil(size(munged_data,'GB')) * 4 + 10} HDD"
    memory: "~{mem} GB"
    cpu : mem/4
  }
  output {
    Array[File] core_files = ["~{core_prefix}.txt.gz","~{core_prefix}.parquet","~{core_prefix}.log","~{core_prefix}_schema.json"]
    Array[File] meta_files = ["~{meta_prefix}.txt.gz","~{meta_prefix}.parquet","~{meta_prefix}.log","~{meta_prefix}_schema.json"]
  }
}

task merge {
  input {
    Array[File] munged_chunks
    String prefix
    String docker
  }
  String out_file = prefix + ".txt.gz"
  String dup_file = prefix +"_duplicates.txt.gz"
  command <<<
  # write header to reports file
  zcat ~{munged_chunks[0]} | head -n1 | bgzip -c > tmp.txt.gz
  # merge files including reports
  while read f; do echo $f && date +%Y-%m-%dT%H:%M:%S && zcat $f | sed -E 1d | bgzip -c >> tmp.txt.gz ; done < <(cat ~{write_lines(munged_chunks)} | sort -V )

  python3 /core/duplicates.py --input tmp.txt.gz --prefix ~{prefix}

  >>>
  runtime {
    disks: "local-disk ~{ceil(size(munged_chunks,'GB')) * 4 + 10} HDD"
    docker : "~{docker}"
  }
  output {
    File merged_file = out_file
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
  for f in {err,warn} ; do  cat logs.txt | grep $f | head -n1 | xargs head -n1 > ~{prefix}"_"$f".txt"; done
  for f in {err,warn,log} ;do while read i ;do cat $i | sed -E 1d >> ~{prefix}"_"$f".txt"; done < <(cat logs.txt | grep $f | sort -V);done
  >>>
  runtime {disks: "local-disk ~{ceil(size(logs,'GB')) * 4 + 10} HDD"}
  output {
    File out_log  = "~{prefix}_log.txt"
    File out_err  = "~{prefix}_err.txt"
    File out_warn = "~{prefix}_warn.txt"
  }
}

task munge {
  input {
    String docker
    File chunk
    String prefix
    Int cpus
  }

  command <<<
  set -euxo pipefail
  python3 /core/main.py --gz  --mp --raw-data ~{chunk} --prefix ~{prefix} 
  ls 
  >>>
  runtime {
    docker : "~{docker}"
    disks: "local-disk ~{ceil(size(chunk,'GB')) * 3 + 10} HDD"
    mem: "~{cpus} GB"
    cpu : "~{cpus}"
  }
  output {
    File munged_chunk = "~{prefix}.txt.gz"
    Array[File] logs = glob("./~{prefix}*txt")
  }
}

task split{
  input {
    File kanta_data
    Int n_chunks
    Boolean test
  }

  Int chunks = if test then 4  else n_chunks
  command <<<
  zcat ~{kanta_data} | head -n1 > header.txt
  zcat ~{kanta_data} | sed -E 1d ~{if test then " | head -n 100000 "  else ""} > tmp.tsv
  for f in {00..~{chunks-1}}; do cat header.txt | bgzip -c > kanta$f.gz; done
  split tmp.tsv -n l/~{chunks} -d kanta --filter='gzip >> $FILE.gz'
  >>>

  runtime {
    disks: "local-disk ~{ceil(size(kanta_data,'GB')) * 10 + 20} HDD"
  }

  output {
    Array[File] chunks = glob("./kanta*gz")
    File header = "header.txt"
  }
}
