version 1.0

workflow pre_merge {
  input {
    Boolean test
    File kanta_list
    String prefix
    String version
    
  }
  String docker = "eu.gcr.io/finngen-sandbox-v3-containers/bioinformatics:1.0.1"

  # Remove quotation marks (and test if needed)
  scatter (year_files in read_tsv(kanta_list)) {
    # THIS STEP REMOVES QUOTATION BLOCKS
    call process_file as process_responses {input :input_file= year_files[0],test = test,docker=docker}
    call process_file as process_ft { input :input_file= year_files[1],test = test ,docker=docker}
    call merge_ft {input: responses_file = process_responses.cleaned_file,ft_file = process_ft.cleaned_file,docker=docker}
  }

  call merge_files {input:rr_files = merge_ft.merged_year,out_file = sub(prefix,"VERSION",if test then version +"_test" else version),docker=docker }
  output {
    File merged_kanta =merge_files.merged_file
    }
}

task merge_ft {
  input {
    File responses_file
    File ft_file
    String docker
  }

  String out_file = sub(basename(responses_file),'.txt.gz','_merged.txt.gz')
  command <<<
  set -euo pipefail
  F1="~{responses_file}"
  F2="~{ft_file}"
  OUT="~{out_file}"
  # 1. Get headers safely. 
  # 'head -1' often causes 'zcat' to return exit code 141 (SIGPIPE).
  # '|| true' ensures H1/H2 assignments don't trigger 'set -e'.
  H1=$(zcat -f "$F1" | head -1 || true)
  H2=$(zcat -f "$F2" | head -1 || true)

# Check if we actually got headers before proceeding
  if [[ -z "$H1" || -z "$H2" ]]; then
      echo "Error: Could not read headers from input files." >&2
      exit 1
  fi
  # Get headers and find indices for columns in F2 not in F1
  OFF=$(echo "$H1" | tr '\t' '\n' | wc -l)

  # Join files and process in one AWK pass
  paste <(zcat -f "$F1") <(zcat -f "$F2") | awk -F'\t' -v OFS='\t' -v h1="$H1" -v h2="$H2" -v off="$OFF" '
  BEGIN {
    split(h1, a1); split(h2, a2)
    for(i in a1) map[a1[i]] = i
    for(i in a2) if(a2[i] in map) pairs[map[a2[i]]] = i + off; else new[++n] = i + off
  }
  {
    for(p in pairs) if($p != $pairs[p]) { print "Err line "NR": "$p" != "$pairs[p] > "/dev/stderr"; exit 1 }
    res = $1; for(i=2; i<=off; i++) res = res OFS $i
    for(i=1; i<=n; i++) res = res OFS $(new[i])
    print res
    if(NR%50000==0) printf "\rRow %d", NR > "/dev/stderr"
  }' | gzip > "$OUT"


  >>>
  runtime {
    disks: "local-disk ~{ceil(size(responses_file,'GB')*3) + 10} HDD"
    docker : "~{docker}"
  }
  output {
    File merged_year = out_file
  }
}

task process_file {
  input {
    File input_file
    Boolean test
    String docker
  }
  String base = sub(basename(input_file),'.txt.gz','_cleaned.txt.gz')
  command <<<
  zcat -f ~{input_file} | sed 's/\(^\|\t\)"/\1/g; s/"\(\t\|$\)/\1/g' | tr -d '\r' | awk -F'\t' '/^FG/{if(NR>1)print ""; printf "%s",$0; next} {printf " %s",$0} END{print ""}' | awk -F'\t' 'BEGIN{OFS="\t"} NR==1{cols=NF} {if(NF<cols) for(i=NF+1;i<=cols;i++) $i="NA"; NF=cols; print}' | bgzip -c > ~{base}
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(input_file,'GB')*3) + 10} HDD"
    docker:"~{docker}"
  }
  output {File cleaned_file = base}
   
}

task merge_files {
  input {
    Array[File] rr_files
    String out_file
    String docker
  }
  command <<<
  zcat ~{rr_files[0]} | head -n1 | bgzip -c > ~{out_file}
  while read f; do  echo $f &&  zcat $f | sed -E 1d | bgzip -c >> ~{out_file}; done < ~{write_lines(rr_files)}
  zcat ~{out_file} | wc -l
  >>>
  runtime {
    disks: "local-disk ~{ceil(size(rr_files,'GB'))*3 + 10} HDD"
    docker:"~{docker}"
  }
  output { File merged_file = out_file}
}
