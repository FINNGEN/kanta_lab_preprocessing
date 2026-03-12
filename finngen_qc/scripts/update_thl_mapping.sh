# https://koodistopalvelu.kanta.fi/codeserver/pages/classification-view-page.xhtml?classificationKey=88&versionKey=120
# download file and run
curl 'https://koodistopalvelu.kanta.fi/codeserver/pages/download?name=120_1387444168447.txt&pKey=pubfiles0' -o thl_lab_id_abbrv_map_UPDATED.txt

OLD_FILE="/home/pete/Dropbox/Projects/kanta_lab_preprocessing/finngen_qc/data/thl_lab_id_abbrv_map.tsv"
NEW_FILE="./thl_new_map.txt"
PARQUET_FILE="/mnt/disks/data/kanta/test/kanta_dev_2026_02_19_extended_columns.parquet"
USAGI_SOURCE=~/Dropbox/Projects/kanta_lab_preprocessing/finngen_qc/data/LABfi_ALL.usagi.csv
OUT_FILE="mapping_changes_with_usagi.txt"


#  BUILD NEW MAPPING
iconv -f ISO-8859-1 -t UTF-8 thl_lab_id_abbrv_map_UPDATED.txt | tr -d '\r' | awk -F';' 'NR==1{print "CodeId\tAbbreviation"} NR>1{abbr=$2; gsub(/ /,"",abbr); print $1"\t"tolower(abbr)}' > $NEW_FILE


#COMPARE NEW AND OLD
python3 - "$OLD_FILE" "$NEW_FILE" << 'EOF'
import pandas as pd
import sys

old, new = [pd.read_csv(p, sep='\t', dtype=str) for p in sys.argv[1:3]]
merged = old.merge(new, on='CodeId', how='outer', suffixes=('_old', '_new'))

res = pd.concat([
    merged[merged['Abbreviation_old'].isna()].assign(change_type='NEW'),
    merged[merged['Abbreviation_new'].isna()].assign(change_type='REMOVED'),
    merged[(~merged['Abbreviation_old'].isna()) & 
           (~merged['Abbreviation_new'].isna()) & 
           (merged['Abbreviation_old'] != merged['Abbreviation_new'])].assign(change_type='CHANGED')
], ignore_index=True).fillna('NA')[['CodeId','Abbreviation_old','Abbreviation_new','change_type']]

res.to_csv('diff_output.tsv', sep='\t', index=False)

with open('diff_output.md', 'w') as f:
    f.write('# Lab Code Differences\n\n| CodeId | Abbreviation_old | Abbreviation_new | Change |\n|--------|-----------------|------------------|--------|\n')
    for _, r in res.iterrows():
        f.write(f'| {r.CodeId} | {r.Abbreviation_old} | {r.Abbreviation_new} | {r.change_type} |\n')

print(f"Done: {sum(res.change_type == 'CHANGED')} changed, {sum(res.change_type == 'NEW')} new, {sum(res.change_type == 'REMOVED')} removed")
EOF


# EXTRACT TEST NAMES IN PARQUET FILE
clickhouse --query "SELECT TEST_NAME,OMOP_CONCEPT_ID, count() as n FROM file('$PARQUET_FILE', 'Parquet') WHERE OMOP_CONCEPT_ID IS NOT NULL GROUP BY TEST_NAME,OMOP_CONCEPT_ID ORDER BY n DESC" > test_name_counts.tsv


# CHECK WHICH CHANGES AFFECT THE DATA AND IF THE NEW TEST NAME IS IN USAGI ALREADY
{
  # 1. Print Header
  echo -e "OLD_TEST\tOMOP_ID\tCOUNT\tNEW_TEST\tIS_IN_USAGI"

  # 2. Process Data Stream
  join -t $'\t' -1 1 -2 2 <(sort test_name_counts.tsv) <(sort -k2 diff_output.tsv) | 
  sort -rgk 3 | 
  cut -f 1,2,3,5 | 
  awk -F'\t' -v usagi_file="$USAGI_SOURCE" '
    BEGIN {
        # Load abbreviations into memory from the CSV (25th column)
        # Using comma separator for the source file
        while ((getline < usagi_file) > 0) {
            split($0, csv_cols, ",");
            abbrv = csv_cols[25];
            gsub(/^[ \t]+|[ \t]+$/, "", abbrv);
            if (length(abbrv) > 0) usagi_map[abbrv] = 1;
        }
        close(usagi_file);
    }
    {
        newtest = $4; 
        gsub(/^[ \t]+|[ \t]+$/, "", newtest);
        
        # Check against memory map instead of disk
        found = (newtest in usagi_map) ? "true" : "false";
        
        print $0 "\t" found
    }
  '
} > "$OUT_FILE"

