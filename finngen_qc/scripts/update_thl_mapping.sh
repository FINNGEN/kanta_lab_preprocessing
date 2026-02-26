# https://koodistopalvelu.kanta.fi/codeserver/pages/classification-view-page.xhtml?classificationKey=88&versionKey=120
# download file and run
curl 'https://koodistopalvelu.kanta.fi/codeserver/pages/download?name=120_1387444168447.txt&pKey=pubfiles0' -o thl_lab_id_abbrv_map_UPDATED.txt

OLD_FILE="/home/pete/Dropbox/Projects/kanta_lab_preprocessing/finngen_qc/data/thl_lab_id_abbrv_map.tsv"
NEW_FILE="./thl_new_map.txt"
PARQUET_FILE="/mnt/disks/data/kanta/test/kanta_dev_2026_02_19_extended_columns.parquet"
iconv -f ISO-8859-1 -t UTF-8 thl_lab_id_abbrv_map_UPDATED.txt | tr -d '\r' | awk -F';' 'NR==1{print "CodeId\tAbbreviation"} NR>1{abbr=$2; gsub(/ /,"",abbr); print $1"\t"tolower(abbr)}' > $NEW_FILE

python3 - "$OLD_FILE" "$NEW_FILE" << 'EOF'
import pandas as pd
import sys

old_path, new_path = sys.argv[1], sys.argv[2]

old = pd.read_csv(old_path, sep='\t', dtype=str)
new = pd.read_csv(new_path, sep='\t', dtype=str)

m = old.merge(new, on='CodeId', suffixes=('_old', '_new'), how='outer')

missing_old = m[m['Abbreviation_old'].isna()].copy()
missing_new = m[m['Abbreviation_new'].isna()].copy()
changed = m[m['Abbreviation_old'].notna() & m['Abbreviation_new'].notna() & (m['Abbreviation_old'] != m['Abbreviation_new'])].copy()

missing_old['change_type'] = 'NEW'
missing_new['change_type'] = 'REMOVED'
changed['change_type'] = 'CHANGED'

combined = pd.concat([changed, missing_old, missing_new]).fillna('NA')[['CodeId', 'Abbreviation_old', 'Abbreviation_new', 'change_type']]
combined.to_csv('diff_output.tsv', sep='\t', index=False)

with open('diff_output.md', 'w') as f:
    f.write('# Lab Code Differences\n\n')
    f.write('| CodeId | Abbreviation_old | Abbreviation_new | Change |\n')
    f.write('|--------|-----------------|------------------|--------|\n')
    for _, r in combined.iterrows():
        f.write(f'| {r.CodeId} | {r.Abbreviation_old} | {r.Abbreviation_new} | {r.change_type} |\n')

print(f"Done: {len(changed)} changed, {len(missing_old)} new, {len(missing_new)} removed")
EOF


clickhouse --query "SELECT TEST_NAME,OMOP_CONCEPT_ID, count() as n FROM file('$PARQUET_FILE', 'Parquet') WHERE OMOP_CONCEPT_ID IS NOT NULL GROUP BY TEST_NAME,OMOP_CONCEPT_ID ORDER BY n DESC" > test_name_counts.tsv


echo -e "OLD_TEST\tOMOP_ID\tCOUNT\tNEW_TEST" > mapping_changes.txt && join -t $'\t'  -1 1 -2 2  <( sort  test_name_counts.tsv) <(sort -k2 diff_output.tsv )   | sort -rgk 3 | cut -f 1,2,3,5 >> mapping_changes.txt
