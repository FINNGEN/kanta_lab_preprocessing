#!/bin/bash
set -e

# === CONFIGURE PATHS ===
BASE_DIR="/mnt/disks/data/kanta/test"
PARQUET_DATA="$BASE_DIR/kanta_v3_harmonized_2026_03_06_formatted.parquet"
PARQUET_TESTS="$BASE_DIR/finngen_R14_kanta_lab_1.0_extended_columns.parquet"

OUT_CF_COUNTS="$BASE_DIR/omop_convfactor_counts.tsv"
OUT_AMBIGUOUS="$BASE_DIR/ambiguous_omop_cf_ids_sorted.txt"
OUT_TEST_COUNTS="$BASE_DIR/testname_omopid_count_sorted.tsv"
OUT_INJECTION="$BASE_DIR/injection_candidates.txt"

echo "FETCH AMBIGUOUS IDS/CONVERSION FACTORS"
clickhouse -q "
SELECT 
  OMOP_CONCEPT_ID,
  CONVERSION_FACTOR,
  count() AS cnt 
FROM file('$PARQUET_DATA', 'Parquet')
WHERE MEASUREMENT_UNIT_CLEANED IS NOT NULL 
  AND MEASUREMENT_VALUE_HARMONIZED IS NOT NULL 
  AND CONVERSION_FACTOR IS NOT NULL 
GROUP BY OMOP_CONCEPT_ID, CONVERSION_FACTOR 
ORDER BY OMOP_CONCEPT_ID, cnt DESC
" --format=TabSeparated > "$OUT_CF_COUNTS"
wc "$OUT_CF_COUNTS"
head "$OUT_CF_COUNTS"

echo "BUILD AMBIGUOUSNESS TABLE BASED ON CONVERSION FACTOR PREVALENCE > 5% (INCLUDE TOTAL COUNT)"
python3 -c "
from collections import defaultdict
d, t = defaultdict(list), defaultdict(int)
with open('$OUT_CF_COUNTS') as f:
    for l in f:
        omop, cf, cnt = l.rstrip().split('\t')
        cnt = int(cnt)
        d[omop].append((cf, cnt))
        t[omop] += cnt
for omop in sorted(d, key=int):
    shares = {cf: round(c / t[omop], 2) for cf, c in d[omop]}
    threshold = 0.05
    ambiguous = 1 if len(shares) > 1 and sorted(shares.values(), reverse=True)[1] > threshold else 0
    total = t[omop]
    print(f'{omop}\t' + '{' + ','.join(f'\"{cf}\":{v}' for cf, v in shares.items()) + f'}}\t{ambiguous}\t{total}')
" | sort > "$OUT_AMBIGUOUS"
head "$OUT_AMBIGUOUS"
wc "$OUT_AMBIGUOUS"

echo "GET ALL EXTRACTED TESTS COUNTS"
clickhouse -q "
SELECT 
    TEST_NAME,
    OMOP_CONCEPT_ID,
    count() AS COUNT
FROM file('$PARQUET_TESTS', 'Parquet')
WHERE MEASUREMENT_VALUE_EXTRACTED IS NOT NULL
GROUP BY TEST_NAME, OMOP_CONCEPT_ID
ORDER BY COUNT DESC
" --format=TabSeparated | sort -k2,2 > "$OUT_TEST_COUNTS"

wc "$OUT_TEST_COUNTS"
head "$OUT_TEST_COUNTS"


echo "JOIN AND FILL MISSING"
join -t $'\t' -1 2 -2 1 -a 1 "$OUT_TEST_COUNTS" "$OUT_AMBIGUOUS" \
| sort -grk3 \
| awk -F'\t' -v OFS='\t' '{for(i=1;i<=5;i++) if($i=="") $i=0; print $1,$2,$3,$4,$5}' > "$OUT_INJECTION"

echo "SAMPLE JOINT OUTPUT"
head "$OUT_INJECTION"
wc "$OUT_INJECTION"

echo "SUM & RELATIVE FRACTION OF PROBLEMATIC VS UNPROBLEMATIC (BY COLUMN 5)"

awk -F'\t' '
{
  # $3 = count, $5 = ambiguous flag
  if ($5==1) prob+=$3; else nonprob+=$3
}
END {
  total = prob + nonprob;
  printf "Problematic: %d (%.2f%%)\n", prob, (prob/total*100);
  printf "Non-problematic: %d (%.2f%%)\n", nonprob, (nonprob/total*100);
}
' "$OUT_INJECTION"
