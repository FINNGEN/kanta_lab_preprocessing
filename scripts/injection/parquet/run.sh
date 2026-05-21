INPUT_FILE="/home/pete/fg-3/kanta_v3/core/kanta_dev_2026_03_09.txt.gz"
TXT_GZ_FILE="INJECT.txt.gz"
TXT_QUERY="/home/pete/Dropbox/Projects/kanta_lab_preprocessing/scripts/injection/parquet/make_tsv_gzipped.sql"


clickhouse --param_filePathMungedTxtGz $INPUT_FILE --param_outputFile $TXT_GZ_FILE --queries-file $TXT_QUERY | gzip -c > $TXT_GZ_FILE


PARQ="INJECT.parquet"
PARQ_QUERY="/home/pete/Dropbox/Projects/kanta_lab_preprocessing/scripts/injection/parquet/make_parquet.sql"
 clickhouse  --param_filePathCleanTxtGz "$TXT_GZ_FILE"   --queries-file $PARQ_QUERY  > $PARQ
