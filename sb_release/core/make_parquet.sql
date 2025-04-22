SELECT
  ROW_ID,
  FINNGENID,
  SEX,
  EVENT_AGE :: Float64 AS EVENT_AGE,
  concat(APPROX_EVENT_DATETIME, ':00') :: DateTime64(3, 'UTC') AS APPROX_EVENT_DATETIME,
  TEST_NAME,
  nullIf(OMOP_CONCEPT_ID, 'NA') :: Nullable(String) AS OMOP_CONCEPT_ID,
  nullIf(MEASUREMENT_UNIT_HARMONIZED, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT_HARMONIZED,
  nullIf(MEASUREMENT_VALUE_HARMONIZED, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_HARMONIZED,
  nullIf(MEASUREMENT_VALUE_EXTRACTED, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_EXTRACTED,
  nullIf(MEASUREMENT_VALUE_MERGED, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_MERGED,
  nullIf(TEST_OUTCOME, 'NA') :: Nullable(String) AS TEST_OUTCOME,
  nullIf(TEST_OUTCOME_IMPUTED, 'NA') :: Nullable(String) AS TEST_OUTCOME_IMPUTED,
  nullIf(OUTCOME_POS_EXTRACTED, 'NA') :: Nullable(Int8) AS OUTCOME_POS_EXTRACTED,
  nullIf(TEST_OUTCOME_TEXT_EXTRACTED, 'NA') :: Nullable(String) AS TEST_OUTCOME_TEXT_EXTRACTED

  FROM file({filePathCleanTxtGz:String}, TSVWithNames)
	 

-- Make the output deterministic by using ORDER BY on all columns
 ORDER BY ROW_ID

FORMAT Parquet
SETTINGS
    input_format_tsv_use_best_effort_in_schema_inference = 0,
    output_format_parquet_compression_method = 'zstd',
    output_format_parquet_string_as_string = 1
