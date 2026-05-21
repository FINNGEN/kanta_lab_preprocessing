SELECT
  ROW_ID :: Int64 AS ROW_ID,  -- Cast to Int64 to ensure numerical sorting
  FINNGENID,
  SEX,
  EVENT_AGE :: Float64 AS EVENT_AGE,
  concat(APPROX_EVENT_DATETIME, ':00') :: DateTime64(3, 'UTC') AS APPROX_EVENT_DATETIME,

  -- OMOP harmonization
  nullIf(OMOP_CONCEPT_ID, 'NA') :: Nullable(String) AS OMOP_CONCEPT_ID,

  -- Test identification
  TEST_ID,
  TEST_ID_IS_NATIONAL :: Bool AS TEST_ID_IS_NATIONAL,

  -- Test names
  TEST_NAME,
  TEST_NAME_SOURCE,

  -- Measurement values
  nullIf(MEASUREMENT_UNIT_HARMONIZED, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT_HARMONIZED,
  nullIf(MEASUREMENT_VALUE_HARMONIZED, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_HARMONIZED,
  nullIf(MEASUREMENT_VALUE_EXTRACTED, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_EXTRACTED,
  nullIf(MEASUREMENT_VALUE_MERGED, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_MERGED,
  nullIf(MEASUREMENT_UNIT_CLEANED, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT_CLEANED,
  nullIf(MEASUREMENT_UNIT_PRE_FIX, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT_PRE_FIX,
  nullIf(MEASUREMENT_VALUE_SOURCE, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_SOURCE,
  nullIf(MEASUREMENT_UNIT_SOURCE, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT_SOURCE,

  -- Coding systems
  nullIf(CODING_SYSTEM_ORG, 'NA') :: Nullable(String) AS CODING_SYSTEM_ORG,
  nullIf(CODING_SYSTEM_OID, 'NA') :: Nullable(String) AS CODING_SYSTEM_OID
    
FROM file({filePathCleanTxtGz:String}, TSVWithNames)
      
FORMAT Parquet
SETTINGS
  input_format_tsv_use_best_effort_in_schema_inference = 0,
  output_format_parquet_compression_method = 'zstd',
  output_format_parquet_string_as_string = 1;
