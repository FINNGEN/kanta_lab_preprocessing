SELECT
  ROW_ID :: Int64 AS ROW_ID,  -- Cast to Int64 to ensure numerical sorting
  FINNGENID,
  SEX,
  EVENT_AGE :: Float64 AS EVENT_AGE,
  concat(APPROX_EVENT_DATETIME, ':00') :: DateTime64(3, 'UTC') AS APPROX_EVENT_DATETIME,
  nullIf(OMOP_CONCEPT_ID, 'NA') :: Nullable(String) AS OMOP_CONCEPT_ID,
  TEST_ID,
  TEST_ID_IS_NATIONAL :: Bool AS TEST_ID_IS_NATIONAL,
  TEST_NAME_SOURCE,
  nullIf(MEASUREMENT_VALUE_SOURCE, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_SOURCE,
  nullIf(MEASUREMENT_UNIT_SOURCE, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT_SOURCE,
  nullIf(MEASUREMENT_STATUS, 'NA') :: Nullable(String) AS MEASUREMENT_STATUS,
  nullIf(REFERENCE_RANGE_GROUP, 'NA') :: Nullable(String) AS REFERENCE_RANGE_GROUP,
  nullIf(REFERENCE_RANGE_LOW_VALUE, 'NA') :: Nullable(Float64) AS REFERENCE_RANGE_LOW_VALUE,
  nullIf(REFERENCE_RANGE_LOW_UNIT, 'NA') :: Nullable(String) AS REFERENCE_RANGE_LOW_UNIT,
  nullIf(REFERENCE_RANGE_HIGH_VALUE, 'NA') :: Nullable(Float64) AS REFERENCE_RANGE_HIGH_VALUE,
  nullIf(REFERENCE_RANGE_HIGH_UNIT, 'NA') :: Nullable(String) AS REFERENCE_RANGE_HIGH_UNIT,
  nullIf(CODING_SYSTEM_ORG, 'NA') :: Nullable(String) AS CODING_SYSTEM_ORG,
  nullIf(CODING_SYSTEM_OID, 'NA') :: Nullable(String) AS CODING_SYSTEM_OID,
  nullIf(SERVICE_PROVIDER_ID, 'NA') :: Nullable(String) AS SERVICE_PROVIDER_ID

FROM file({filePathCleanTxtGz:String}, TSVWithNames)

-- Make the output deterministic by using ORDER BY on all columns
ORDER BY (ROW_ID)

FORMAT Parquet
SETTINGS
    input_format_tsv_use_best_effort_in_schema_inference = 0,
    output_format_parquet_compression_method = 'zstd',
    output_format_parquet_string_as_string = 1
