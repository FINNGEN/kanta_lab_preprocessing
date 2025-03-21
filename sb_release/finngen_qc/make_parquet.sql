SELECT
    FINNGENID,
    SEX,
    EVENT_AGE :: Float64 AS EVENT_AGE,
    concat(APPROX_EVENT_DATETIME, ':00') :: DateTime64(3, 'UTC') AS APPROX_EVENT_DATETIME,
    TEST_NAME,
    TEST_ID,
    TEST_ID_IS_NATIONAL :: Bool AS TEST_ID_IS_NATIONAL,
    nullIf(OMOP_CONCEPT_ID, 'NA') :: Nullable(String) AS OMOP_CONCEPT_ID,
    nullIf(MEASUREMENT_VALUE, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE,
    nullIf(MEASUREMENT_UNIT, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT,
    nullIf(MEASUREMENT_VALUE_HARMONIZED, 'NA') :: Nullable(Float64) AS MEASUREMENT_VALUE_HARMONIZED,
    nullIf(MEASUREMENT_UNIT_HARMONIZED, 'NA') :: Nullable(String) AS MEASUREMENT_UNIT_HARMONIZED,
    nullIf(TEST_OUTCOME, 'NA') :: Nullable(String) AS TEST_OUTCOME,
    nullIf(TEST_OUTCOME_IMPUTED, 'NA') :: Nullable(String) AS TEST_OUTCOME_IMPUTED,
    nullIf(MEASUREMENT_STATUS, 'NA') :: Nullable(String) AS MEASUREMENT_STATUS,
    nullIf(REFERENCE_RANGE_GROUP, 'NA') :: Nullable(String) AS REFERENCE_RANGE_GROUP,
    nullIf(REFERENCE_RANGE_LOW_VALUE, 'NA') :: Nullable(Float64) AS REFERENCE_RANGE_LOW_VALUE,
    nullIf(REFERENCE_RANGE_LOW_UNIT, 'NA') :: Nullable(String) AS REFERENCE_RANGE_LOW_UNIT,
    nullIf(REFERENCE_RANGE_HIGH_VALUE, 'NA') :: Nullable(Float64) AS REFERENCE_RANGE_HIGH_VALUE,
    nullIf(REFERENCE_RANGE_HIGH_UNIT, 'NA') :: Nullable(String) AS REFERENCE_RANGE_HIGH_UNIT,
    nullIf(CODING_SYSTEM_ORG, 'NA') :: Nullable(String) AS CODING_SYSTEM_ORG,
    nullIf(CODING_SYSTEM_OID, 'NA') :: Nullable(String) AS CODING_SYSTEM_OID

FROM file({filePathCleanTxtGz:String}, TSVWithNames)

-- Make the output deterministic by using ORDER BY on all columns
ORDER BY (
    FINNGENID,
    SEX,
    APPROX_EVENT_DATETIME,
    EVENT_AGE,
    OMOP_CONCEPT_ID,
    TEST_NAME,
    TEST_ID,
    TEST_ID_IS_NATIONAL,
    MEASUREMENT_STATUS,
    TEST_OUTCOME,
    TEST_OUTCOME_IMPUTED,
    MEASUREMENT_VALUE,
    MEASUREMENT_UNIT,
    MEASUREMENT_VALUE_HARMONIZED,
    MEASUREMENT_UNIT_HARMONIZED,
    REFERENCE_RANGE_GROUP,
    REFERENCE_RANGE_LOW_VALUE,
    REFERENCE_RANGE_LOW_UNIT,
    REFERENCE_RANGE_HIGH_VALUE,
    REFERENCE_RANGE_HIGH_UNIT,
    CODING_SYSTEM_OID,
    CODING_SYSTEM_ORG
)

FORMAT Parquet
SETTINGS
    input_format_tsv_use_best_effort_in_schema_inference = 0,
    output_format_parquet_compression_method = 'zstd',
    output_format_parquet_string_as_string = 1
