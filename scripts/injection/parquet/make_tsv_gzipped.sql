SELECT
  ROW_ID :: Int64 AS ROW_ID,  -- Cast to Int64 to ensure numerical sorting
  FINNGENID,
  SEX,
  EVENT_AGE,
  APPROX_EVENT_DATETIME,
  
  -- OMOP harmonization
  if(`harmonization_omop::OMOP_ID` IN ('-1', '0'), 'NA', `harmonization_omop::OMOP_ID`) AS OMOP_CONCEPT_ID,
    -- Test identification
    TEST_ID,
    TEST_ID_IS_NATIONAL,
    
    -- Test names (both cleaned and source)
    `cleaned::TEST_NAME_ABBREVIATION` AS TEST_NAME,
    `source::TEST_NAME_ABBREVIATION` AS TEST_NAME_SOURCE,
    
    `cleaned::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_CLEANED,
    `cleaned-pre-fix::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_PRE_FIX,
    
    -- Measurement values (harmonized, extracted, merged, and source)
    `harmonization_omop::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_HARMONIZED,
    `harmonization_omop::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_HARMONIZED,
    `extracted::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_EXTRACTED,
    `extracted::MEASUREMENT_VALUE_MERGED` AS MEASUREMENT_VALUE_MERGED,
    `source::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_SOURCE,
    `source::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_SOURCE,
    -- Coding systems
    CODING_SYSTEM_MAP AS CODING_SYSTEM_ORG,
    CODING_SYSTEM AS CODING_SYSTEM_OID
    FROM file({filePathMungedTxtGz:String}, TSVWithNames) kanta_lab_table
-- Set output format to TSV-gzipped with a header
FORMAT TSVWithNames
	  
-- Disable data type inference from TSV text file
SETTINGS input_format_tsv_use_best_effort_in_schema_inference = 0
