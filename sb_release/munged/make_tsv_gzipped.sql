SELECT
  ROW_ID :: Int64 AS ROW_ID,  -- Cast to Int64 to ensure numerical sorting
  FINNGENID,
  SEX,
  EVENT_AGE,
  APPROX_EVENT_DATETIME,
  
  -- OMOP harmonization
  if(`harmonization_omop::OMOP_ID` IN ('-1', '0'), 'NA', `harmonization_omop::OMOP_ID`) AS OMOP_CONCEPT_ID,
  `harmonization_omop::omopQuantity` AS OMOP_QUANTITY,
  `harmonization_omop::mappingStatus` AS MAPPING_STATUS,
  `harmonization_omop::IS_UNIT_VALID` AS IS_UNIT_VALID,
  `harmonization_omop::CONVERSION_FACTOR` AS CONVERSION_FACTOR,

  -- Test identification
  TEST_ID,
  TEST_ID_IS_NATIONAL,
  
  -- Test names (both cleaned and source)
  `cleaned::TEST_NAME_ABBREVIATION` AS TEST_NAME,
  `source::TEST_NAME_ABBREVIATION` AS TEST_NAME_SOURCE,
  
  -- Measurement values (harmonized, cleaned, source, and free text)
  `harmonization_omop::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_HARMONIZED,
  `harmonization_omop::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_HARMONIZED,
  `cleaned::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_CLEANED,
  `cleaned::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_CLEANED,
  `cleaned-pre-fix::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_PRE_FIX,
  `source::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_SOURCE,
  `source::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_SOURCE,
  MEASUREMENT_FREE_TEXT,
  
  -- Test outcomes
  TEST_OUTCOME,
  
  -- Measurement status and reference ranges
  MEASUREMENT_STATUS,
  REFERENCE_RANGE_GROUP,
  REFERENCE_RANGE_LOWER_VALUE AS REFERENCE_RANGE_LOW_VALUE,
  REFERENCE_RANGE_LOWER_UNIT AS REFERENCE_RANGE_LOW_UNIT,
  REFERENCE_RANGE_UPPER_VALUE AS REFERENCE_RANGE_HIGH_VALUE,
  REFERENCE_RANGE_UPPER_UNIT AS REFERENCE_RANGE_HIGH_UNIT,
  
  -- Coding systems
  CODING_SYSTEM_MAP AS CODING_SYSTEM_ORG,
  CODING_SYSTEM AS CODING_SYSTEM_OID
    
FROM file({filePathMungedTxtGz:String}, TSVWithNames) kanta_lab_table
      
-- Set output format to TSV-gzipped with a header
FORMAT TSVWithNames
      
-- Disable data type inference from TSV text file
SETTINGS input_format_tsv_use_best_effort_in_schema_inference = 0
