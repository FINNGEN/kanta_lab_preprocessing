SELECT
  ROW_ID :: Int64 AS ROW_ID,  -- Cast to Int64 to ensure numerical sorting
  FINNGENID,
  SEX,
  EVENT_AGE,
  APPROX_EVENT_DATETIME,
  multiIf(`harmonization_omop::OMOP_ID` = '-1', 'NA', `harmonization_omop::OMOP_ID` = '0', 'NA', `harmonization_omop::OMOP_ID`) AS OMOP_CONCEPT_ID,
  `cleaned::TEST_NAME_ABBREVIATION` AS TEST_NAME,
  `harmonization_omop::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_HARMONIZED,
  `harmonization_omop::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_HARMONIZED,
  `extracted::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_EXTRACTED,
  `extracted::MEASUREMENT_VALUE_MERGED` AS MEASUREMENT_VALUE_MERGED,
  TEST_OUTCOME,
  `imputed::TEST_OUTCOME` AS TEST_OUTCOME_IMPUTED,
  `extracted::TEST_OUTCOME_TEXT` AS TEST_OUTCOME_TEXT_EXTRACTED,
  `extracted::IS_POS` AS OUTCOME_POS_EXTRACTED
    
  FROM file({filePathMungedTxtGz:String}, TSVWithNames) kanta_lab_table
	 
  -- Sorting by ROW_ID
 ORDER BY ROW_ID
	  
  -- Set output format to TSV-gzipped with a header
	  FORMAT TSVWithNames
	  
  -- Disable data type inference from TSV text file.
	  SETTINGS input_format_tsv_use_best_effort_in_schema_inference = 0
