SELECT
  ROW_ID :: Int64 AS ROW_ID,  -- Cast to Int64 to ensure numerical sorting
  FINNGENID,
  SEX,
  EVENT_AGE,
  APPROX_EVENT_DATETIME,
  multiIf(`harmonization_omop::OMOP_ID` = '-1', 'NA', `harmonization_omop::OMOP_ID` = '0', 'NA', `harmonization_omop::OMOP_ID`) AS OMOP_CONCEPT_ID,
  TEST_ID,
  TEST_ID_IS_NATIONAL,
  `source::TEST_NAME_ABBREVIATION` as TEST_NAME_SOURCE,
  `source::MEASUREMENT_VALUE` AS MEASUREMENT_VALUE_SOURCE,
  `source::MEASUREMENT_UNIT` AS MEASUREMENT_UNIT_SOURCE,
  MEASUREMENT_STATUS,
  REFERENCE_RANGE_GROUP,
  REFERENCE_RANGE_LOWER_VALUE AS REFERENCE_RANGE_LOW_VALUE,
  REFERENCE_RANGE_LOWER_UNIT AS REFERENCE_RANGE_LOW_UNIT,
  REFERENCE_RANGE_UPPER_VALUE AS REFERENCE_RANGE_HIGH_VALUE,
  REFERENCE_RANGE_UPPER_UNIT AS REFERENCE_RANGE_HIGH_UNIT,
  CODING_SYSTEM_MAP AS CODING_SYSTEM_ORG,
  CODING_SYSTEM AS CODING_SYSTEM_OID,
  SERVICE_PROVIDER_ID
    
  FROM file({filePathMungedTxtGz:String}, TSVWithNames) kanta_lab_table
	 
  -- Sorting using all columns to make the output deterministic.
 ORDER BY (ROW_ID)
	  
  -- Set output format to TSV-gzipped with a header.
	  FORMAT TSVWithNames
	  
  -- Disable data type inference from TSV text file.
	  SETTINGS input_format_tsv_use_best_effort_in_schema_inference = 0
	  
