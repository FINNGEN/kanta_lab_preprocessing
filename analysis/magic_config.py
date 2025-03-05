config = {
    'cols' :    [
        'FINNGENID',
        'EVENT_AGE',
        'APPROX_EVENT_DATETIME',
        'TEST_ID',
        'TEST_ID_IS_NATIONAL',
        'CODING_SYSTEM',
        'CODING_SYSTEM_MAP',
        'TEST_OUTCOME',
        'imputed::TEST_OUTCOME',
        'MEASUREMENT_STATUS',
        'REFERENCE_RANGE_GROUP',
        'REFERENCE_RANGE_LOWER_VALUE',
        'REFERENCE_RANGE_LOWER_UNIT',
        'REFERENCE_RANGE_UPPER_VALUE',
        'REFERENCE_RANGE_UPPER_UNIT',
        'cleaned::TEST_NAME_ABBREVIATION',
        'cleaned::MEASUREMENT_VALUE',
        'cleaned::MEASUREMENT_UNIT',
        'harmonization_omop::MEASUREMENT_VALUE',
        'harmonization_omop::MEASUREMENT_UNIT',
        'harmonization_omop::CONVERSION_FACTOR',
        'harmonization_omop::IS_UNIT_VALID',
        'harmonization_omop::mappingStatus',
        'harmonization_omop::sourceCode',
        'harmonization_omop::OMOP_ID',
        'harmonization_omop::omopQuantity',
        'source::MEASUREMENT_VALUE',
        'source::MEASUREMENT_UNIT',
        'source::TEST_NAME_ABBREVIATION',
        'MEASUREMENT_EXTRA_INFO',
        'MEASUREMENT_FREE_TEXT',
        'SERVICE_PROVIDER_ID',
        'SEX'
    ],
    'added_cols': [
        "extracted::MEASUREMENT_VALUE",
        "extracted::IS_MEASUREMENT_EXTRACTED",
        "extracted::IS_POS"
     ],
    'err_cols':['FINNGENID','APPROX_EVENT_DATETIME','ERR','ERR_VALUE'],
    'omop_unit_map':'finngen_qc/data/harmonization_counts.txt',
    'posneg_map':'analysis/data/negpos_mapping.tsv',
    'date_time_format': "%Y-%m-%dT%H:%M:%S",
    'free_text_measurement_replacements': [
        (r'\*', ''),
        (r',', '.') #replace commas with dots 
    ],
    'free_text_result_strings' : ("tutkimuksentulos:","resultat:","provresultat:")

}
