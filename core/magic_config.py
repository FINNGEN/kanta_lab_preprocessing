config = {
    'cols' :    [
        'ROW_ID',
        'FINNGENID',
        'EVENT_AGE',
        'APPROX_EVENT_DATETIME',
        'TEST_ID',
        'TEST_ID_IS_NATIONAL',
        'CODING_SYSTEM',
        'CODING_SYSTEM_MAP',
        'TEST_OUTCOME',
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
        'harmonization_omop::OMOP_ID',
        'harmonization_omop::omopQuantity',
        'source::MEASUREMENT_VALUE',
        'source::MEASUREMENT_UNIT',
        'source::TEST_NAME_ABBREVIATION',
        'MEASUREMENT_EXTRA_INFO',
        'MEASUREMENT_FREE_TEXT',
        'SERVICE_PROVIDER_ID',
        'STATEMENT_ID',
        'STATEMENT_TEXT',
        'SEX'
        
    ],
    'added_cols': [
        "imputed::TEST_OUTCOME",
        "extracted::MEASUREMENT_VALUE",
        "extracted::MEASUREMENT_VALUE_MERGED",
        "extracted::IS_POS",
        "extracted::TEST_OUTCOME_TEXT"
    ],
    'sensitive_cols':[
        'MEASUREMENT_FREE_TEXT',
        'STATEMENT_ID',
        'STATEMENT_TEXT'
    ],

    'err_cols':['ROW_ID','FINNGENID','APPROX_EVENT_DATETIME','ERR','ERR_VALUE'],
    'dup_cols':['FINNGENID','APPROX_EVENT_DATETIME','harmonization_omop::OMOP_ID','cleaned::TEST_NAME_ABBREVIATION','extracted::MEASUREMENT_VALUE_MERGED','TEST_OUTCOME','extracted::TEST_OUTCOME_TEXT','extracted::IS_POS'],

    'omop_unit_map':'finngen_qc/data/harmonization_counts.txt',
    'posneg_map':'core/data/negpos_mapping.tsv',

    'date_time_format': "%Y-%m-%dT%H:%M:%S",
    'free_text_measurement_replacements': [
        (r'\*', ''),
        (r',', '.') #replace commas with dots 
    ],
    'free_text_result_strings' : ("tutkimuksentulos:", "resultat:", "provresultat:","tutkimuksen tulos:", "tulos:", "vastaus:"),
    'status_indicators' : ('<', '>', 'yli', 'alle'),
    'abnormality_table':"data/abnormality_estimation.table.tsv",

    

}
