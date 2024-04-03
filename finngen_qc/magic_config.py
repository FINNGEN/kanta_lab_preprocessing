config = {
    # DIRECT COLUMN MAPPING
    'rename_cols' : {
        'potilashenkilotunnus':           'FINREGISTRYID',
        'tutkimusaika':                   'LAB_DATE_TIME',
        'palverluntuottaja_organisaatio': 'LAB_SERVICE_PROVIDER',
        'paikallinentutkimusnimike':      'LAB_ABBREVIATION',
        'tutkimustulosarvo':              'LAB_VALUE',
        'tutkimustulosyksikko':           'LAB_UNIT',
        'tuloksenpoikkeavuus':            'LAB_ABNORMALITY',
        'viitevaliteksti':                'REFERENCE_VALUE_TEXT',
        'tutkimusvastauksentila':         'MEASUREMENT_STATUS'
    },
    # ACCESSORY COLUMNS
    'other_cols' : ['paikallinentutkimusnimikeid','laboratoriotutkimusnimikeid','hetu_root'],
    # LIST OF OUTPUT COLUMNS TO INCLUDE (VALUES ABOVE PLUS NEWLY GENERATED COLUMNS)
    'out_cols' : ['FINREGISTRYID', 'LAB_DATE_TIME', 'LAB_SERVICE_PROVIDER', 'LAB_ABBREVIATION', 'LAB_VALUE', 'LAB_UNIT', 'LAB_ABNORMALITY', 'REFERENCE_VALUE_TEXT', 'MEASUREMENT_STATUS'],
    #REJECTION LINES
    'NA_kws': ['Puuttuu','""',"TYHJÄ","_","NULL","-1"],
    'NA_map' : {'tutkimustulosarvo':['Puuttuu','""',"TYHJÄ","_","NULL"]},
    # hetu_root required value
    'hetu_kw' : '1.2.246.21',




    
    
}

