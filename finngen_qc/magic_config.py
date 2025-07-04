config = {
    # DIRECT COLUMN MAPPING
    'rename_cols' : {
        'FINNGENID':                      'FINNGENID',
        'EVENT_AGE' :                     'EVENT_AGE',
        'tutkimuskoodistonjarjestelmaid': 'CODING_SYSTEM',
        'paikallinentutkimusnimike':      'TEST_NAME_ABBREVIATION',
        'tutkimustulosarvo':              'MEASUREMENT_VALUE',
        'tutkimustulosyksikko':           'MEASUREMENT_UNIT',
        'tutkimusvastauksentilaid':       'MEASUREMENT_STATUS',
        'tuloksenpoikkeavuusid':          'TEST_OUTCOME',
        'viitearvoryhma':                 'REFERENCE_RANGE_GROUP',
        'viitevalialkuarvo':              'REFERENCE_RANGE_LOWER_VALUE',
        'viitevalialkuyksikko':           'REFERENCE_RANGE_LOWER_UNIT',
        'viitevaliloppuarvo':             'REFERENCE_RANGE_UPPER_VALUE',
        'viitevaliloppuyksikko':          'REFERENCE_RANGE_UPPER_UNIT',
        'tutkimuksenlisatieto' :          'MEASUREMENT_EXTRA_INFO',
        'tutkimustulosteksti':            'MEASUREMENT_FREE_TEXT',
        'antaja_organisaatioid':          'SERVICE_PROVIDER_ID',
        'lausunnontilaid':                'STATEMENT_ID',
   	'lausuntoteksti':                 'STATEMENT_TEXT'  
        
    },
    "source_cols" : ['MEASUREMENT_VALUE','MEASUREMENT_UNIT','TEST_NAME_ABBREVIATION'],
    # ACCESSORY COLUMNS
    'other_cols' : ['paikallinentutkimusnimikeid','laboratoriotutkimusnimikeid','APPROX_EVENT_DAY','TIME','ROW_ID'],
    # Cols used for sorting in the wdl.
    # N.B. the order is important as it is kept in the grepping!
    'sort_cols' : ['FINNGENID','APPROX_EVENT_DAY','TIME','laboratoriotutkimusnimikeid','paikallinentutkimusnimikeid','tutkimusvastauksentilaid','tutkimustulosarvo','tutkimustulosyksikko','tutkimustulosteksti'],
    # LIST OF OUTPUT COLUMNS TO INCLUDE (VALUES ABOVE PLUS NEWLY GENERATED COLUMNS)
    'out_cols' :    [
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
   	'STATEMENT_TEXT'  
    ],
    'cleaned_cols':    [
        'TEST_NAME_ABBREVIATION',
        'MEASUREMENT_VALUE',
        'MEASUREMENT_UNIT',
    ],
    
    'err_cols':['ROW_ID','FINNGENID','APPROX_EVENT_DATETIME','ERR','ERR_VALUE'],
    'date_time_format': "%Y-%m-%dT%H:%M:%S",

    #REJECTION LINES
    'NA_kws': ['Puuttuu','""',"TYHJÄ","_","NULL","-1"], # FOR ALL COLUMNS DEFAULT
    'NA_map' : {'MEASUREMENT_VALUE':['Puuttuu','""',"TYHJÄ","_","NULL"]}, #SPECIFIC COLUMN EXCEPTIONS
    # hetu_root required value
    'hetu_kw' : '1.2.246.21',
    # MEASUREMENT STATUS
    'problematic_status':  ('MEASUREMENT_STATUS',['K','W','X','I','D','P']),
    # SPACES
    'columns_with_spaces':['MEASUREMENT_FREE_TEXT','STATEMENT_TEXT'],
    # DEFAULT PATHS TO MAP FILES FOR LAB ABBREVIATIONS/ID
    'thl_lab_map_file' : 'data/thl_lab_id_abbrv_map.tsv',
    'thl_sote_map_file' : 'data/thl_sote_map_named.tsv',
    'thl_sote_manual_map' : 'data/thl_coding_manual_mapping.txt',
    'unit_map_file' : 'data/unit_mapping.txt',
    # VALUES TO REMOVE/FIX FOR LAB UNIT/ABNORMALITY
    'fix_units':{'MEASUREMENT_UNIT':[' ','_',',','.','-','(',')','{','}',"\\",'?','!'],'TEST_OUTCOME':{'<':'L','>':'H',"POS":"A","NEG":"N"}},
    # BIG REGEX FOR LAB UNIT
    'unit_replacements' : [
        (r"(^\*+$|^$)","NA"),
        (r"\bc\b","°c"),
        (r'(^(\b)?\d+(?=e\d+))',""),
        (r"(à?x?(10)?e0?(?=\d)|x?10(\^|\*)|^\^(?=[0-9]+.?l))","e"),
        (r"(y|µ)ks(ikkö)?","u"),
        (r"y","u"),
        (r"lµ","ly"),
        (r"tehtµ","tehty"),
        (r"µg","ug"),
        (r"m([a-z]?)µ","mu"),
        (r"^mµ.?l$","mu/l"),
        (r"^µ.?l$","u/l"),
        (r"^u.?l$","u/l"),
        (r"µmol","umol"),
        (r"^µmol.?l$","umol/l"),
        (r"^(µ|u)g.?l$","ug/l"),
        (r"^(m)?mmo(l)?/","mmol/"),
        (r"(mo(t|l|i)?(l)?)(?=$)|nol","mol"),
        (r"^mmol.?(l|i).?$","mmol/l"),
        (r"krea",""),
        (r"^mmol.?mol.?$","mmol/mol"),
        (r"(^(m)?m(h)?/h$|^mh.?h$)","mm/h"),
        (r"^.?mg.?l$","mg/l"),
        (r"^ml/min.*","ml/min/173m2"),
        (r"^inrarvo$","inr"),
        (r"^mg/lfeu$","mg/l"),
        (r"^mo(l)?sm/kg.*$","mosm/kgh2o"),
        (r"(^tilo(s)?$|^(til)osuu(s)$)","osuus"),
        (r"(kopio(t)?(a)?|klp|sol(y|µ|u)|sol(y|µ|u)a|pisteet)","kpl"),
        (r"(n(ä)?kö(ke)?k(enttä|entt)?|s(y|µ)n(fält|f)?$)","nk"),
        (r"(^(kpla)/nk|^kpl.?nk$|/nk$)","kpl/nk"),
        (r"^.*ti(i)?t(t)?er(i)?.*$","titre"),
        (r"^elia(u|µ)","eliau"),
        (r"^eliau/m$","eliau/ml"),
        (r"^a(u|µ)/ml$","au/ml"),
        (r"(gulos(t.*)$|gulo)","gstool"),
        (r"((u|µ)g/g(\s+)?stool|(u|µ)g/g(f)?)","ug/g"),
        (r"(^promil(l)?$|^o/oo$)","promille"),
        (r"(^\-$|^negat$|^neg$)","N"),
        (r"(^pos$|^\+$)","A"),
        (r"^p.?g$","pg"),
        (r"^f.?l$","fl"),
        (r"\/\/","/"),
        (r"(c)?aste(c)?","aste"),
        (r"sek","s"),
        (r"ve/","responseequivalent/"),
        (r"^ve$","responseequivalent"),
        (r"aru","au"),
        (r"liter","l"),
        (r"(/d$|/vrk$)","/24h"),
        (r"nk$","field"),
        (r"kpl","u"),
        (r"(lausunto|lomake)","form"),
        (r"indeksi","index"),
        (r"arvio","estimate"),
        (r"suhde","ratio"),
        (r"krt","times"),
        (r"/100le(uk)$","/100leuk"),
        (r"/l(/|)?(4|37c|ph7|ph74)+","/l"),
        (r"nmol(bce)?/mmol","nmol/mmol"),
        (r"^ku/l$","u/ml"),
        (r"^pg/ml$","ng/l"),
        (r"^(µ|u)g/ml$","mg/l"),
        (r'(^\s+$|^$)',"NA")
    ],
    #Regex for abbreviation (from Javier)
    'abbreviation_deletions': [
        '_|\\*|#|%',
        '^\\d{4},',
        ',\\d{4}$'
    ],

    'abbreviation_replacements': [(r'–','-')],
    'harmonization_repo':'https://raw.githubusercontent.com/FINNGEN/kanta_lab_harmonisation_public/adding-formulas-to-units-conversion/MAPPING_TABLES/',
    #list of harmonization files along with columns to use
    'harmonization_files' : {
        'usagi_units':[['sourceCode'],'UNITSfi.usagi.csv'],
        'unit_abbreviation_fix':[['TEST_NAME_ABBREVIATION','source_unit_clean','source_unit_clean_fix'],'fix_unit_based_in_abbreviation.tsv'],
        'usagi_mapping':[['mappingStatus','conceptId','ADD_INFO:omopQuantity','ADD_INFO:testNameAbbreviation','ADD_INFO:measurementUnit'],'LABfi_ALL.usagi.csv'],
        'unit_conversion':[['omop_quantity','source_unit_valid','to_source_unit_valid','conversion','only_to_omop_concepts'],'quantity_source_unit_conversion.tsv']
    },
    
    'harmonization_col_map' : {
        'mappingStatus':'harmonization_omop::mappingStatus',
        'conceptId':'harmonization_omop::OMOP_ID',
        'sourceCode':"harmonization_omop::sourceCode",
        'ADD_INFO:omopQuantity':"harmonization_omop::omopQuantity",
        "omop_quantity":"harmonization_omop::omopQuantity",
        'to_source_unit_valid':"harmonization_omop::MEASUREMENT_UNIT",
        'conversion':"harmonization_omop::CONVERSION_FACTOR",
        'ADD_INFO:testNameAbbreviation':"TEST_NAME_ABBREVIATION",
        'ADD_INFO:measurementUnit':"MEASUREMENT_UNIT"
    }
}
