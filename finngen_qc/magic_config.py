config = {
    # DIRECT COLUMN MAPPING
    'rename_cols' : {
        'FINNGENID':                      'FINNGENID',
        'EVENT_AGE' :                     'AGE_AT_MEASUREMENT',
        'tutkimuskoodistonjarjestelmaid': 'TEST_SERVICE_PROVIDER',
        'paikallinentutkimusnimike':      'TEST_NAME_ABBREVIATION',
        'tutkimustulosarvo':              'MEASUREMENT_VALUE',
        'tutkimustulosyksikko':           'MEASUREMENT_UNIT',
        'tutkimusvastauksentilaid':       'MEASUREMENT_STATUS',
        'tuloksenpoikkeavuusid':          'RESULT_ABNORMALITY',
        'viitearvoryhma':                 'TEST_REFERENCE_GROUP',
        'viitevalialkuarvo':              'TEST_REFERENCE_MIN_VALUE',
        'viitevalialkuyksikko':           'TEST_REFERENCE_MIN_UNIT',
        'viitevaliloppuarvo':             'TEST_REFERENCE_MAX_VALUE',
        'viitevaliloppuyksikko':          'TEST_REFERENCE_MAX_UNIT',
        
        
    },
    "source_cols" : ['MEASUREMENT_VALUE','MEASUREMENT_UNIT','TEST_NAME_ABBREVIATION'],
    # ACCESSORY COLUMNS
    'other_cols' : ['paikallinentutkimusnimikeid','laboratoriotutkimusnimikeid','APPROX_EVENT_DAY','TIME'],
    # Cols used for sorting in the wdl.
    # N.B. the order is important as it is kept in the grepping!
    'sort_cols' : ['FINREGISTRYID','APPROX_EVENT_DAY','TIME','paikallinentutkimusnimike','tutkimusvastauksentilaid'],
    
    # LIST OF OUTPUT COLUMNS TO INCLUDE (VALUES ABOVE PLUS NEWLY GENERATED COLUMNS)
    'out_cols' : ['FINNGEN_ID', 'TEST_DATE_TIME','AGE_AT_MEASUREMENT','TEST_SERVICE_PROVIDER', 'TEST_ID','TEST_ID_SOURCE','TEST_NAME_ABBREVIATION', 'MEASUREMENT_VALUE', 'MEASUREMENT_UNIT', 'RESULT_ABNORMALITY',  'MEASUREMENT_STATUS','TEST_REFERENCE_GROUP','TEST_REFERENCE_MIN_VALUE','TEST_REFERENCE_MIN_VALUE','TEST_REFERENCE_MIN_UNIT','TEST_REFERENCE_MAX_VALUE','TEST_REFERENCE_MAX_UNIT','IS_UNIT_VALID','mappingStatus','sourceCode','conceptId','ADD_INFO:omopQuantity','MEASUREMENT_VALUE_SOURCE','MEASUREMENT_UNIT_SOURCE','TEST_NAME_ABBREVIATION_SOURCE'],
    'err_cols':['FINREGISTRYID','TEST_DATE_TIME','ERR','ERR_VALUE'],
    
    'date_time_format': "%Y-%m-%dT%H:%M:%S",


    #REJECTION LINES
    'NA_kws': ['Puuttuu','""',"TYHJÄ","_","NULL","-1"], # FOR ALL COLUMNS DEFAULT
    'NA_map' : {'MEASUREMENT_VALUE':['Puuttuu','""',"TYHJÄ","_","NULL"]}, #SPECIFIC COLUMN EXCEPTIONS
    # hetu_root required value
    'hetu_kw' : '1.2.246.21',
    # MEASUREMENT STATUS
    'problematic_status':  ('MEASUREMENT_STATUS',['K','W','X','I','D','P']),
    # DEFAULT PATHS TO MAP FILES FOR LAB ABBREVIATIONS/ID
    'thl_lab_map_file' : 'data/thl_lab_id_abbrv_map.tsv',
    'thl_sote_map_file' : 'data/thl_sote_map_named.tsv',
    'unit_map_file' : 'data/unit_mapping.txt',
    # VALUES TO REMOVE/FIX FOR LAB UNIT/ABNORMALITY
    'fix_units':{'MEASUREMENT_UNIT':[' ','_',',','.','-','(',')','{','}',"\\",'?','!'],'RESULT_ABNORMALITY':{'<':'L','>':'H',"POS":"A","NEG":"N"}},
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
    'abbreviation_replacements': [
        '_|\\*|#|%',
        '^\\d{4},',
        ',\\d{4}$',
    ],
    'harmonization_repo':'https://raw.githubusercontent.com/FINNGEN/kanta_lab_harmonisation_public/main/MAPPING_TABLES/',
    #list of harmonization files along with columns to use
    'harmonization_files' : {'usagi_units':[['sourceCode'],'UNITSfi.usagi.csv'],'unit_abbreviation_fix':[None,'fix_unit_based_in_abbreviation.tsv'],'usagi_mapping':[['mappingStatus','sourceCode','conceptId','ADD_INFO:omopQuantity'],'LABfi_ALL.usagi.csv']}
    
}
