config = {
    # DIRECT COLUMN MAPPING
    'rename_cols' : {
        'potilashenkilotunnus':           'FINREGISTRYID',
        'tutkimusaika':                   'LAB_DATE_TIME',
        'palvelutuottaja_organisaatio'  : 'LAB_SERVICE_PROVIDER',
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
    'out_cols' : ['FINREGISTRYID', 'LAB_DATE_TIME', 'LAB_SERVICE_PROVIDER', 'LAB_ID','LAB_ID_SOURCE','LAB_ABBREVIATION', 'LAB_VALUE', 'LAB_UNIT', 'LAB_ABNORMALITY', 'REFERENCE_VALUE_TEXT', 'MEASUREMENT_STATUS'],
    'err_cols':['FINREGISTRYID','LAB_DATE_TIME','ERR','ERR_VALUE'],
    #REJECTION LINES
    'NA_kws': ['Puuttuu','""',"TYHJÄ","_","NULL","-1"], # FOR ALL COLUMNS DEFAULT
    'NA_map' : {'LAB_VALUE':['Puuttuu','""',"TYHJÄ","_","NULL"]}, #SPECIFIC COLUMN EXCEPTIONS
    # hetu_root required value
    'hetu_kw' : '1.2.246.21',
    # MEASUREMENT STATUS
    'problematic_status':  ('MEASUREMENT_STATUS',['K','W','X','I','D','P']),
    # DEFAULT PATHS TO MAP FILES FOR LAB ABBREVIATIONS/ID
    'thl_lab_map_file' : 'data/thl_lab_id_abbrv_map.tsv',
    'thl_sote_map_file' : 'data/thl_sote_map_named.tsv',
    # VALUES TO REMOVE/FIX FOR LAB UNIT/ABNORMALITY
    'fix_units':{'LAB_UNIT':[' ','_',',','.','-','(',')','{','}',"\\",'?','!'],'LAB_ABNORMALITY':{'<':'L','>':'H',"POS":"A","NEG":"N"}},
    # BIG REGEX FOR LAB UNIT
    'unit_replacements' : [
        (r"(^\*+$|^$)","NA"),
        (r'(^(\b)?\d+(?=e\d+))',""),
        (r"(à?x?(10)?e0?(?=\d)|x?10(\^|\*)|^\^(?=[0-9]+.?l))","e"),
        (r"(y|µ)ks(ikkö)?","u"),
        (r"y","µ"),
        (r"lµ","ly"),
        (r"tehtµ","tehty"),
        (r"ug","µg"),
        (r"m([a-z]?)µ","mµ"),
        (r"^mµ.?l$","mµ/l"),
        (r"^µ.?l$","µ/l"),
        (r"^u.?l$","u/l"),
        (r"umol","µmol"),
        (r"^µmol.?l$","µmol/l"),
        (r"^(µ|u)g.?l$","µg/l"),
        (r"^(m)?mmo(l)?/","mmol/"),
        (r"(mo(t|l|i)?(l)?)(?=$)|nol","mol"),
        (r"^mmol.?(l|i).?$","mmol/l"),
        (r"krea",""),
        (r"^mmol.?mol.?$","mmol/mol"),
        (r"(^(m)?m(h)?/h$|^mh.?h$)","mm/h"),
        (r"^.?mg.?l$","mg/l"),
        (r"^ml/min.*","ml/min/1.73m2"),
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
        (r"((u|µ)g/g(\s+)?stool|(u|µ)g/g(f)?)","µg/g"),
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
    ]

}
