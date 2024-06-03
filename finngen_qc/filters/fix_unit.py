import pandas as pd
import re

def unit_fixing(df,args):

    df = (
        df
        .pipe(lab_unit_filter,args)
        .pipe(lab_unit_map,args)
        .pipe(abnormality_fix,args)
        .pipe(replace_abnormality,args)
        .pipe(check_percentage,args)
        )
    return df



def check_percentage(df,args):
    """
    Removes % sign from abbreviations ending with it but also checks that the unit is indeed % (or NA)
    """
    pattern = args.config['percentage']['pattern']
    col = 'TEST_NAME_ABBREVIATION'
    mask = (df[col].str.contains(pattern) & df['MEASUREMENT_UNIT'].isin(args.config['percentage']['values']))
    
    abb_df = df[['FINREGISTRYID', 'TEST_DATE_TIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']].copy()
    df.loc[mask,col] = df.loc[mask,col].replace(pattern,"",regex=True)
    abb_df['new'] = df[col].copy()
    unit_mask = (abb_df[col] != abb_df['new'])
    abb_df[unit_mask].to_csv(args.abbr_file, mode='a', index=False, header=False,sep="\t")

    
    return df

def replace_abnormality(df,args):
    """
    TODO:
     Moves lab unit information on abnormality to lab abnormality column and lab abnormality information to lab value column for binary tests where abnormality is the only information.
    """
    return df

def abnormality_fix(df,args):

    """
    Fixes abnormality abbreviations to be consistent with the standard definition see AR/LABRA - Poikkeustilanneviestit. This means replacing
< with L, > with H, POS with A and NEG with N.
If the abbreviation is not one of these, it is replaced with NA.

    if(lab_abnorm == "<") {
        lab_abnorm = "L";
    } else if(lab_abnorm == ">") {
        lab_abnorm = "H";
    } else if(lab_abnorm == "POS") {
        lab_abnorm = "A";
    } else if(lab_abnorm == "NEG") {
        lab_abnorm = "N";
    }
    
    if((lab_abnorm != "A") & (lab_abnorm != "AA") & (lab_abnorm != "H") & (lab_abnorm != "HH") & (lab_abnorm != "L") & (lab_abnorm != "LL") & (lab_abnorm != "N")) {
        lab_abnorm = "NA";
    }
    """
    col = 'RESULT_ABNORMALITY'
    # update values based on mapping
    map_mask = df[col].isin(args.config['fix_units'][col])
    df.loc[map_mask,col] = df.loc[map_mask,col].map(args.config['fix_units'][col])
    accepted_values = [elem for elem in args.config['fix_units'][col].values()]
    accepted_values += [ elem + elem for elem in accepted_values]
    df.loc[~df[col].isin(accepted_values),col] = "NA"

    return df



def lab_unit_regex(df,args,map_mask=None):
    """
    Function that replaces unit values via regex. It can work standalone or conjoint with the replacement via map
    """
    
    col ='MEASUREMENT_UNIT'
    unit_df = df[['FINREGISTRYID', 'TEST_DATE_TIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']].copy()
    # REGEX SUBSTIUTION
    for rep in args.config['unit_replacements']:
        if map_mask is not None:
            df.loc[~map_mask,col] = df.loc[~map_mask,col].replace(rep[0],rep[1],regex=True)
        else:
            df.loc[:,col] = df.loc[:,col].replace(rep[0],rep[1],regex=True)

    # LOG CHANGES
    
    unit_df['new'] = df[col].copy()
    unit_mask = (unit_df[col] != unit_df['new'])
    unit_df[unit_mask].to_csv(args.unit_file, mode='a', index=False, header=False,sep="\t")
    
    return df


def lab_unit_map(df,args):
    """
    Fixes units based on approved mapping and uses regex for other
    """
    
    col ='MEASUREMENT_UNIT'
    map_mask = df[col].isin(args.config['unit_map'])
    df.loc[map_mask,col] = df.loc[map_mask,col].map(args.config['unit_map'])

    df = lab_unit_regex(df,args,map_mask)
    return df


def lab_unit_filter(df,args):
    '''
    Fixes strange characters in lab unit field. Also moves to lower case for non NA values.
    '''
    col = 'MEASUREMENT_UNIT'
    values = args.config['fix_units'][col]
    regex = r'(' + '|'.join([re.escape(x) for x in values]) + r')'
    df[col] = df[col].replace(regex,"",regex=True).replace(r'^\s*$',"NA", regex=True)
    na_mask = df[col] != "NA"
    df.loc[na_mask,col] = df.loc[na_mask,col].str.lower()
    return df

