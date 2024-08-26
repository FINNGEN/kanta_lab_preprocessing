import pandas as pd
import re
import numpy as np


def unit_fixing(df,args):

    df = (
        df
        .pipe(lab_unit_filter,args)
        .pipe(lab_unit_mapping_func,args)
        .pipe(abnormality_fix,args)
        )
    return df




def abnormality_fix(df,args):

    """
    Fixes abnormality abbreviations to be consistent with the standard definition see AR/LABRA - Poikkeustilanneviestit. This means replacing
< with L, > with H, POS with A and NEG with N.
    If the abbreviation is not one of these, it is replaced with NA.

    if(lab_abnorm == "<") {lab_abnorm = "L";}
    else if(lab_abnorm == ">") { lab_abnorm = "H";}
    else if(lab_abnorm == "POS") {lab_abnorm = "A";}
    else if(lab_abnorm == "NEG") {lab_abnorm = "N";}
    
    if((lab_abnorm != "A") & (lab_abnorm != "AA") & (lab_abnorm != "H") & (lab_abnorm != "HH") & (lab_abnorm != "L") & (lab_abnorm != "LL") & (lab_abnorm != "N")) {lab_abnorm = "NA";}
    """
    col = 'TEST_OUTCOME'
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
    # COPY SO THEN LATER I CAN LOG CHANGES
    old_col = df[[col]].copy()
    # do replacements based on whether a  mask is passed or not
    for rep in args.config['unit_replacements']:
        if map_mask is not None:
            df.loc[~map_mask,col] = df.loc[~map_mask,col].replace(rep[0],rep[1],regex=True)
        else:
            df.loc[:,col] = df.loc[:,col].replace(rep[0],rep[1],regex=True)
    # LOG CHANGES
    unit_df = df[['FINNGENID', 'APPROX_EVENT_DATETIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']].copy()
    unit_df['OLD'] = old_col
    unit_df['SOURCE'] = "regex"    
    unit_mask = (unit_df["OLD"] != unit_df[col])
    unit_df[unit_mask][['FINNGENID', 'APPROX_EVENT_DATETIME','TEST_NAME_ABBREVIATION','OLD','MEASUREMENT_UNIT','SOURCE']].to_csv(args.unit_file, mode='a', index=False, header=False,sep="\t")
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


def lab_unit_mapping_func(df,args):
    if args.unit_map =="map":
        return lab_unit_map(df,args)
    elif args.unit_map == 'regex':
        return lab_unit_regex(df,args)
    else:
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

