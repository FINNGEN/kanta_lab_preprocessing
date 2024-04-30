import pandas as pd
import re

def unit_fixing(df,args):

    df = (
        df
        .pipe(lab_unit_filter,args)
        .pipe(lab_unit_regex,args)
        .pipe(abnormality_fix,args)
        )
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
    col = 'LAB_ABNORMALITY'
    # update values based on mapping
    map_mask = df[col].isin(args.config['fix_units'][col])
    df.loc[map_mask,col] = df.loc[map_mask,col].map(args.config['fix_units'][col])
    accepted_values = [elem for elem in args.config['fix_units'][col].values()]
    accepted_values += [ elem + elem for elem in accepted_values]
    map_mask = ~df[col].isin(accepted_values)
    df.loc[map_mask,col] = "NA"

    return df


def lab_unit_regex(df,args):

    col ='LAB_UNIT'
    # copy lab unit to new df before changing them
    unit_df = df[[col]].copy()
    for rep in args.config['unit_replacements']:
        df[col] = df[col].replace(rep[0],rep[1],regex=True)

    # LOG CHANGES
    unit_df['new'] = df[col].copy()
    unit_mask = (unit_df[col] != unit_df['new'])
    unit_df[unit_mask][['LAB_UNIT','new']].to_csv(args.unit_file, mode='a', index=False, header=False,sep="\t")

    return df

def lab_unit_filter(df,args):
    '''
    Fixes strange characters in lab unit field
    '''
    col = 'LAB_UNIT'
    values = args.config['fix_units'][col]
    regex = r'(' + '|'.join([re.escape(x) for x in values]) + r')'
    df[col] = df[col].replace(regex,"",regex=True)
    return df

