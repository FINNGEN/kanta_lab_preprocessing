import pandas as pd
import numpy as np
import re


def filter_minimal(df,args):
    """
    This function collects all functions here
    """
    df = (
        df
        .pipe(initialize_out_cols,args)
        .pipe(remove_spaces)
        .pipe(fix_na,args)
        .pipe(filter_hetu,args)
        .pipe(filter_measurement_status,args)
        .pipe(lab_id_source,args)
        .pipe(get_lab_abbrv,args)
        .pipe(get_service_provider_name,args)
        .pipe(fix_abbreviation,args)
    )
    return df


def fix_abbreviation(df,args):
    """
    Removes characthers from abbreviation
    """
    col = 'TEST_NAME_ABBREVIATION'
    abb_df = df[['FINREGISTRYID', 'TEST_DATE_TIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']].copy()
    pattern = '|'.join(args.config['abbreviation_replacements'])
    df[col] = df[col].replace(pattern,'',regex=True)
    #log changes
    abb_df['new'] = df[col].copy()
    unit_mask = (abb_df[col] != abb_df['new'])
    abb_df[unit_mask].to_csv(args.abbr_file, mode='a', index=False, header=False,sep="\t")
    
    return df



def get_service_provider_name(df,args):
    """
    Updates TEST_SERVICE_PROVIDER based on mapping. NA is default
    """
    df.loc[:,'TEST_SERVICE_PROVIDER'] = df.loc[:,"TEST_SERVICE_PROVIDER"].map(args.config['thl_sote_map'])
    return df


def get_lab_abbrv(df,args):
    """
    It assigns TEST_NAME_ABBREVIATION, keeping the local name if source is local (TEST_ID==0) or mapping it if source is THL (TEST_ID ==1). If the value is missing from the mapping it will be mapped to NA
    N.B.LAB ABBREVIATION is already read on reading from paikallinentutkimusnimike (from config) so no need to create it, just update
    """
    col="TEST_NAME_ABBREVIATION"
    df[col] =df[col].str.lower()     #fix lab abbrevation in general before updated mapping
    mask = df.TEST_ID_SOURCE == "1"
    df.loc[mask,col] = df.loc[mask,"TEST_ID"].map(args.config['thl_lab_map'])
    df[col] = df[col].str.replace('"', '')     # remove single quotes
    return df


def lab_id_source(df,args):
    """
    Update/create TEST_ID and TEST_ID SOURCE.
    In this function we uses local_lab_id (paikallinentutkimusnimikeid)  and thl lab_id (laboratoriotutkimusnimikeid) if possible.
    """
    
    local_mask =  (df['laboratoriotutkimusnimikeid'] == 'NA')
    df["TEST_ID_SOURCE"] = np.where(local_mask,"0","1")
    df["TEST_ID"] = np.where(local_mask,df.paikallinentutkimusnimikeid,df.laboratoriotutkimusnimikeid)
    return df
    
def filter_measurement_status(df,args):
    """
    Here we remove values that are not in the accepted value list.
    """
    col,problematic_values=args.config['problematic_status']
    err_mask = df[col].isin(problematic_values)
    err_df = df[err_mask].copy()
    err_df.loc[:,'ERR'] = 'measurement_status'
    err_df.loc[:,'ERR_VALUE'] = err_df.loc[:,col]
    err_df[args.config['err_cols']].to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[~err_mask]

def filter_hetu(df,args):
    """
    Filters out if hetu root is incorrect
    """
    err_mask = df['hetu_root'] != args.config['hetu_kw']
    err_df = df[err_mask].copy()
    err_df.loc[:,'ERR'] = 'hetu_root'
    err_df.loc[:,'ERR_VALUE'] = err_df.loc[:,'hetu_root']
    err_df[args.config['err_cols']].to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[~err_mask]
    
def fix_na(df,args):
    """
    Fixes NAs across columns.
    -1 can be a valid entry for the actual result of the lab analysis so we need to skip that column
    """
    # get special exclusion values dictionary
    exception_columns = set(args.config['NA_map'].keys())
    for col in exception_columns:
        df[col] = df[col].replace(args.config['NA_map'][col],"NA")
    #apply the basic one to all other columns
    other_cols = df.columns.difference(exception_columns)
    df[other_cols] = df[other_cols].replace(args.config['NA_kws'],"NA")
    return df

def remove_spaces(df):
    """
    Trim whitespace from ends of each value across all series in dataframe.
    In testing sometimes fields are empty strings, so I will replace those cases with NA. Gotta check if it's the case with real data too
 
    """
    for col in df.columns:
        # removes all spaces (including inside text, kinda messess up date, but fixes issues across the board.
        df[col] = df[col].str.strip().str.replace(r'\s', '', regex=True).fillna("NA") # this removes ALL spaces
        # only trailing/leading
        #df[col] = df[col].str.strip().str.replace(r"^ +| +$", r"", regex=True).fillna("NA")
    return df


def initialize_out_cols(df,args):
    #Makes sure that the columns for output exist

    # These columns need be copied back to original name
    df = df.rename(columns = args.config['rename_cols'])
    for col in args.config['source_cols']:
        df[col +"_SOURCE"] = df[col]
    for col in args.config['out_cols'] + args.config['err_cols']:
        if col not in df.columns.tolist():
            df[col] = "NA"
    return df
