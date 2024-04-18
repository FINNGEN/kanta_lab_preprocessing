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
        
    )
    return df

def get_service_provider_name(df,args):
    """
    Updates LAB_SERVICE_PROVIDER based on mapping
    """
    df.loc[:,'LAB_SERVICE_PROVIDER'] = df.loc[:,"LAB_SERVICE_PROVIDER"].map(args.config['thl_sote_map'])
    return df


def get_lab_abbrv(df,args):
    """
    get_lab_abbrv from Kira
    It assigns LAB_ABBREVIATION, keeping the local name if source is local (LAB_ID==0) or mapping it if source is THL (LAB_ID ==1)
    N.B.LAB ABBREVIATION is already read on reading from paikallinentutkimusnimike (from config) so no need to create it, just update
    """
    col="LAB_ABBREVIATION"
    #fix lab abbrevation in general before updated mapping
    df[col] =df[col].str.lower()

    # update using lab_map dictionary in /data/   
    mask = df.LAB_ID_SOURCE != "0"
    df.loc[mask,col] = df.loc[mask,"LAB_ID"].map(args.config['thl_lab_map'])
    # remove single quotes
    df[col] = df[col].str.replace('"', '')
    return df

def lab_id_source(df,args):
    """
    # column idx/name mapping for kira's data
    1	laboratoriotutkimusnimikeid
    11	laboratoriotutkimusoid
    32	paikallinentutkimusnimike
    33	paikallinentutkimusnimikeid
    # from kira
    std::string local_lab_abbrv = remove_chars(line_vec[31], ' ') --> paikallinentutkimusnimike
    std::string local_lab_id = remove_chars(line_vec[32], ' ') -->    paikallinentutkimusnimikeid
    std::string thl_lab_id = remove_chars(line_vec[0], ' ') -->       laboratoriotutkimusnimikeid

    In this function she uses local_lab_id (paikallinentutkimusnimikeid)  and thl lab_id (laboratoriotutkimusnimikeid)
    """
    # if id is local assign local lab id ekse assign national THL value
    
    local_mask =  df['laboratoriotutkimusnimikeid'] == 'NA'
    df["LAB_ID_SOURCE"] = np.where(local_mask,"0","1")
    df["LAB_ID"] = np.where(local_mask,df.paikallinentutkimusnimikeid,df.laboratoriotutkimusnimikeid)

    #print(df[['LAB_ID_SOURCE',"LAB_ID","laboratoriotutkimusnimikeid","paikallinentutkimusnimikeid"]].value_counts().reset_index(name='count'))
    #print(df[['LAB_ID_SOURCE',"LAB_ID","laboratoriotutkimusnimikeid","paikallinentutkimusnimikeid"]].value_counts().reset_index(name='count')['count'].sum(),len(df))

    return df
    
def filter_measurement_status(df,args):
    """
    Here we remove values that are not in the accepted value list.
    """
    col,problematic_values=args.config['problematic_status']
    
    err_mask = df[col].isin(problematic_values)
    err_df = df[err_mask]
    err_df.loc[:,'ERR'] = 'measurement_status'
    err_df.loc[:,'ERR_VALUE'] = err_df.loc[err_mask,col]
    err_df[args.config['err_cols']].to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")

    return df[~err_mask]
    

def filter_hetu(df,args):
    """
    Filters out if hetu root is incorrect
    """
    err_mask = df['hetu_root'] != args.config['hetu_kw']
    err_df = df[err_mask]
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
        df[col] = df[col].str.strip().fillna("NA")

    return df



def initialize_out_cols(df,args):
    #Makes sure that the columns for output exist
    for col in args.config['out_cols'] + args.config['err_cols']:
        if col not in df.columns.tolist():
            df[col] = "NA"
            
    return df
