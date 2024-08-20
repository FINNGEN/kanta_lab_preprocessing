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
        .pipe(fix_date,args)
        .pipe(remove_spaces,args)
        .pipe(fix_na,args)
        .pipe(filter_measurement_status,args)
        .pipe(lab_id_source,args)
        .pipe(get_lab_abbrv,args)
        .pipe(get_service_provider_name,args)
        .pipe(fix_abbreviation,args)
        #.pipe(filter_missing,args)

    )
    return df

def filter_missing(df,args):
    """
    Removes entry if missing both value and abnormality are NAs
    """
    cols = ['MEASUREMENT_VALUE','TEST_OUTCOME']
    err_mask = (df[cols]=="NA").prod(axis=1).astype(bool)
    err_df = df[err_mask].copy()
    err_df['ERR'] = 'NA'
    err_df['ERR_VALUE'] = err_df[cols[0]] + "_" +  err_df[cols[1]]
    err_df[args.config['err_cols']].to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")

    return df


def fix_abbreviation(df,args):
    """
    Removes characthers from abbreviation
    """
    col = 'TEST_NAME_ABBREVIATION'
    abb_df = df[['FINNGENID', 'APPROX_EVENT_DATETIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']].copy()
    pattern = '|'.join(args.config['abbreviation_deletions'])
    df[col] = df[col].replace(pattern,'',regex=True)
    #log changes
    abb_df['new'] = df[col].copy()
    unit_mask = (abb_df[col] != abb_df['new'])
    abb_df[unit_mask].to_csv(args.abbr_file, mode='a', index=False, header=False,sep="\t")

    # replace problematic characters in abbrevation (strange minus sign)
    values = args.config['abbreviation_replacements']
    abb_df = df[['FINNGENID', 'APPROX_EVENT_DATETIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']].copy()
    for rep in args.config['abbreviation_replacements']:
        df.loc[:,col] = df.loc[:,col].replace(rep[0],rep[1],regex=True)
     
    abb_df['new'] = df[col].copy()
    unit_mask = (abb_df[col] != abb_df['new'])
    abb_df[unit_mask].to_csv(args.abbr_file, mode='a', index=False, header=False,sep="\t")
   
    return df

def get_service_provider_name(df,args):
    """
    Updates CODING_SYSTEM based on mapping. Keeps original if missing
    """
    col = 'CODING_SYSTEM'
    #mask = df[col].isin(args.config['thl_sote_map'])
    # FIRST ROUND
    df.loc[:,col] = df.loc[:,col].map(args.config['thl_sote_map'])
    # SECOND ROUND
    # create column with mappable name
    df['TMP_SYSTEM'] =  df['CODING_SYSTEM'].str.replace("1.2.246.10.","").str.replace("1.2.246.537.10.","").str.split('.',expand=True,n=1)[0]
    # I need this step since i create some strange entries with value 1 for 1.2.246.537.6.3.2006 and of the sort
    mask = df['TMP_SYSTEM'].isin(args.config['thl_manual_map'])
    df.loc[mask,col] = df.loc[mask,'TMP_SYSTEM'].map(args.config['thl_manual_map'])
    return df


def get_lab_abbrv(df,args):
    """
    It assigns TEST_NAME_ABBREVIATION, keeping the local name if source is local (TEST_ID==0) or mapping it if source is THL (TEST_ID ==1). If the value is missing from the mapping it will be mapped to NA
    N.B.LAB ABBREVIATION is already read on reading from paikallinentutkimusnimike (from config) so no need to create it, just update
    """
    col="TEST_NAME_ABBREVIATION"
    df[col] =df[col].str.lower()     #fix lab abbrevation in general before updated mapping
    mask = df.TEST_ID_IS_NATIONAL == "1"

    #log where id is present but cannot me mapped
    map_mask = ~df["TEST_ID"].isin(args.config['thl_lab_map'].keys())

    warn_mask = (mask & map_mask)
    warn_df = df[warn_mask].copy()
    warn_df['ERR'] = 'lab_mapping'
    warn_df['ERR_VALUE'] = warn_df["TEST_ID"]
    warn_df[args.config['err_cols']].to_csv(args.warn_file, mode='a', index=False, header=False,sep="\t")

    df.loc[mask,col] = df.loc[mask,"TEST_ID"].map(args.config['thl_lab_map'])
    df[col] = df[col].str.replace('"', '')     # remove single quotes
    return df


def lab_id_source(df,args):
    """
    Update/create TEST_ID and TEST_ID SOURCE.
    In this function we uses local_lab_id (paikallinentutkimusnimikeid)  and thl lab_id (laboratoriotutkimusnimikeid) if possible.
    """
    
    local_mask =  (df['laboratoriotutkimusnimikeid'] == 'NA')
    df["TEST_ID_IS_NATIONAL"] = np.where(local_mask,"0","1")
    df["TEST_ID"] = np.where(local_mask,df.paikallinentutkimusnimikeid,df.laboratoriotutkimusnimikeid)
    return df
    
def filter_measurement_status(df,args):
    """
    Here we remove values that are not in the accepted value list.
    """
    col,problematic_values=args.config['problematic_status']
    err_mask = df[col].isin(problematic_values)
    err_df = df[err_mask].copy()
    err_df['ERR'] = 'measurement_status'
    err_df['ERR_VALUE'] = err_df[col]
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


def fix_date(df,args):
    """
    Joins day and time to make a single date field.
    """
    
    #df['APPROX_EVENT_DATETIME'] = pd.to_datetime(df.APPROX_EVENT_DAY +" "+df.TIME,errors='coerce').dt.strftime(args.config['date_time_format'])
    df['APPROX_EVENT_DATETIME'] =df.APPROX_EVENT_DAY +"T"+df.TIME
    err_mask = pd.to_datetime(df.APPROX_EVENT_DATETIME, format=args.config['date_time_format'], errors='coerce').notna()
    err_df = df[err_mask].copy()
    err_df['ERR'] = 'DATE'
    err_df['ERR_VALUE'] = err_df.APPROX_EVENT_DAY +" "+err_df.TIME
    err_df[args.config['err_cols']].to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[~err_mask]



def remove_spaces(df,args):
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

    df = df.rename(columns = args.config['rename_cols'])
    # These columns need be copied back to original name
    for col in args.config['source_cols']:
        df['source::'+col ] = df[col]
        
    to_be_initialized = [col for col in args.config['out_cols'] + args.config['err_cols']]
    for col in to_be_initialized:
        if col not in df.columns.tolist() and not col.startswith("cleaned::"):
            df[col] = "NA"
            
    return df
