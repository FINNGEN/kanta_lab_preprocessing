import pandas as pd
import re
import numpy as np

def extract_all(df,args):

    df = (
        df
        .pipe(extract_measurement,args)
        .pipe(extract_positive,args)
    )
    return df


def extract_measurement(df,args):
    """
    Creates new extracted::MEASURMENT_VALUE column with data extracted from MEASUREMENT_FREE_TEXT column
    """

    col_name = "extracted::MEASUREMENT_VALUE"
    extracted_bool_col = "extracted::IS_MEASUREMENT_EXTRACTED"
    omop_col = "harmonization_omop::MEASUREMENT_VALUE"
    ft_col = "MEASUREMENT_FREE_TEXT"
    unit_col= 'harmonization_omop::MEASUREMENT_UNIT'

    
    # these are the values i want to try to work with
    mask = df[omop_col].isna() & ~df[ft_col].isna()
    # create series with measurement data i'll try to extract
    ft_data = df[ft_col].copy().where(mask,np.nan)
    df.loc[:,col_name] = df.loc[:,ft_col].astype(str).str.lower().str.strip().str.replace(r'\s', '', regex=True).fillna("NA") # this removes ALL spaces
    # create series with target unit for omop values and remove that from the free text column
    target_unit = df["harmonization_omop::OMOP_ID"].astype(int).map(args.omop_unit_table)
    df.loc[:,col_name] = df.apply(lambda row: row[col_name].replace(str(target_unit[row.name]), ''), axis=1)
    
    # regex replacements
    all_replacements =  [(rf'\b{re.escape(word)}\b', '') for word in args.config['free_text_result_strings']] + args.config['free_text_measurement_replacements']
    for rep in all_replacements:
        df.loc[:,col_name] = df.loc[:,col_name].replace(rep[0],rep[1],regex=True)
    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
    df.loc[:,extracted_bool_col] = (~df[col_name].isna()).astype(int)
    df.loc[:,col_name] = df[col_name].fillna(df[omop_col].astype(float))
    
    return df
    



def extract_positive(df,args):
    """
    Creates new column with pos/neg extracted information
    """

    df = pd.merge(df,args.posneg_table, on ="MEASUREMENT_FREE_TEXT",how='left')

    return df
