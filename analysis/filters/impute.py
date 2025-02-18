import pandas as pd
import re
import numpy as np

def impute_all(df,args):

    df = (
        df
        .pipe(impute_measurement,args)
        .pipe(impute_positive,args)
    )
    return df


def impute_measurement(df,args):
    """
    Creates new imputed::MEASURMENT_VALUE column with data extracted from MEASUREMENT_FREE_TEXT column
    """

    col_name = "imputed::MEASUREMENT_VALUE"
    mes_col = "harmonization_omop::MEASUREMENT_VALUE"
    ft_col = "MEASUREMENT_FREE_TEXT"
    unit_col= 'harmonization_omop::MEASUREMENT_UNIT'
    # get mask where mes is na
    mask = df[mes_col].isna() & ~df[ft_col].isna()
    # copy over numerical values from harmonized data and start the string manipulation
    df.loc[:,col_name] = df.loc[:,ft_col].where(mask,df.loc[:,mes_col]).astype(str).str.lower().str.strip().str.replace(r'\s', '', regex=True).fillna("NA") # this removes ALL spaces
    # create series with target unit for omop values and remove that from the free text column
    target_unit = df["harmonization_omop::OMOP_ID"].astype(int).map(args.omop_unit_table)
    df.loc[:,col_name] = df.apply(lambda row: row[col_name].replace(
    str(target_unit[row.name]), ''  # Replace target unit with empty string
), axis=1)
    
    # regex replacements
    all_replacements =  [(rf'\b{re.escape(word)}\b', '') for word in args.config['free_text_result_strings']] + args.config['free_text_measurement_replacements']
    for rep in all_replacements:
        df.loc[:,col_name] = df.loc[:,col_name].replace(rep[0],rep[1],regex=True)
    df[col_name] = pd.to_numeric(df[col_name], errors='coerce')


    return df
    


def check_potential_dates(series):
    """
    Check if integers in a pandas Series could be mistaken for dates in DDMMYY format.
    Returns a boolean mask where True indicates a potential date.
    
    Rules for DDMMYY format:
    - DD: 01-31
    - MM: 01-12
    - YY: 00-99
    
    Parameters:
    series (pd.Series): Input series of integers
    
    Returns:
    pd.Series: Boolean mask where True indicates potential dates
    """
    # Convert to string with leading zeros to ensure 6 digits
    str_series = series.fillna(0).astype(int).astype(str)
    is_six_digits = str_series.str.len() == 6
    # Only process 6-digit values
    if is_six_digits.any():
        valid_numbers = series[is_six_digits]
        
        # Extract potential day, month, year
        days = str_series[is_six_digits].str[:2].astype(int)
        months = str_series[is_six_digits].str[2:4].astype(int)
        years = str_series[is_six_digits].str[4:].astype(int)
        
        # Check if values fall within valid date ranges
        valid_days = (days >= 1) & (days <= 31)
        valid_months = (months >= 1) & (months <= 12)
        valid_years = (years >= 0) & (years <= 99)
        
        # Combine all conditions
        date_mask = valid_days & valid_months & valid_years
    
        return date_mask

    return 0

def impute_positive(df,args):
    """
    Creates new column with pos/neg extracted information
    """

    df = pd.merge(df,args.posneg_table, on ="MEASUREMENT_FREE_TEXT",how='left')

    return df
