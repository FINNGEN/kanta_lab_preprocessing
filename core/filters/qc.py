import pandas as pd



def qc(df,args):

    df = (
        df
        .pipe(check_dates_in_measurement,args)
    )
    return df





def check_dates_in_measurement(df, args):
    """
    Chekcs if the extracted data contains dates
    """
    col_name = "extracted::MEASUREMENT_VALUE"
    mes_col = "harmonization_omop::MEASUREMENT_VALUE"
    
    # First get only extracted rows
    str_series = df[col_name].fillna(0).astype(int).astype(str)
    is_six_digits = str_series.str.len() == 6
    err_mask = is_six_digits # this will either be replace or be a an all False array
    if is_six_digits.any():
        
        # Extract potential day, month, year
        days = str_series[is_six_digits].str[:2].astype(int)
        months = str_series[is_six_digits].str[2:4].astype(int)
        years = str_series[is_six_digits].str[4:].astype(int)
        
        # Check if values fall within valid date ranges
        valid_days = (days >= 1) & (days <= 31)
        valid_months = (months >= 1) & (months <= 12)
        valid_years = (years >= 0) & (years <= 99)
        
        # Combine all conditions
        err_mask = valid_days & valid_months & valid_years

        
    err_df = df[err_mask].copy().fillna("NA")
    err_df['ERR'] = 'DATE_IN_MEASUREMENT'
    err_df['ERR_VALUE'] = err_df['cleaned::TEST_NAME_ABBREVIATION'] + "::" + err_df[mes_col].astype(str) + "::" +  err_df.MEASUREMENT_FREE_TEXT  + '::' + err_df[col_name].astype(str)
    err_df[args.config['err_cols']].to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[~err_mask]
def high_low_filters(df,args):


    return df
