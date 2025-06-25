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

    err_mask = pd.Series(False, index=df.index)
    # Handle NaN values by creating a mask of valid values first
    non_na_mask = df[col_name].notna()
    # Process only if there are non-NA values
    if non_na_mask.any():
        # Convert to string, ensuring we only process non-NA values
        str_values = df.loc[non_na_mask, col_name].astype(int).astype(str)
        # Identify six-digit values (potential dates)
        six_digit_mask = str_values.str.len() == 6
        # Process only if there are six-digit values
        if six_digit_mask.any():
            # Get indices of rows with six-digit values
            six_digit_indices = six_digit_mask[six_digit_mask].index
            # Filter to only work with six-digit values
            six_digit_values = str_values[six_digit_mask]
            # Extract day, month, year components
            days = six_digit_values.str[:2].astype(int)
            months = six_digit_values.str[2:4].astype(int)
            years = six_digit_values.str[4:].astype(int)
            # Check for valid date ranges
            valid_days = (days >= 1) & (days <= 31)
            valid_months = (months >= 1) & (months <= 12)
            valid_years = (years >= 0) & (years <= 99)
            # Combine all conditions
            valid_dates = valid_days & valid_months & valid_years
            # Get indices of rows with valid dates
            valid_date_indices = six_digit_indices[valid_dates]
            # Update the mask for these valid indices
            err_mask.loc[valid_date_indices] = True
    
    err_df = df[err_mask].copy()
    err_df['ERR'] = 'DATE_IN_MEASUREMENT'
    err_df['ERR_VALUE'] = err_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str) + "::" + err_df[mes_col].astype(str) + "::" + err_df['MEASUREMENT_FREE_TEXT'].astype(str) + '::' + err_df[col_name].astype(str)
    err_df[args.config['err_cols']].to_csv(args.err_file, mode='a', index=False, header=False,sep="\t")
    return df[~err_mask]
def high_low_filters(df,args):


    return df
