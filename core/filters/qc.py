import pandas as pd
import numpy as np


def qc(df, args):
    """
    Main quality control pipeline that processes a DataFrame through multiple QC steps.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe containing measurement data to be quality controlled
    args : object
        Configuration object containing parameters and lookup tables needed for QC
        
    Returns:
    --------
    pandas.DataFrame
        Quality controlled dataframe with QC columns added and fixes applied
    """
    df = (
        df
        .pipe(check_dates_in_measurement, args)
        .pipe(initialize_qc_columns, args)
        .pipe(fix_omop_conversion, args)
        .pipe(flag_omop_qc, args)
    )
    return df


def initialize_qc_columns(df, args):
    """
    Initialize quality control tracking columns with default values.
    
    Adds two columns to track QC status:
    - QC_NOTES: Stores notes about any QC issues or corrections (default: 'NA')
    - QC_PASS: Binary flag indicating if record passes QC (default: "1" for pass)
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe to add QC columns to
    args : object
        Configuration object (unused in this function but kept for pipeline consistency)
        
    Returns:
    --------
    pandas.DataFrame
        Dataframe with QC columns added
    """
    df = df.copy()
    df.loc[:, 'QC_NOTES'] = 'NA'
    df.loc[:, 'QC_PASS'] = "1"    
    return df

def fix_omop_conversion_old(df, args):
    """
    Apply unit conversion factors and update QC notes based on OMOP ID lookup table.
    
    This function corrects measurement values that fall outside expected thresholds
    by applying conversion factors. For example, if a glucose value is below a 
    threshold, it may indicate the value is in mg/dL and needs conversion to mmol/L.
    
    The function operates IN-PLACE on the input DataFrame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Main dataframe containing:
        - 'harmonization_omop::OMOP_ID': OMOP concept identifier
        - 'extracted::MEASUREMENT_VALUE_MERGED': Numeric measurement values
        - 'QC_NOTES': Quality control notes column
        Modified in-place.
        
    args : object
        Configuration object containing:
        - omop_fix_table: Lookup table with columns:
            * harmonization_omop::OMOP_ID: OMOP identifier to match on
            * COLUMN_NAME: Column name to apply threshold filter on
            * VALUE_THRESHOLD: Threshold value for comparison
            * SIDE: Comparison operator ('<' or '>')
            * CONVERSION: Multiplication factor to apply
            * NOTES: Description of the correction applied
        
    Returns:
    --------
    pandas.DataFrame
        The input dataframe, modified in-place with corrected values and updated QC notes
        
    Example:
    --------
    If a glucose measurement is 90 and threshold is 100 with SIDE='<' and CONVERSION=0.0555,
    the value will be converted: 90 * 0.0555 = 4.995 mmol/L
    """
    # Merge lookup table to add threshold and conversion columns
    # Using left join to preserve all original rows
    merged = df.merge(
        args.omop_fix_table,
        on='harmonization_omop::OMOP_ID', 
        how='left', 
        suffixes=('', '_threshold')
    )
    
    # Create boolean mask identifying rows that need conversion
    # Start with all False
    mask = pd.Series(False, index=merged.index)
    
    # Apply vectorized comparisons based on SIDE operator
    # For '<': Convert if measured value is below threshold in the specified column
    less_than_mask = (merged['SIDE'] == '<') & \
                     (merged['COLUMN_NAME'].notna())
    
    for idx in merged[less_than_mask].index:
        col_name = merged.loc[idx, 'COLUMN_NAME']
        if col_name in df.columns:
            mask.loc[idx] = df.loc[idx, col_name] < merged.loc[idx, 'VALUE_THRESHOLD']
    
    # For '>': Convert if measured value is above threshold in the specified column
    greater_than_mask = (merged['SIDE'] == '>') & \
                        (merged['COLUMN_NAME'].notna())
    
    for idx in merged[greater_than_mask].index:
        col_name = merged.loc[idx, 'COLUMN_NAME']
        if col_name in df.columns:
            mask.loc[idx] = df.loc[idx, col_name] > merged.loc[idx, 'VALUE_THRESHOLD']
    
    # Get the actual index positions where mask is True
    mask_idx = mask[mask].index
    
    # Apply conversion factor to flagged measurements
    df.loc[mask_idx, 'extracted::MEASUREMENT_VALUE_MERGED'] = \
        merged.loc[mask_idx, 'extracted::MEASUREMENT_VALUE_MERGED'] * merged.loc[mask_idx, 'CONVERSION']
    
    # Update QC notes for converted values
    # Get existing notes and new notes as strings
    existing_notes = df.loc[mask_idx, 'QC_NOTES'].astype(str)
    new_notes = merged.loc[mask_idx, 'NOTES'].astype(str)
    
    # Concatenate notes: if existing is 'NA', replace with new; otherwise append
    concatenated_notes = np.where(
        existing_notes == 'NA',  
        new_notes,  # Replace 'NA' with new note
        existing_notes + '; ' + new_notes  # Append to existing notes
    )
    
    # Write concatenated notes back to original dataframe
    df.loc[mask_idx, 'QC_NOTES'] = concatenated_notes
    # Create error dataframe with flagged rows
    qc_df = df.loc[mask_idx].copy()
    qc_df['ERR'] = 'QC'
    qc_df['ERR_VALUE'] = (
        qc_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str).fillna("") + ';' + 
        qc_df['QC_NOTES'].astype(str).fillna("")
    )
 
    qc_df[args.config['err_cols']].to_csv(args.warn_file, mode='a', index=False, header=False,sep="\t")
    
    return df



def fix_omop_conversion(df, args):
    """
    Apply OMOP conversion rules based on current rule table format.
    Vectorized, index-safe, and includes QC note updates + QC file output.
    """

    df = df.copy().reset_index(drop=True)
    rules = args.omop_fix_table.copy().reset_index(drop=True)

    all_fixed_idx = []

    for _, rule in rules.iterrows():

        omop = rule["harmonization_omop::OMOP_ID"]
        cmp_col = rule["COLUMN_NAME"]
        thr = rule["VALUE_THRESHOLD"]
        op = rule["SIDE"]
        mult = rule["CONVERSION"]
        note = rule["NOTES"]

        # Skip rules referencing columns not present in df
        if cmp_col not in df.columns:
            continue

        # Base OMOP match
        mask = df["harmonization_omop::OMOP_ID"] == omop

        # Apply comparison
        if op == "<":
            mask &= df[cmp_col] < thr
        elif op == ">":
            mask &= df[cmp_col] > thr
        else:
            raise ValueError(f"Unsupported operator: {op}")

        # Skip rule if no rows match
        if not mask.any():
            continue

        mask_idx = df.index[mask].tolist()
        all_fixed_idx.extend(mask_idx)

        # Apply conversion to MEASUREMENT_VALUE_MERGED (your current behavior)
        df.loc[mask, "extracted::MEASUREMENT_VALUE_MERGED"] = (
            df.loc[mask, "extracted::MEASUREMENT_VALUE_MERGED"] * mult
        )

        # ------------------------------
        # QC NOTES UPDATE (your block)
        # ------------------------------
        existing_notes = df.loc[mask, "QC_NOTES"].astype(str)
        new_notes = str(note)

        concatenated_notes = np.where(
            existing_notes == "NA",
            new_notes,
            existing_notes + "; " + new_notes
        )

        df.loc[mask, "QC_NOTES"] = concatenated_notes

    # ------------------------------
    # QC ERROR OUTPUT (your block)
    # ------------------------------
    if all_fixed_idx:
        qc_df = df.loc[all_fixed_idx].copy()

        qc_df["ERR"] = "QC_CONVERT"
        qc_df["ERR_VALUE"] = (
            qc_df["cleaned::TEST_NAME_ABBREVIATION"].astype(str).fillna("") +
            ";" +
            qc_df["QC_NOTES"].astype(str)
        )

        qc_df[args.config["err_cols"]].to_csv(
            args.warn_file,
            mode="a",
            index=False,
            header=False,
            sep="\t"
        )

    return df


def check_dates_in_measurement(df, args):
    """
    Identify and remove rows where measurement values are actually dates.
    
    This function detects when numeric measurement fields contain dates encoded
    as 6-digit integers (DDMMYY format). Such rows are logged to an error file
    and removed from the dataset.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe containing:
        - extracted::MEASUREMENT_VALUE: Raw measurement value (may contain dates)
        - harmonization_omop::MEASUREMENT_VALUE: Harmonized measurement value
        - cleaned::TEST_NAME_ABBREVIATION: Test abbreviation
        - MEASUREMENT_FREE_TEXT: Free text measurement field
        
    args : object
        Configuration object containing:
        - err_file: Path to error log file
        - config['err_cols']: List of column names to write to error log
        
    Returns:
    --------
    pandas.DataFrame
        Input dataframe with rows containing date values removed
        
    Side Effects:
    -------------
    Appends error records to args.err_file
        
    Example:
    --------
    Value 311299 would be detected as 31/12/99 (Dec 31, 1999) and flagged as error
    """
    col_name = "extracted::MEASUREMENT_VALUE"
    mes_col = "harmonization_omop::MEASUREMENT_VALUE"

    # Initialize error mask (all False)
    err_mask = pd.Series(False, index=df.index)
    
    # Create mask for non-null values to avoid NaN processing errors
    non_na_mask = df[col_name].notna()
    
    # Only process if there are non-null values
    if non_na_mask.any():
        # Convert non-null values to string via integer (removes decimals)
        str_values = df.loc[non_na_mask, col_name].astype(int).astype(str)
        
        # Identify potential dates: exactly 6 digits (DDMMYY format)
        six_digit_mask = str_values.str.len() == 6
        
        # Only process if there are six-digit values
        if six_digit_mask.any():
            # Get indices of rows with six-digit values
            six_digit_indices = six_digit_mask[six_digit_mask].index
            six_digit_values = str_values[six_digit_mask]
            
            # Parse date components from DDMMYY format
            days = six_digit_values.str[:2].astype(int)
            months = six_digit_values.str[2:4].astype(int)
            years = six_digit_values.str[4:].astype(int)
            
            # Validate date component ranges
            valid_days = (days >= 1) & (days <= 31)
            valid_months = (months >= 1) & (months <= 12)
            valid_years = (years >= 0) & (years <= 99)
            
            # Combine all validation checks
            valid_dates = valid_days & valid_months & valid_years
            
            # Get indices of rows with valid date patterns
            valid_date_indices = six_digit_indices[valid_dates]
            
            # Mark these rows as errors
            err_mask.loc[valid_date_indices] = True
    
    # Create error dataframe with flagged rows
    err_df = df[err_mask].copy()
    err_df['ERR'] = 'DATE_IN_MEASUREMENT'
    
    # Create detailed error value string combining multiple fields
    err_df['ERR_VALUE'] = (
        err_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str) + "::" + 
        err_df[mes_col].astype(str) + "::" + 
        err_df['MEASUREMENT_FREE_TEXT'].astype(str) + '::' + 
        err_df[col_name].astype(str)
    )
    
    # Append errors to log file
    err_df[args.config['err_cols']].to_csv(
        args.err_file, 
        mode='a',  # Append mode
        index=False, 
        header=False, 
        sep="\t"
    )
    
    # Return dataframe with error rows removed
    return df[~err_mask]


def flag_omop_qc(df, args):
    """
    Flag measurements that fail QC thresholds and mark them as QC failures.
    
    This function identifies measurement values that fall outside acceptable ranges
    and marks them as QC failures by setting QC_PASS to 0. For example, if a 
    measurement is below an expected minimum threshold, it may indicate incorrect
    units or data entry errors.
    
    The function operates IN-PLACE on the input DataFrame.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Main dataframe containing:
        - 'harmonization_omop::OMOP_ID': OMOP concept identifier
        - 'extracted::MEASUREMENT_VALUE_MERGED': Numeric measurement values to check
        - 'QC_PASS': Quality control pass/fail flag (1=pass, 0=fail)
        - 'QC_NOTES': Quality control notes column
        Modified in-place.
        
    args : object
        Configuration object containing:
        - omop_qc_table: Lookup table with columns:
            * harmonization_omop::OMOP_ID: OMOP identifier to match on
            * THRESHOLD: Threshold value for comparison
            * SIDE: Comparison operator ('<' or '>')
            * QC_NOTES: Description of the QC failure
            * MISC: Additional information (optional)
        
    Returns:
    --------
    pandas.DataFrame
        The input dataframe, modified in-place with QC_PASS flags and updated QC notes
        
    Example:
    --------
    If a measurement is 0.5 and threshold is 1 with SIDE='<',
    the row will be flagged: QC_PASS set to 0, QC_NOTES updated with failure reason
    """
    # Merge lookup table to add threshold and QC criteria columns
    # Using left join to preserve all original rows
    merged = df.merge(
        args.omop_qc,
        on='harmonization_omop::OMOP_ID', 
        how='left', 
        suffixes=('', '_qc')
    )
    
    # Create boolean mask identifying rows that fail QC
    # Start with all False
    mask = pd.Series(False, index=merged.index)
    
    # Apply vectorized comparisons based on SIDE operator
    # For '<': Fail QC if measured value is below threshold
    mask |= (merged['SIDE'] == '<') & \
            (merged['extracted::MEASUREMENT_VALUE_MERGED'] < merged['THRESHOLD'])
    
    # For '>': Fail QC if measured value is above threshold
    mask |= (merged['SIDE'] == '>') & \
            (merged['extracted::MEASUREMENT_VALUE_MERGED'] > merged['THRESHOLD'])
    
    # Get the actual index positions where mask is True
    mask_idx = mask[mask].index
    
    # Set QC_PASS to 0 for flagged measurements
    df.loc[mask_idx, 'QC_PASS'] = "0"
    
    # Update QC notes for flagged values
    # Get existing notes and new notes as strings
    existing_notes = df.loc[mask_idx, 'QC_NOTES'].astype(str)
    new_notes = merged.loc[mask_idx, 'QC_NOTES_qc'].astype(str)
    
    # Concatenate notes: if existing is 'NA', replace with new; otherwise append with separator
    concatenated_notes = np.where(
        existing_notes == 'NA',  
        new_notes,  # Replace 'NA' with new note
        existing_notes + '; ' + new_notes  # Append to existing notes
    )
    
    # Write concatenated notes back to original dataframe
    df.loc[mask_idx, 'QC_NOTES'] = concatenated_notes

    qc_df = df.loc[mask_idx].copy()
    qc_df['ERR'] = 'QC_PASS'
    qc_df['ERR_VALUE'] = (
        qc_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str).fillna("") + ';' + 
        qc_df['QC_NOTES'].astype(str).fillna("")
    )
    qc_df[args.config['err_cols']].to_csv(args.warn_file, mode='a', index=False, header=False,sep="\t")
    return df
