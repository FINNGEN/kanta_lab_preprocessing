import pandas as pd
import numpy as np
import operator


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
        .pipe(flag_outcome_mismatch,args)
        .pipe(flag_omop_extraction_blacklist,args)
    )
    return df

def flag_outcome_mismatch(df, args):
    """
    Identifies and flags records where the categorical test outcome and the 
    extracted binary result do not align, setting QC_PASS to 0 for mismatches.
    """
    # 0. Preparation and Index Reset
    df = df.copy().reset_index(drop=True)
    df['QC_PASS'] = df['QC_PASS'].astype(int)
    
    ops = {
        '<': operator.lt, '<=': operator.le, '>': operator.gt,
        '>=': operator.ge, '==': operator.eq, '!=': operator.ne
    }

    # 1. Define the columns to check and the mismatch criteria
    check_cols = ['TEST_OUTCOME', 'extracted::IS_POS']
    match_tuples = [tuple(x) for x in args.config['outcome_mismatch']]
    qc_note = "OUTCOME_EXTRACT_CONFLICT"

    # 2. Create the mask for failures
    # Logic: MultiIndex check identifies rows matching the "bad" pairs
    fail_mask = pd.MultiIndex.from_frame(df[check_cols]).isin(match_tuples)

    # 3. Update QC Status and Notes for flagged values
    if fail_mask.any():
        # Set QC_PASS to 0 for mismatches
        df.loc[fail_mask, 'QC_PASS'] = 0
        
        # Handle Note Concatenation
        existing_notes = df.loc[fail_mask, 'QC_NOTES'].astype(str)
        concatenated_notes = np.where(
            (existing_notes == 'NA') | (existing_notes == 'nan') | (existing_notes == ''),
            str(qc_note),
            existing_notes + '; ' + str(qc_note)
        )
        df.loc[fail_mask, 'QC_NOTES'] = concatenated_notes

        # 4. Create error dataframe/log for external warning file
        qc_df = df[fail_mask].copy()
        qc_df['ERR'] = 'QC_NOTE_APPENDED'
        qc_df['ERR_VALUE'] = (
            qc_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str).fillna("") + ';' + 
            qc_df['QC_NOTES'].astype(str).fillna("")
        )
        
        # Append to the warning file
        qc_df[args.config['err_cols']].to_csv(
            args.warn_file, mode='a', index=False, header=False, sep="\t"
        )

    return df

def flag_omop_extraction_blacklist(df, args):
    """
    Flag measurements for extraction blacklist and mark them as QC failures.
    
    This function identifies measurements that should be excluded based on the
    extraction blacklist (e.g., unit mismatches) and marks them as QC failures
    by setting QC_PASS to 0, setting MEASUREMENT_VALUE_MERGED to NA, and adding
    appropriate QC notes. Only processes rows where extracted::MEASUREMENT_VALUE
    is not NA.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Main dataframe containing:
        - 'harmonization_omop::OMOP_ID': OMOP concept identifier
        - 'extracted::MEASUREMENT_VALUE': Original extracted measurement values
        - 'extracted::MEASUREMENT_VALUE_MERGED': Numeric measurement values to nullify
        - 'QC_PASS': Quality control pass/fail flag (1=pass, 0=fail)
        - 'QC_NOTES': Quality control notes column
        
    args : object
        Configuration object containing:
        - omop_extraction_blacklist: Lookup table with columns:
            * harmonization_omop::OMOP_ID: OMOP identifier to match on
            * QC_NOTES: Description of the QC failure (e.g., "Mismatch of units")
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with nullified MEASUREMENT_VALUE_MERGED, QC_PASS flags set to 0,
        and updated QC notes
    """
    df = df.copy().reset_index(drop=True)
    
    # Process each blacklist rule separately
    for _, blacklist_rule in args.omop_extraction_blacklist.iterrows():
        omop_id = blacklist_rule['harmonization_omop::OMOP_ID']
        qc_note = blacklist_rule['QC_NOTES']
        
        # Create mask for rows matching this OMOP ID AND where MEASUREMENT_VALUE is not NA
        blacklist_mask = (
            (df['harmonization_omop::OMOP_ID'] == omop_id) &
            (df['extracted::MEASUREMENT_VALUE'].notna())
        )
        
        # Skip if no rows match
        if not blacklist_mask.any():
            continue
        
        # Get indices of blacklisted rows
        blacklist_idx = df.index[blacklist_mask].tolist()
        
        # Set MEASUREMENT_VALUE_MERGED to NA
        df.loc[blacklist_idx, 'extracted::MEASUREMENT_VALUE_MERGED'] = np.nan
        
        # Set QC_PASS to 0 for flagged measurements
        df.loc[blacklist_idx, 'QC_PASS'] = 0
        
        # Update QC notes for flagged values
        existing_notes = df.loc[blacklist_idx, 'QC_NOTES'].astype(str)
        
        # Concatenate notes: if existing is 'NA', replace with new; otherwise append
        concatenated_notes = np.where(
            existing_notes == 'NA',  
            str(qc_note),  # Replace 'NA' with new note
            existing_notes + '; ' + str(qc_note)  # Append to existing notes
        )
        
        # Write concatenated notes back to dataframe
        df.loc[blacklist_idx, 'QC_NOTES'] = concatenated_notes
    
    # Create error dataframe with all flagged rows
    failed_mask = df['QC_PASS'] == "0"
    if failed_mask.any():
        qc_df = df[failed_mask].copy()
        qc_df['ERR'] = 'QC_PASS'
        qc_df['ERR_VALUE'] = (
            qc_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str).fillna("") + ';' + 
            qc_df['QC_NOTES'].astype(str).fillna("")
        )
        qc_df[args.config['err_cols']].to_csv(
            args.warn_file, mode='a', index=False, header=False, sep="\t"
        )
    
    return df


def initialize_qc_columns(df, args):
    """
    Initialize quality control tracking columns with default values.
    
    Adds two columns to track QC status:
    - QC_NOTES: Stores notes about any QC issues or corrections (default: 'NA')
    - QC_PASS: Status flag indicating the QC state of the record.
    
    QC_PASS Status Codes:
    --------------------
    2 : Unchecked (Default)
    1 : Checked, passed the QC
    0 : Checked, did not pass the QC
    
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
    df['QC_NOTES'] = 'NA'
    # Force the column to be int64 from the start
    df['QC_PASS'] = pd.Series(2, index=df.index, dtype='int64')
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

        # 1. Identify which values are valid floats/numbers
        # pd.to_numeric converts non-numbers to NaN; .notna() creates a boolean mask
        is_numeric = pd.to_numeric(df[cmp_col], errors='coerce').notna()

        # 2. Update the mask to ensure the value is numeric before comparing
        mask &= is_numeric

        # 3. Apply comparison
        if op == "<":
            mask &= df[cmp_col].astype(float, errors='ignore') < thr
        elif op == ">":
            mask &= df[cmp_col].astype(float, errors='ignore') > thr
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
    Flag measurements that fail QC thresholds.
    Ensures QC_PASS remains an integer column (0, 1, or 2).
    """
    df = df.copy().reset_index(drop=True)
    
    # Ensure QC_PASS is integer type if it isn't already
    df['QC_PASS'] = df['QC_PASS'].astype(int)
    
    ops = {
        '<': operator.lt, '<=': operator.le, '>': operator.gt,
        '>=': operator.ge, '==': operator.eq, '!=': operator.ne
    }

    # 1. Identify IDs in the QC lookup and set to 1 (Checked)
    relevant_omop_ids = args.omop_qc['harmonization_omop::OMOP_ID'].unique()
    is_in_qc_list = df['harmonization_omop::OMOP_ID'].isin(relevant_omop_ids)
    df.loc[is_in_qc_list, 'QC_PASS'] = 1
    
    # 2. Process each QC rule
    for _, qc_rule in args.omop_qc.iterrows():
        omop_id = qc_rule['harmonization_omop::OMOP_ID']
        threshold = qc_rule['THRESHOLD']
        side = qc_rule['SIDE']
        qc_note = qc_rule['QC_NOTES']
        
        # Skip if placeholder/NA (handles your new lines: 3006923 NA NA...)
        if side not in ops or pd.isna(threshold) or str(threshold).upper() == 'NA':
            continue
        
        omop_mask = df['harmonization_omop::OMOP_ID'] == omop_id
        
        # Ensure values are numeric for comparison
        measurements = pd.to_numeric(df['extracted::MEASUREMENT_VALUE_MERGED'], errors='coerce')
        fail_mask = omop_mask & ops[side](measurements, float(threshold))
        
        if not fail_mask.any():
            continue
        
        # 3. Set to 0 (Integer) for failures
        df.loc[fail_mask, 'QC_PASS'] = 0
        
        # Update notes
        existing_notes = df.loc[fail_mask, 'QC_NOTES'].astype(str)
        concatenated_notes = np.where(
            existing_notes == 'NA',  
            str(qc_note),
            existing_notes + '; ' + str(qc_note)
        )
        df.loc[fail_mask, 'QC_NOTES'] = concatenated_notes
    
    # 4. Error logging for failures (QC_PASS == 0)
    failed_mask = df['QC_PASS'] == 0
    
    if failed_mask.any():
        qc_df = df[failed_mask].copy()
        qc_df['ERR'] = 'QC_PASS'
        qc_df['ERR_VALUE'] = (
            qc_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str).fillna("") + ';' + 
            qc_df['QC_NOTES'].astype(str).fillna("")
        )
        qc_df[args.config['err_cols']].to_csv(
            args.warn_file, mode='a', index=False, header=False, sep="\t"
        )
    
    return df
