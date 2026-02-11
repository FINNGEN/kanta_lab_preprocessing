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


def check_dates_in_measurement(df, args):
    """
    Identifies and removes rows where measurement values are incorrectly encoded as dates.
    
    This function targets numeric fields that contain 6-digit integers following 
    the DDMMYY format. It cleans the input to handle potential float-to-string 
    conversion artifacts (like '.0'), extracts date components via regex, 
    validates them, and logs flagged rows to an error file.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input data containing measurement values and metadata.
    args : object
        Configuration object containing 'err_file' (path) and 'config["err_cols"]'.
        
    Returns:
    --------
    pandas.DataFrame
        Dataframe with detected date-entry rows removed.
    """
    col_name = "extracted::MEASUREMENT_VALUE"
    mes_col = "harmonization_omop::MEASUREMENT_VALUE"

    # 1. Clean data: Convert to string, remove '.0' from floats, and strip whitespace.
    # This prevents 'inf' or 'NaN' from crashing the processing.
    clean_series = df[col_name].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # 2. Extract potential date components using regex (DD MM YY)
    # The pattern ^(\d{2})(\d{2})(\d{2})$ ensures we only look at exactly 6 digits.
    parts = clean_series.str.extract(r'^(\d{2})(\d{2})(\d{2})$')

    # 3. Validate components numerically
    # pd.to_numeric with errors='coerce' ensures non-matches (NaN) don't break the logic.
    days = pd.to_numeric(parts[0], errors='coerce')
    months = pd.to_numeric(parts[1], errors='coerce')
    years = pd.to_numeric(parts[2], errors='coerce')

    # 4. Create a boolean mask for valid date patterns
    # Any row failing the range check or the regex match becomes False via .fillna().
    is_date_mask = (
        (days >= 1) & (days <= 31) &
        (months >= 1) & (months <= 12) &
        (years >= 0) & (years <= 99)
    ).fillna(False)

    # 5. Process and log errors
    err_df = df[is_date_mask].copy()
    
    if not err_df.empty:
        # Construct the detailed error message
        err_df['ERR'] = 'DATE_IN_MEASUREMENT'
        err_df['ERR_VALUE'] = (
            err_df['cleaned::TEST_NAME_ABBREVIATION'].astype(str) + "::" + 
            err_df[mes_col].astype(str) + "::" + 
            err_df['MEASUREMENT_FREE_TEXT'].astype(str) + '::' + 
            err_df[col_name].astype(str)
        )
        
        # Append to the external error log
        err_df[args.config['err_cols']].to_csv(
            args.err_file, 
            mode='a', 
            index=False, 
            header=False, 
            sep="\t"
        )

    # 6. Return the filtered dataframe
    return df[~is_date_mask]
