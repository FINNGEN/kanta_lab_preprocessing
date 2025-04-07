import pandas as pd
import numpy as np


def all_outcome(df, args):
    """
    Master function that processes test outcomes by applying a series of transformations.
    
    This function applies three sequential processing steps to the dataframe:
    1. Initialize merged outcome columns
    2. Map outcomes from free text extraction
    3. Impute outcomes based on measurement values and reference ranges
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The input dataframe containing test results data
    args : object
        An object containing reference data needed for processing:
        - ft_outcome_map: DataFrame mapping test outcomes from free text
        - ab_limits: DataFrame with reference ranges (LOW_LIMIT and HIGH_LIMIT)
    
    Returns:
    --------
    pandas.DataFrame
        The processed dataframe with populated 'merged::TEST_OUTCOME' column
    """
    df = (
        df
        .pipe(init_merged_outcome, args)
        .pipe(map_ft_outcome, args)
        .pipe(impute_outcome, args)
    )

    return df


def init_merged_outcome(df, args):
    """
    Initialize the merged outcome column with values from the original TEST_OUTCOME column.
    
    This function:
    1. Creates and initializes 'merged::TEST_OUTCOME' and 'merged::OUTCOME_SOURCE' columns
    2. Populates these columns with values from TEST_OUTCOME where available
    3. Sets the source indicator to 'O' (Original) for these values
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The input dataframe containing test results
    args : object
        Arguments object (not used in this function but kept for consistent interface)
    
    Returns:
    --------
    pandas.DataFrame
        Dataframe with initialized merged outcome columns
    """
    col = 'merged::TEST_OUTCOME'
    value_col = 'TEST_OUTCOME'
    source_col = 'merged::OUTCOME_SOURCE'

    # Initialize the merged outcome column
    df[col] = "NA"
    df[source_col] = "NA"
    df[value_col] = df[value_col].fillna("NA")

    # Create a boolean mask for non-NA original outcomes
    mask = (df[value_col] != "NA")
    
    # Apply the values based on the mask
    df.loc[mask, col] = df.loc[mask, value_col]
    df.loc[mask, source_col] = "O"  # 'O' indicates outcome from original source
    
    return df


def map_ft_outcome(df, args):
    """
    Map free-text extracted outcomes to standardized values for outcomes that weren't
    populated in the initialization step.
    
    This function:
    1. Merges the dataframe with a mapping table for free-text outcomes
    2. Updates the merged outcome only where it is currently "NA"
    3. Sets the source indicator to 'FT' (Free Text) for these values
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Dataframe with initialized merged outcome columns
    args : object
        Arguments object containing:
        - ft_outcome_map: DataFrame with mappings from free text to standardized outcomes
    
    Returns:
    --------
    pandas.DataFrame
        Dataframe with outcomes mapped from free text extraction
    """
    outcome_col = 'merged::TEST_OUTCOME'
    ft_outcome_col = 'extracted::TEST_OUTCOME'
    source_col = 'merged::OUTCOME_SOURCE'

    # Merge with the free text outcome mapping table
    df = pd.merge(
        df, 
        args.ft_outcome_map, 
        how='left', 
        on=['harmonization_omop::OMOP_ID', "extracted::TEST_OUTCOME_TEXT"]
    ).fillna({ft_outcome_col: "NA"})

    # Create a mask for rows where merged::TEST_OUTCOME is NA
    na_mask = df[outcome_col] == "NA"

    # Create a mask for rows where the mapped outcome is not NA
    mapped_not_na_mask = df[ft_outcome_col] != "NA"

    # Combine the masks to update only where merged::TEST_OUTCOME is NA
    # and there's a valid mapping
    update_mask = na_mask & mapped_not_na_mask

    # Update the outcome and source columns
    df.loc[update_mask, outcome_col] = df.loc[update_mask, ft_outcome_col]
    df.loc[update_mask, source_col] = "FT"  # 'FT' indicates outcome from free text

    return df


def impute_outcome(df, args):
    """
    Impute test outcomes based on measurement values and reference ranges.
    
    This function populates outcomes that are still "NA" after previous steps by:
    1. Comparing measurement values to reference ranges (LOW_LIMIT and HIGH_LIMIT)
    2. Assigning outcome categories:
       - "L" for low values (below LOW_LIMIT)
       - "L*" for critically low values (L + LOW_PROBLEM flag)
       - "H" for high values (above HIGH_LIMIT)
       - "H*" for critically high values (H + HIGH_PROBLEM flag)
       - "N" for normal values (within reference range)
    3. Setting the source indicator to 'V' (Value-based) for imputed outcomes
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Dataframe with partially populated outcome columns
    args : object
        Arguments object containing:
        - ab_limits: DataFrame with reference ranges and problem indicators
    
    Returns:
    --------
    pandas.DataFrame
        Dataframe with fully populated outcome columns
    """
    outcome_col = 'merged::TEST_OUTCOME'
    value_col = 'extracted::MEASUREMENT_VALUE'
    source_col = 'merged::OUTCOME_SOURCE'

    # Create a mask for rows where merged::TEST_OUTCOME is NA
    na_outcome_mask = df[outcome_col] == "NA"

    # Prepare columns for merging with reference ranges
    left_col = 'harmonization_omop::OMOP_ID'
    right_col = 'ID'
    # Convert both columns to string type for the merge
    df[left_col] = df[left_col].astype(str)
    args.ab_limits[right_col] = args.ab_limits[right_col].astype(str)

    # Merge with reference ranges table
    df = pd.merge(df, args.ab_limits, how='left', left_on=[left_col], right_on=[right_col])
    
    # Convert value and limit columns to numeric, with non-numeric values becoming NaN
    float_df = df[[value_col, 'LOW_LIMIT', 'HIGH_LIMIT']].apply(pd.to_numeric, errors='coerce')

    # Impute LOW abnormality only where merged::TEST_OUTCOME is NA
    low_mask = na_outcome_mask & (float_df[value_col] < float_df["LOW_LIMIT"])
    df.loc[low_mask, outcome_col] = "L"
    df.loc[low_mask, source_col] = "V"  # 'V' indicates value-based imputation

    # Mark critically low values
    low_problem_mask = df['LOW_PROBLEM'] == 1
    df.loc[low_mask & low_problem_mask, outcome_col] = "L*"
    df.loc[low_mask & low_problem_mask, source_col] = "V"

    # Impute HIGH abnormality only where merged::TEST_OUTCOME is NA
    high_mask = na_outcome_mask & (float_df[value_col] > float_df["HIGH_LIMIT"])
    df.loc[high_mask, outcome_col] = "H"
    df.loc[high_mask, source_col] = "V"

    # Mark critically high values
    high_problem_mask = df['HIGH_PROBLEM'] == 1
    df.loc[high_mask & high_problem_mask, outcome_col] = "H*"
    df.loc[high_mask & high_problem_mask, source_col] = "V"

    # Impute NORMAL only for numerical values and where merged::TEST_OUTCOME is NA
    normal_mask = na_outcome_mask & (float_df["LOW_LIMIT"] <= float_df[value_col]) & (float_df[value_col] <= float_df["HIGH_LIMIT"])
    df.loc[normal_mask, outcome_col] = "N"
    df.loc[normal_mask, source_col] = "V"

    return df

