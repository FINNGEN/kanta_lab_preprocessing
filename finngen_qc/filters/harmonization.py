import pandas as pd
import numpy as np


def harmonization(df,args):

    df = (
        df
        .pipe(approve_status,args)
        .pipe(check_usagi_unit,args)
        .pipe(dump_unit_before_fix,args)
        .pipe(fix_unit_based_on_abbreviation,args)
        .pipe(omop_mapping,args)
        .pipe(unit_harmonization,args)
    )
    return df


def unit_harmonization(df, args):
    """
    Creates two new columns for VALUE/UNIT harmonization.
    
    Process:
    1. Merge with conversion table (may create duplicates when both general and OMOP-specific conversions exist)
    2. Handle duplicates by prioritizing OMOP-specific conversions:
       - _priority = 1 for OMOP-specific (only_to_omop_concepts=True)
       - _priority = 0 for general conversions
       - Sort by _priority descending (1 before 0)
       - Keep first (OMOP-specific) and drop duplicates
    3. Apply conversions:
       - Formula-based: conversion factors containing 'X' (e.g., "10.93*X-23.50")
       - Numeric: simple multiplication (e.g., "0.703")
    """
    # Ensure MEASUREMENT_VALUE is float
    df['MEASUREMENT_VALUE'] = pd.to_numeric(df['MEASUREMENT_VALUE'], errors='coerce')
    
    # Add row identifier before merge to track duplicates
    df['_original_index'] = range(len(df))
    
    # Merge with conversion table (creates duplicates when both general and OMOP-specific exist)
    df = pd.merge(
        df,
        args.config['unit_conversion'],
        on=['harmonization_omop::OMOP_ID', 'harmonization_omop::omopQuantity', 'MEASUREMENT_UNIT'],
        how='left'
    )
    
    # Handle duplicates: prioritize OMOP-specific conversions over general ones
    # _priority = 1 for OMOP-specific (only_to_omop_concepts=True)
    # _priority = 0 for general conversions (only_to_omop_concepts=False/NaN)
    df['_priority'] = df['only_to_omop_concepts'].apply(lambda x: 1 if x is True else 0)
    
    # Sort: same _original_index grouped together, then by _priority descending (1 before 0)
    # This puts OMOP-specific rows first, general rows second
    df = df.sort_values(['_original_index', '_priority'], ascending=[True, False])
    
    # Keep first row per original index (= OMOP-specific if exists, otherwise general)
    df = df.drop_duplicates(subset='_original_index', keep='first')
    df = df.drop(columns=['_original_index', '_priority'])
    
    # Initialize result column
    df['harmonization_omop::MEASUREMENT_VALUE'] = np.nan
    
    # Identify rows with conversions and determine if formula or numeric
    has_conversion = df['harmonization_omop::CONVERSION_FACTOR'].notna()
    df['_has_formula'] = df['harmonization_omop::CONVERSION_FACTOR'].astype(str).str.contains('X', na=False)
    
    # Apply formula-based conversions (conversion factor contains 'X')
    # Example: "10.93*X-23.50" where X is replaced with MEASUREMENT_VALUE
    formula_mask = has_conversion & df['_has_formula'] & df['MEASUREMENT_VALUE'].notna()
    if formula_mask.any():
        df.loc[formula_mask, 'harmonization_omop::MEASUREMENT_VALUE'] = df.loc[formula_mask].apply(
            lambda row: round(
                eval(str(row['harmonization_omop::CONVERSION_FACTOR']).replace(',', '.').replace('X', str(float(row['MEASUREMENT_VALUE'])))),
                2
            ),
            axis=1
        )
    
    # Apply simple numeric conversions (conversion factor is a number)
    # Example: "0.703" multiplied by MEASUREMENT_VALUE
    numeric_mask = has_conversion & ~df['_has_formula'] & df['MEASUREMENT_VALUE'].notna()
    if numeric_mask.any():
        df.loc[numeric_mask, 'harmonization_omop::MEASUREMENT_VALUE'] = (
            pd.to_numeric(df.loc[numeric_mask, 'harmonization_omop::CONVERSION_FACTOR'], errors='coerce') * 
            df.loc[numeric_mask, 'MEASUREMENT_VALUE']
        )
    
    # Clean up helper column
    df = df.drop(columns=['_has_formula'])
    
    # Convert to string with "NA" placeholders for missing values
    df[['harmonization_omop::MEASUREMENT_VALUE', 'MEASUREMENT_VALUE', 
        'harmonization_omop::CONVERSION_FACTOR', 'harmonization_omop::MEASUREMENT_UNIT']] = \
    df[['harmonization_omop::MEASUREMENT_VALUE', 'MEASUREMENT_VALUE', 
        'harmonization_omop::CONVERSION_FACTOR', 'harmonization_omop::MEASUREMENT_UNIT']].fillna("NA")
    
    return df

def dump_unit_before_fix(df,args):
    """
    Step needed for bookkeeping purposes. We build a new column called 'cleaned-pre-fix::MEASUREMENT_UNIT' before injecting units. This way we can keep track of what's going on at the source and compare values before we manually merge them into single distributions
    """
    col = 'MEASUREMENT_UNIT'
    copy_col = "cleaned-pre-fix::MEASUREMENT_UNIT"
    df[copy_col] = df[col]
    return df
    

def omop_mapping(df,args):
    """
    Does omop mapping from LABfi_ALL.usagi.csv
    """
    mapping_columns = ['TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']
    df_omop = args.config['usagi_mapping']
    mask = df_omop['harmonization_omop::mappingStatus'] == "APPROVED"
    df_omop = df_omop.loc[mask,:].fillna("NA")

    # remove duplicate columns that will be overwritten. They are initialized as out_cols so they would get duplicated with _x _y suffixes by the merging step
    df = df.drop(columns=[col for col in df.columns if col in df_omop.columns and col not in mapping_columns])
    # save original types to prevent unwanted changes
    orig = df.dtypes.to_dict()
    orig.update(df_omop.dtypes.to_dict())
    merged=pd.merge(df,df_omop,on=mapping_columns,how='left').fillna({'harmonization_omop::OMOP_ID':"-1"}).fillna("NA")
    # plug back in the types
    df = merged.apply(lambda x: x.astype(orig[x.name]))
    return df

def fix_unit_based_on_abbreviation(df,args):
    """
    Harmonizes units to make sure all abbreviations with similar units are mapped to same one (e.g. osuus --> ratio for b-hkr)
    The df is merged with the unit_abbreviation_fix map and where there is a change MEASUREMENT_UNIT is updated
    """
    col = 'MEASUREMENT_UNIT'
    # this creates new column souce_unit_valid_fix if matching else NA
    original_cols = df.columns
    df = pd.merge(df,args.config['unit_abbreviation_fix'],left_on = ['TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT'],right_on=['TEST_NAME_ABBREVIATION','source_unit_clean'],how='left')
    added_cols = df.columns.difference(original_cols)
    df[added_cols] = df[added_cols].fillna("UNMAPPED")
    # take n
    fix_mask = (df['source_unit_clean_fix'] != "UNMAPPED") & (df['MEASUREMENT_VALUE'] != "NA") & (df['source_unit_clean_fix'] != "NA")
    # Mask 2: Specifically targeting the "NA" clean fix cases
    mask_na_fix = (df['source_unit_clean_fix'] == "NA")
    unit_fix_mask = fix_mask | mask_na_fix
    unit_df = df.loc[unit_fix_mask,['ROW_ID', 'APPROX_EVENT_DATETIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT','source_unit_clean_fix']].copy()
    # CHANGES
    df.loc[unit_fix_mask,"harmonization_omop::IS_UNIT_VALID"] = "unit_fixed"
    df.loc[unit_fix_mask,col] = df.loc[unit_fix_mask,"source_unit_clean_fix"]
    # LOG CHANGES
    unit_df['SOURCE'] = "harmonization_fix"    
    unit_df.to_csv(args.unit_file, mode='a', index=False, header=False,sep="\t")
    return df

def check_usagi_unit(df,args):
    """
    Populates IS_UNIT_VALID column based on whether the unit is in usagi list
    """
    col ='MEASUREMENT_UNIT'
    map_mask = df[col].isin(args.config['usagi_units']['harmonization_omop::sourceCode'])
    df["harmonization_omop::IS_UNIT_VALID"] = np.where(map_mask,"1","0")
    return df
    

def approve_status(df,args):
    """
    Updates mapping status
    """
    approved_mask = args.config['usagi_mapping']['harmonization_omop::mappingStatus'] != "APPROVED"
    args.config['usagi_mapping'].loc[approved_mask,'harmonization_omop::OMOP_ID'] = 0
    return df
