import pandas as pd
import re
import numpy as np

def extract_all(df,args):

    df = (
        df
        .pipe(extract_measurement,args)
        .pipe(extract_positive,args)
        .pipe(extract_outcome,args)
    )
    return df

def extract_outcome(df,args):

    ft_col = "MEASUREMENT_FREE_TEXT"
    col = "extracted::TEST_OUTCOME_TEXT"
    df[col] = "NA"

    col_copy = df[ft_col].copy().str.lower()

    replacements = [(rf'^\s*{word}\s*', '') for word in args.config['free_text_result_strings']]
    replacements += args.config['free_text_measurement_replacements']

    # Remove common prefixes and standardize format
    for pattern, replacement in replacements:
        col_copy = col_copy.str.replace(pattern, replacement, regex=True)
    # Filter for rows containing abnormal indicators
    status_mask = col_copy.str.contains('|'.join(args.config['status_indicators']), na=False)
    # Add spaces around comparison operators for consistent parsing
    for indicator in args.config['status_indicators']:
        col_copy.loc[status_mask] = (
            col_copy.loc[status_mask]
            .replace(indicator, indicator + " ", regex=True)
            .str.replace(r'\s+', ' ', regex=True)
        )

    ft_df = col_copy.loc[status_mask].str.split(' ', expand=True, n=4).reindex(columns=[0, 1, 2, 3])
    ft_df.columns = ['comp', 'value', 'unit','extra']
    if ft_df.empty: return df
    ft_df['ft'] = col_copy.loc[status_mask]

    ft_df['comp'] = ft_df['comp'].replace("alle", "<", regex=True).replace("yli", ">", regex=True)
    # merge togethers situations where int is written as float
    ft_df['value'] = ft_df['value'].astype(str).apply(lambda x: re.sub(r'\.$', '', re.sub(r'0+$', '', x)) if '.' in x else x)
    # need to add unit back
    # remove strange characters
    regex = r'(' + '|'.join([re.escape(x) for x in args.fg_config['fix_units']['MEASUREMENT_UNIT']]) + r')'
    ft_df['unit'] = ft_df['unit'].replace(regex,"",regex=True)
    #map values to target units
    map_mask = ft_df['unit'].isin(args.config['unit_map'])
    ft_df.loc[map_mask,'unit'] = ft_df.loc[map_mask,'unit'].map(args.config['unit_map'])
    # Apply validation conditions directly to create the extracted status
    mask = (
    ft_df['comp'].isin(['<', '>']) &
    pd.to_numeric(ft_df['value'], errors='coerce').notna() &
    (
        ft_df['unit'].isin(list(args.config['usagi_units']['harmonization_omop::sourceCode'].values))
        |
        ft_df['unit'].isna()         
    )
)
    # Initialize all values as "NA" & Only update values that pass the validation
    ft_df[col] = "NA"
    if any(mask): ft_df.loc[mask, col] = ft_df.loc[mask, 'comp'] + ft_df.loc[mask, 'value']+  ft_df.loc[mask, 'unit'].fillna("")
    # Update the original dataframe with the new column
    df.loc[status_mask,col] = ft_df[col].values
    return df
    

def extract_measurement(df, args ):
    """
    Creates new extracted::MEASURMENT_VALUE column with data extracted from MEASUREMENT_FREE_TEXT column
    Also creates a merged column that combines harmonization_omop::MEASUREMENT_VALUE and extracted values
    where harmonization values are NA
    """
    extracted_col_name = "extracted::MEASUREMENT_VALUE"
    omop_col = "harmonization_omop::MEASUREMENT_VALUE"
    ft_col = "MEASUREMENT_FREE_TEXT"
    merged_col_name="extracted::MEASUREMENT_VALUE_MERGED"
    
    # Create a clean copy of the free text for extraction
    df.loc[:, extracted_col_name] = df.loc[:, ft_col].astype(str).str.lower().str.strip().str.replace(r'\s', '', regex=True)
    
    # Remove target unit from the free text
    target_unit = df["harmonization_omop::OMOP_ID"].astype(int).map(args.omop_unit_table)
    df.loc[:, extracted_col_name] = df.apply(lambda row: row[extracted_col_name].replace(str(target_unit[row.name]), ''), axis=1)
    
    # Apply regex replacements
    all_replacements = [(rf'\b{re.escape(word)}\b', '') for word in args.config['free_text_result_strings']] + args.config['free_text_measurement_replacements']
    for rep in all_replacements:
        df.loc[:, extracted_col_name] = df.loc[:, extracted_col_name].replace(rep[0], rep[1], regex=True)
    
    # Convert to numeric and set extraction flag
    df[extracted_col_name] = pd.to_numeric(df[extracted_col_name], errors='coerce')
    
    # Create the merged column - use harmonization values where available, otherwise use extracted values
    df[merged_col_name] = df[omop_col].copy()
    mask = df[omop_col].isna()
    df.loc[mask, merged_col_name] = df.loc[mask, extracted_col_name]
    
    return df



def extract_positive(df,args):
    """
    Creates new column with pos/neg extracted information
    """

    df = pd.merge(df,args.posneg_table, on ="MEASUREMENT_FREE_TEXT",how='left')

    return df
