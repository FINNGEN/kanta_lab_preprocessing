import pandas as pd
import numpy as np


def all_outcome(df, args):
    df = (
        df
        .pipe(impute_outcome, args)
    )

    return df


def impute_outcome(df,args):
    """
    Creates new abnormality column based on
    """
    col ='imputed::TEST_OUTCOME'
    # merge df with low/high tables
    left_col = 'harmonization_omop::OMOP_ID'
    right_col = 'ID'
    # Convert both columns to string type for the merge
    df[left_col] = df[left_col].astype(str)
    args.ab_limits[right_col] = args.ab_limits[right_col].astype(str)

    # Merge with reference ranges table
    df = pd.merge(df, args.ab_limits, how='left', left_on=[left_col], right_on=[right_col])
    
    # LOW/HIGH limits are floats by construction. This way we can then ignore NA measurement values
    float_df = df[['harmonization_omop::MEASUREMENT_VALUE','LOW_LIMIT','HIGH_LIMIT']].apply(pd.to_numeric, errors='coerce')
    # impute LOW abnormality
    low_mask = float_df['harmonization_omop::MEASUREMENT_VALUE'] < float_df["LOW_LIMIT"]
    df.loc[low_mask,col] = "L"
    low_problem_mask = df['LOW_PROBLEM'] ==1
    df.loc[low_mask & low_problem_mask ,col] = "L*"
    # HIGH
    high_mask = float_df['harmonization_omop::MEASUREMENT_VALUE'] > float_df["HIGH_LIMIT"]
    df.loc[high_mask,col] = "H"
    high_problem_mask = df['HIGH_PROBLEM'] ==1
    df.loc[high_mask & high_problem_mask ,col] = "H*"
    # NORMAL ONLY FOR NUMERICAL VALUES (thanks to coercion)
    normal_mask = (float_df["LOW_LIMIT"] <= float_df['harmonization_omop::MEASUREMENT_VALUE']) & (float_df['harmonization_omop::MEASUREMENT_VALUE'] <= float_df["HIGH_LIMIT"])
    df.loc[normal_mask,col] = "N"

    return df



