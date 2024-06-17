import pandas as pd
import numpy as np


def harmonization(df,args):

    df = (
        df
        .pipe(check_usagi_unit,args)
        .pipe(fix_unit_based_on_abbreviation,args)
        .pipe(omop_mapping,args)
    )

    return df


def omop_mapping(df,args):
    """
    Does omop mapping from UNITSfi.usagi.csv
    """
    mapping_columns = ['TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT']
    df_omop = args.config['usagi_mapping']
    # remove duplicate columns that will be overwritten. They are initialized as out_cols so they would get duplicaed with _x _y suffixes by the merging step
    df = df.drop(columns=[col for col in df.columns if col in df_omop.columns and col not in mapping_columns])
    # save original types to prevent unwanted changes
    orig = df.dtypes.to_dict()
    orig.update(df_omop.dtypes.to_dict())
    merged=pd.merge(df,df_omop,on=mapping_columns,how='left').fillna({'conceptId':-1}).fillna("NA")
    # plug back in the 
    df = merged.apply(lambda x: x.astype(orig[x.name]))
    return df

def fix_unit_based_on_abbreviation(df,args):
    """
    Harmonizes units to make sure all abbreviations with similar units are mapped to same one (e.g. mg --> mg/24h for du-prot).
    """

    col = 'MEASUREMENT_UNIT'
    df = pd.merge(df,args.config['unit_abbreviation_fix'],left_on = ['TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT'],right_on=['TEST_NAME_ABBREVIATION','source_unit_valid'],how='left').fillna("NA")
    mask = df['source_unit_valid_fix'] !="NA"
    df.loc[mask,col] = df.loc[mask,'source_unit_valid_fix']
    # LOG CHANGES
    unit_df = df.loc[mask,['FINREGISTRYID', 'TEST_DATE_TIME','TEST_NAME_ABBREVIATION','source_unit_valid','MEASUREMENT_UNIT']].copy()
    unit_df['SOURCE'] = "harmonization_fix"    
    unit_df.to_csv(args.unit_file, mode='a', index=False, header=False,sep="\t")
    
    return df

def check_usagi_unit(df,args):
    """
    Populates IS_UNIT_VALID column based on whether the unit is in usagi list
    """
    col ='MEASUREMENT_UNIT'
    map_mask = df[col].isin(args.config['usagi_units']['sourceCode'])
    df["IS_UNIT_VALID"] = np.where(map_mask,"1","0")
    return df
    
