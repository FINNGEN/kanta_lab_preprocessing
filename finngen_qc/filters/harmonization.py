import pandas as pd
import numpy as np


def harmonization(df,args):

    df = (
        df
        .pipe(approve_status,args)
        .pipe(check_usagi_unit,args)
        .pipe(fix_unit_based_on_abbreviation,args)
        .pipe(omop_mapping,args)
        .pipe(unit_harmonization,args)

    )

    return df



def unit_harmonization(df,args):
    """"
    Creates two new columns for VALUE/UNIT harmonization
    """
    if args.harmonization:
        # add CONVERSION column
        df = pd.merge(df,args.config['unit_conversion'],on=['harmonization_omop::OMOP_ID','harmonization_omop::omopQuantity','MEASUREMENT_UNIT'],how='left').fillna(np.nan)
        # MAKE SURE MEASUREMENT VALUES is as float column
        df['MEASUREMENT_VALUE'] =pd.to_numeric(df['MEASUREMENT_VALUE'],errors='coerce')
        mask = (df['only_to_omop_concepts'] == True)
        # MULTIPLY VALUE*CONVERSION
        df.loc[~mask,'harmonization_omop::MEASUREMENT_VALUE'] = df.loc[~mask,'harmonization_omop::CONVERSION_FACTOR'].astype(float)*df.loc[~mask,'MEASUREMENT_VALUE'].astype(float)
        # Convert X to measurement value in the formula and evaluate
        df.loc[mask, 'harmonization_omop::MEASUREMENT_VALUE'] = df.loc[mask].apply(
                lambda row: round(eval(row['harmonization_omop::CONVERSION_FACTOR'].replace('X', str(float(row['MEASUREMENT_VALUE'])))),2), 
                axis=1
            )
            
        #BRINGS BACK STR TYPE
        df[['harmonization_omop::MEASUREMENT_VALUE','MEASUREMENT_VALUE','harmonization_omop::CONVERSION_FACTOR','harmonization_omop::MEASUREMENT_UNIT']]=df[['harmonization_omop::MEASUREMENT_VALUE','MEASUREMENT_VALUE','harmonization_omop::CONVERSION_FACTOR','harmonization_omop::MEASUREMENT_UNIT']].fillna("NA")
        
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
    df = pd.merge(df,args.config['unit_abbreviation_fix'],left_on = ['TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT'],right_on=['TEST_NAME_ABBREVIATION','source_unit_clean'],how='left').fillna("NA")
    # check where there is a valid entry and put changed element back
    mask = df['source_unit_clean_fix'] !="NA"
    unit_df = df.loc[mask,['FINNGENID', 'APPROX_EVENT_DATETIME','TEST_NAME_ABBREVIATION','MEASUREMENT_UNIT','source_unit_clean_fix']].copy()
    # CHANGES
    df.loc[mask,"harmonization_omop::IS_UNIT_VALID"] = "unit_fixed"
    df.loc[mask,col] = df.loc[mask,"source_unit_clean_fix"]
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
