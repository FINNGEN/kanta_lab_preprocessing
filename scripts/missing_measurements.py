import sys, os,argparse
import numpy as np
import pandas as pd
from functools import reduce
import re
from utils import make_sure_path_exists
# Path to OMOP data directory
omop_path = '/mnt/disks/data/kanta/omop/'

# Columns to extract from input files
# Explicit data type definitions for columns
cols = {
    'FINNGENID':str,
    'EVENT_AGE':float,
    'TEST_ID': str,
    'TEST_OUTCOME': str,
    'harmonization_omop::OMOP_ID': int,
    'harmonization_omop::MEASUREMENT_UNIT':str,
    'harmonization_omop::MEASUREMENT_VALUE': float,
    'MEASUREMENT_FREE_TEXT': str
}


def pretty_print(string, l=30): print('-' * (l - int(len(string)/2)) + '> ' + string + ' <' + '-' * (l - int(len(string)/2)))

def read_df(omop,test=True):

    """
    read and preprocess omop measurement data from a tsv file.
    
    args:
        omop (str/int): identifier for the specific omop dataset
    
    returns:
        tuple: full dataframe and merged dataframe with abnormality infoa
    """
    # construct full file path
    path = f"{os.path.join(omop_path, str(omop) + '.tsv.gz')}"

    pretty_print(str(omop))

    # read csv with specified columns and data types
    df = pd.read_csv(path, sep='\t', usecols=cols, dtype=cols,nrows = 10000 if test else None)
    print(f'data read: {len(df)} entries')
    
    # identify entries with missing measurements
    mask = df['harmonization_omop::MEASUREMENT_VALUE'].isna()
    print(f"{len(df[mask])} entries without measurement")
    
    # identify entries with free text measurements
    textmask = ~df['MEASUREMENT_FREE_TEXT'].isna()
    print(f"{len(df[textmask])} entries with measurement free text")
    
    # entries with free text but no numeric measurement
    final_mask = (mask & textmask)
    print(f"{len(df[final_mask])} entries shared")
    
    # load abnormality estimation table
    abdf = pd.read_csv(
        '/home/pete/Dropbox/Projects/kanta_lab_preprocessing/finngen_qc/data/abnormality_estimation.table.tsv', 
        sep='\t', 
        index_col=0, 
        usecols=['ID', 'LOW_LIMIT', 'HIGH_LIMIT']
    )
    
    # merge dataframe with abnormality table keeping only entries with na values and non na free text
    m = pd.merge(df[final_mask], abdf, how='left', left_on=['harmonization_omop::OMOP_ID'], right_on=['ID'])
    print(f"{100*round(len(m)/len(df),4)} percentage of original entries that can be used")
    return df, m




def return_float(m,unit,original_column_name="MEASUREMENT_FREE_TEXT"):

    """
    Convert free-text measurements to numeric values and analyze.
    
    Args:
        m (pd.DataFrame): Input DataFrame with measurements
        unit (str): target unit to replace
        original_column_name (str, optional): Column with free-text measurements
    
    Returns:
        tuple: DataFrames with float and non-float values
    """
    # Convert column to numeric, coercing errors to NaN
    column_name = 'imputed::MEASUREMENT_FREE_TEXT'
    # this step takes care of all the string manipulations
    replacements = [
        (r'\*', ''),
        (r':', ''),
        (unit, ''),  # your unit variable
        (r',', '.') #replace commas with dots 
    ]
    result_strings =("tutkimuksentulos","resultat","provresultat")
    regex_replacements = [(rf'\b{re.escape(word)}\b', '') for word in result_strings]
    all_replacements =  regex_replacements + replacements
    print(all_replacements)

    m[column_name] = m[original_column_name].str.lower()
    for rep in all_replacements:
        m.loc[:,column_name] = m.loc[:,column_name].replace(rep[0],rep[1],regex=True)

    # force float conversion
    numeric_column = pd.to_numeric(m[column_name], errors='coerce')
    # Separate float and non-float values
    float_mask = numeric_column.notna()
    fm,nfm = m[float_mask],m[~float_mask]
    
    # Print conversion statistics
    print(f"{100*round(len(fm)/len(m),2)} percentage of float values ({len(fm)})")
    print(f"{100*round(len(nfm)/len(m),2)} percentage of non-float values  ({len(nfm)})")
    
    # Extract and convert float values
    values = fm[column_name].astype(float).values
    
    # Get low and high limits
    LOW, HIGH = float(m['LOW_LIMIT'][0]), float(m['HIGH_LIMIT'][0])
    
    # Filter values within ±50% of limits
    fmask = (values > LOW*0.5) & (values < HIGH * 1.5)
    
    # Print diagnostic information
    print(f"ABORMALITY LIMITS: {LOW} {HIGH}")
    print(f"MEDIAN VALUE: {np.median(values)}")
    print(f"MIN/MAX RANGES: {np.min(values)} {np.max(values)}")
    print(f"values within ±150% of LOW/HIGH LIMIT: {round(fmask.sum()/len(fm),4)}")
    
    return fm, nfm

if __name__ == "__main__":
    # Process OMOP dataset from command-line argument
    parser = argparse.ArgumentParser()
    parser.add_argument('--omop',required=True)
    parser.add_argument('--omop_path',default = '/mnt/disks/data/kanta/omop/')
    parser.add_argument('--out_path')
    parser.add_argument("--test", action='store_true', help="Test run")

    args = parser.parse_args()

    # Read and process data
    df, m = read_df(args.omop,args.test)
    if args.test:
        print(df)
        print(m)

    #extract target unit
    unit = df['harmonization_omop::MEASUREMENT_UNIT'].dropna().mode()[0]
    print(f"TARGET_UNIT:{unit}")
    
    fm, nfm = return_float(m,unit)
    # dump float df
    if args.test:
        print(fm)
        print(nfm)

    if args.out_path is None:
        args.out_path = os.path.join(omop_path,'measurement_free_text')
    make_sure_path_exists(args.out_path)

    path = f"{os.path.join(args.out_path, str(args.omop) + '_FREE_TEXT_FLOAT.tsv.gz')}"
    fm.to_csv(path,sep='\t', index=False)
    path = f"{os.path.join(args.out_path, str(args.omop) + '_FREE_TEXT_NONFLOAT.tsv.gz')}"
    nfm.to_csv(path,sep='\t', index=False)

