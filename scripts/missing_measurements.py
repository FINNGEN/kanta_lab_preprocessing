import sys, os,argparse
import numpy as np
import pandas as pd

from utils import make_sure_path_exists
# Path to OMOP data directory
omop_path = '/mnt/disks/data/kanta/omop/'

# Columns to extract from input files
cols = [
    'FINNGENID',
    'TEST_ID',
    'TEST_OUTCOME', 
    'harmonization_omop::OMOP_ID',
    'harmonization_omop::MEASUREMENT_UNIT',
    'source::MEASUREMENT_VALUE', 
    'MEASUREMENT_FREE_TEXT'
]

# Explicit data type definitions for columns
dtype = {
    'FINNGENID': str,
    'TEST_ID': str,
    'TEST_OUTCOME': str,
    'harmonization_omop::OMOP_ID': int,
    'harmonization_omop::MEASUREMENT_UNIT':str,
    'source::MEASUREMENT_VALUE': float,
    'MEASUREMENT_FREE_TEXT': str
}


def pretty_print(string, l=30): print('-' * (l - int(len(string)/2)) + '> ' + string + ' <' + '-' * (l - int(len(string)/2)))

def read_df(omop,test=True):
    """
    Read and preprocess OMOP measurement data from a TSV file.
    
    Args:
        omop (str/int): Identifier for the specific OMOP dataset
    
    Returns:
        tuple: Full DataFrame and merged DataFrame with abnormality info
    """
    # Construct full file path
    path = f"{os.path.join(omop_path, str(omop) + '.tsv')}"

    pretty_print(str(omop))

    # Read CSV with specified columns and data types
    df = pd.read_csv(path, sep='\t', usecols=cols, dtype=dtype,nrows = 1000 if test else None)
    print(f'Data read: {len(df)} entries')
    
    # Identify entries with missing measurements
    mask = df['source::MEASUREMENT_VALUE'].isna()
    print(f"{len(df[mask])} entries without measurement")
    
    # Identify entries with free text measurements
    textmask = ~df['MEASUREMENT_FREE_TEXT'].isna()
    print(f"{len(df[textmask])} entries with measurement free text")
    
    # Entries with free text but no numeric measurement
    final_mask = (mask & textmask)
    print(f"{len(df[final_mask])} entries with free text and without measurement")
    
    # Load abnormality estimation table
    abdf = pd.read_csv(
        '/home/pete/Dropbox/Projects/kanta_lab_preprocessing/finngen_qc/data/abnormality_estimation.table.tsv', 
        sep='\t', 
        index_col=0, 
        usecols=['ID', 'LOW_LIMIT', 'HIGH_LIMIT']
    )
    
    # Merge dataframe with abnormality table
    m = pd.merge(df[mask & textmask], abdf, how='left', left_on=['harmonization_omop::OMOP_ID'], right_on=['ID'])
    print(f"{100*round(len(m)/len(df),4)} percentage of missing values")
    
    return df, m



def clean_value(val):
        if pd.isna(val):
            return val
            
        # If numeric, return as is
        if isinstance(val, (int, float)):
            return val
            
        val_str = str(val)
        # Match any number (including decimals) at the start
        # Ignore any non-numeric characters that follow
        match = re.match(r'^(\d*\.?\d+)', val_str)
        
        if match:
            return match.group(1)
        return val
    
def return_float(m,unit, original_column_name="MEASUREMENT_FREE_TEXT"):
    """
    Convert free-text measurements to numeric values and analyze.
    
    Args:
        m (pd.DataFrame): Input DataFrame with measurements
        column_name (str, optional): Column with free-text measurements
    
    Returns:
        tuple: DataFrames with float and non-float values
    """
    # Convert column to numeric, coercing errors to NaN
    column_name = 'imputed::MEASUREMENT_FREE_TEXT'
    m[column_name] = m[original_column_name].str.lower().str.replace('*', '').str.replace(unit,'').str.replace(',','.')
    print(m)
    numeric_column = pd.to_numeric(m[column_name], errors='coerce')
    # Separate float and non-float values
    float_mask = numeric_column.notna()
    fm = m[float_mask]
    nfm = m[~float_mask]
    
    # Print conversion statistics
    print(f"{100*round(len(fm)/len(m),2)} percentage of float values")
    print(f"{100*round(len(nfm)/len(m),2)} percentage of non-float values")
    
    # Extract and convert float values
    values = fm[column_name].astype(float).values
    
    # Get low and high limits
    LOW, HIGH = float(m['LOW_LIMIT'][0]), float(m['HIGH_LIMIT'][0])
    
    # Filter values within ±50% of limits
    fmask = (values > LOW*0.5) & (values < HIGH * 1.5)
    
    # Print diagnostic information
    print(f"LIMITS: {LOW} {HIGH}")
    print(f"MEDIAN: {np.median(values)}")
    print(f"MIN/MAX: {np.min(values)} {np.max(values)}")
    print(f"values within ±50% of LOW/HIGH LIMIT: {round(fmask.sum()/len(fm),4)}")
    
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
    unit = df['harmonization_omop::MEASUREMENT_UNIT'].dropna().mode()[0]
    print(f"TARGET_UNIT:{unit}")
    
    fm, nfm = return_float(m,unit)
    # dump float df
    print(fm)
    if args.out_path is None:
        args.out_path = os.path.join(omop_path,'measurement_free_text')
    make_sure_path_exists(args.out_path)
    path = f"{os.path.join(args.out_path, str(args.omop) + '_FREE_TEXT_FLOAT.tsv')}"
    fm.to_csv(path,sep='\t', index=False)
    print(nfm)
    path = f"{os.path.join(args.out_path, str(args.omop) + '_FREE_TEXT_NONFLOAT.tsv')}"
    nfm.to_csv(path,sep='\t', index=False)

