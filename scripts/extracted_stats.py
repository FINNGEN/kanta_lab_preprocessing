#!/usr/bin/env python3
import pandas as pd
import os
import matplotlib.pyplot as plt
import argparse
from scipy import stats
import numpy as np
import scipy
import statsmodels.api as sm
import seaborn as sns
sns.set(palette='Set2')

def parse_args():
    parser = argparse.ArgumentParser(description='Create age vs measurement value plot for OMOP data')
    parser.add_argument('--omop', type=int, required=True,
                      help='OMOP identifier')
    parser.add_argument('--data-dir', type=str, default = '/mnt/disks/data/kanta/analysis/omop',
                      help='Directory containing the input data')
    parser.add_argument('--out-dir', type=str, default = "/mnt/disks/data/kanta/analysis/figs/",
                      help='Directory for output files')
    parser.add_argument('--test', action='store_true',
                        help='Run in test mode with only 10000 rows')

    return parser.parse_args()


def read_data(omop,data_dir,test):
    df = pd.read_csv(
            os.path.join(data_dir, f"{omop}.tsv.gz"),
            sep='\t',
            usecols=['TEST_OUTCOME', 'harmonization_omop::MEASUREMENT_VALUE','extracted::IS_MEASUREMENT_EXTRACTED', 'extracted::IS_POS','MEASUREMENT_FREE_TEXT'],
            nrows = 100 if test else None
    )

    return df



def get_masks(df):
    extracted_outcome_mask = (~df['extracted::IS_POS'].isna()).astype(bool)
    extracted_measurement_mask = (df['extracted::IS_MEASUREMENT_EXTRACTED']).astype(bool)
    return extracted_outcome_mask,extracted_measurement_mask
    


def get_extraction_rate(df, extracted_mask,original_col ='TEST_OUTCOME' , target_col='extracted::IS_POS'):
    # for the target col, i check how many values are extracted
    #print(df[[original_col,target_col]][extracted_mask])
    n_extracted = extracted_mask.sum()
    overall_extraction_rate  = 100*n_extracted/len(df)
    # then i get the background of how many missing  entries we have in the original data
    relative_extraction_rate = 0
    if n_extracted>0:
        relative_extraction_rate  = 100*df[extracted_mask][original_col].isna().sum()/n_extracted

    return n_extracted,f"{overall_extraction_rate:.4f}",f"{relative_extraction_rate:.4f}"

    



def main():
    args = parse_args()
    df = read_data(args.omop,args.data_dir,args.test)
    extracted_outcome_mask,extracted_measurement_mask = get_masks(df)
    res = [str(args.omop)]
    for cols,extracted_mask,desc in zip([['harmonization_omop::MEASUREMENT_VALUE','extracted::IS_MEASUREMENT_EXTRACTED'],['TEST_OUTCOME','extracted::IS_POS']],[extracted_measurement_mask,extracted_outcome_mask],['EXTRACTED_VALUE','EXTRACTED_POS']):
        res +=  list(map(str,get_extraction_rate(df,extracted_mask,cols[0],cols[1])))
    print('\t'.join(res))
    
    
if __name__ == "__main__":
    main()
