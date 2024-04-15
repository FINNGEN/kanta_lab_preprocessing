import pandas as pd
import regex as re


def unit_fixing(df,args):

    col ='LAB_UNIT'
    replacements = args.config['unit_replacements']
    unit_df = df[[col]].copy()
    print(unit_df)
    for rep in replacements:
        df[col] = df[col].replace(rep[0],rep[1],regex=True)

    # LOG CHANGES
    unit_df['new'] = df[col].copy()
    unit_mask = (unit_df[col] != unit_df['new'])
    unit_df[unit_mask][['LAB_UNIT','new']].to_csv(args.unit_file, mode='a', index=False, header=False,sep="\t")

    return df
