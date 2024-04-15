import pandas as pd
import regex as re


def unit_fixing(df,args):

    col ='LAB_UNIT'
    replacements = args.config['unit_replacements']
    for rep in replacements:
        df[col] = df[col].replace(rep[0],rep[1],regex=True)
    return df
