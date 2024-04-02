import pandas as pd


def filter_minimal(df,args):
    """
    This function collects all functions here
    """
    df = fix_na(df)
    return df



def fix_na(df):
    """
    Fixes NAs across columns.
    -1 can be a valid entry for the actual result of the lab analysis so we need to skip that column
    """

    # replace the basic one in the lab value
    arvo_col = "tutkimustulosarvo"
    rej_lines = ['Puuttuu','""',"TYHJÄ","_","NULL"]
    df[arvo_col].replace(rej_lines,"NA",inplace=True)
    #apply the basic one to all other columns
    other_cols = df.columns.difference([arvo_col])
    rej_lines = ['Puuttuu','""',"TYHJÄ","_","NULL","-1"]
    df[other_cols] = df[other_cols].replace(rej_lines,"NA")
    return df
